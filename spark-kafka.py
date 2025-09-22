# pyspark==3.5+, Python 3.10+
# Требуются пакеты: requests, fastavro
# spark-submit:
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
# --py-files your_zip_with_deps.zip  (или поставить deps на кластере)

import os
import json
import struct
import requests
from fastavro import schemaless_reader
from io import BytesIO

from pyspark.sql import SparkSession, functions as F, types as T

# --------------------------
# Конфигурация
# --------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092")
INPUT_TOPICS = os.getenv("INPUT_TOPICS", "raw-input").split(",")
ERRORS_TOPIC = os.getenv("ERRORS_TOPIC", "errors")
# Базовый префикс для целевых топиков (например, route_<bucket>)
ROUTE_TOPIC_PREFIX = os.getenv("ROUTE_TOPIC_PREFIX", "route")

# Apicurio (Confluent-compatible API)
# Пример: http://apicurio:8080/apis/ccompat/v6
REGISTRY_COMPAT_URL = os.getenv("REGISTRY_COMPAT_URL", "http://apicurio:8080/apis/ccompat/v6")

# Если сообщения НЕ в «confluent wire format», можно зафиксировать схему явно:
# SUBJECT / ARTIFACT не нужен, когда во wire-фрейме есть schema id.
# Но оставим возможность «захардкодить» дефолтную схему:
DEFAULT_SCHEMA_STR = os.getenv("DEFAULT_SCHEMA_STR", "")   # JSON Avro schema string (optional)

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/ckp/stream-kafka-avro-routing")
OUTPUT_KAFKA_ACKS = os.getenv("OUTPUT_KAFKA_ACKS", "all")  # для идемпотентности

# Фильтры маршрутизации: phone -> topic bucket
# Можно передать JSON вида:
# {"^\\+7\\d{10}$":"ru", "^\\+380\\d{9}$":"ua", ".*":"rest"}
ROUTING_RULES_JSON = os.getenv("ROUTING_RULES_JSON", '{"^\\+7\\d{10}$":"ru", ".*":"rest"}')
ROUTING_RULES = json.loads(ROUTING_RULES_JSON)

# Имя поля телефона в Avro
PHONE_FIELD = os.getenv("PHONE_FIELD", "phone")

# --------------------------
# Spark
# --------------------------
spark = (
    SparkSession.builder
    .appName("kafka-avro-router-with-errors")
    # Лучше явный Kryo для сериализации
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------
# Кэш схем (Broadcast)
# --------------------------
# Для сообщений в «confluent wire format» первый байт = 0,
# следующие 4 байта — schema id (big-endian int).
# Будем по требованию подтягивать схему из реестра и кэшировать в драйвере,
# затем раздавать executors через broadcast-переменную (простая реализация).

class SchemaCache:
    def __init__(self, registry_compat_url, default_schema_str=""):
        self.url = registry_compat_url.rstrip("/")
        self.default_schema = json.loads(default_schema_str) if default_schema_str else None
        self.cache = {}

    def get_by_id(self, schema_id: int):
        if schema_id in self.cache:
            return self.cache[schema_id]
        # Apicurio Confluent API: /schemas/ids/{id}
        resp = requests.get(f"{self.url}/schemas/ids/{schema_id}", timeout=5)
        resp.raise_for_status()
        schema_json = resp.json()  # {"schema":"<json-string>", ...}
        schema = json.loads(schema_json["schema"])
        self.cache[schema_id] = schema
        return schema

    def get_default(self):
        if not self.default_schema:
            raise RuntimeError("DEFAULT_SCHEMA_STR не задан, а schema id не найден в сообщении.")
        return self.default_schema

schema_cache_driver = SchemaCache(REGISTRY_COMPAT_URL, DEFAULT_SCHEMA_STR)

# Широковещательно делимся регистрационным URL и (вдруг) дефолтной схемой;
# сам HTTP-клиент на executors не дергаем – вытаскиваем схему на драйвере через UDF with state.
# Для простоты — передадим минимальные настройки и будем дергать драйверский кэш через RPC UDF нельзя,
# поэтому используем простой подход: тянем схемы прямо на executors, но с локальным кэшем.
# Если кластер без выхода наружу — задайте DEFAULT_SCHEMA_STR или локальный proxy.
REGISTRY_URL_B = spark.sparkContext.broadcast(REGISTRY_COMPAT_URL)
DEFAULT_SCHEMA_STR_B = spark.sparkContext.broadcast(DEFAULT_SCHEMA_STR)

# --------------------------
# Pandas UDF для «мягкого» парсинга Avro
# --------------------------
# Возвращаем:
# - parsed_phone: string | null
# - parse_error: string | null
# - keep_original_value: binary (для форварда как есть)
# - route_bucket: string | null (для выбора топика)
# Ни при каких ошибках не кидаем исключение — только заполняем parse_error.

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import re

@pandas_udf(
    "string, string, binary, string",
    PandasUDFType.SCALAR
)
def parse_and_route(value_series: pd.Series) -> pd.DataFrame:
    local_cache = {}
    def get_schema_by_id(schema_id: int):
        if schema_id in local_cache:
            return local_cache[schema_id]
        # тянем напрямую у Apicurio (совместимый confluent endpoint)
        url = REGISTRY_URL_B.value.rstrip("/") + f"/schemas/ids/{schema_id}"
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        schema = json.loads(r.json()["schema"])
        local_cache[schema_id] = schema
        return schema

    def get_default_schema():
        s = DEFAULT_SCHEMA_STR_B.value
        if not s:
            raise RuntimeError("DEFAULT_SCHEMA_STR is empty")
        return json.loads(s)

    out_phone = []
    out_error = []
    out_value = []
    out_bucket = []

    compiled_rules = [(re.compile(p), bucket) for p, bucket in ROUTING_RULES.items()]

    for raw in value_series:
        parsed_phone = None
        err = None
        bucket = None
        try:
            if raw is None:
                raise ValueError("Empty value")

            buf = BytesIO(raw)
            # пробуем распознать confluent wire format
            first = buf.read(1)
            if len(first) == 0:
                raise ValueError("Empty payload")
            if first == b"\x00":
                # schema id big-endian
                sid_bytes = buf.read(4)
                if len(sid_bytes) != 4:
                    raise ValueError("Bad wire header (no schema id)")
                schema_id = struct.unpack(">I", sid_bytes)[0]
                schema = get_schema_by_id(schema_id)
                record = schemaless_reader(buf, schema)
            else:
                # не-wire формат — читаем целиком по дефолтной схеме
                buf.seek(0)
                schema = get_default_schema()
                record = schemaless_reader(buf, schema)

            # достаем телефон
            val = record.get(PHONE_FIELD)
            if val is not None:
                parsed_phone = str(val)

            # подбираем bucket по regex-правилам
            if parsed_phone is not None:
                for rx, b in compiled_rules:
                    if rx.match(parsed_phone):
                        bucket = b
                        break
            # если телефон не распознан — отправим в "rest", если таковое правило есть
            if bucket is None and ".*" in ROUTING_RULES:
                bucket = ROUTING_RULES[".*"]

        except Exception as e:
            err = str(e)

        out_phone.append(parsed_phone)
        out_error.append(err)
        out_value.append(raw)   # исходное бинарное Avro — «как есть»
        out_bucket.append(bucket)

    return pd.DataFrame({
        "parsed_phone": out_phone,
        "parse_error": out_error,
        "keep_original_value": out_value,
        "route_bucket": out_bucket
    })

# --------------------------
# Читаем из Kafka
# --------------------------
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", ",".join(INPUT_TOPICS))
    .option("startingOffsets", "latest")  # или "earliest"
    .option("failOnDataLoss", "false")
    .load()
)

# value: binary
parsed = df.select(
    F.col("key").alias("in_key"),
    F.col("value").alias("in_value"),
    F.col("headers").alias("in_headers"),  # array<struct<key:binary,value:binary>>
    *parse_and_route(F.col("value")).alias(
        "parsed_phone", "parse_error", "keep_original_value", "route_bucket"
    )
)

# --------------------------
# Разветвление потоков
# --------------------------

# 1) Ошибочные сообщения -> errors topic с header=error
errors_stream = (
    parsed
    .where(F.col("parse_error").isNotNull())
    .select(
        F.col("in_key").cast("binary").alias("key"),
        # В errors отправляем «как пришло»
        F.col("keep_original_value").alias("value"),
        F.array(
            F.struct(
                F.lit("error").cast("binary").alias("key"),
                F.col("parse_error").cast("binary").alias("value")
            )
        ).alias("headers"),
        F.lit(ERRORS_TOPIC).alias("topic")
    )
)

# 2) Валидные — маршрутизация по bucket в разные топики
# Kafka sink позволяет задать столбец 'topic' (строка).
valid_stream = (
    parsed
    .where(F.col("parse_error").isNull())
    .select(
        F.col("in_key").cast("binary").alias("key"),
        F.col("keep_original_value").alias("value"),
        F.col("in_headers").alias("headers"),     # хотим сохранить входные заголовки — можно убрать, если не нужно
        F.concat(F.lit(ROUTE_TOPIC_PREFIX + "_"), F.col("route_bucket")).alias("topic")
    )
)

# --------------------------
# Запись в Kafka (два независимых sink'а)
# --------------------------
def write_stream(ds, name, checkpoint_subdir):
    return (
        ds.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.acks", OUTPUT_KAFKA_ACKS)
        .option("kafka.max.request.size", 10485760)  # 10MB, при необходимости
        .option("checkpointLocation", os.path.join(CHECKPOINT_LOCATION, checkpoint_subdir))
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )

q_errors = write_stream(errors_stream, "errors-writer", "errors")
q_valid  = write_stream(valid_stream,  "valid-writer",  "valid")

spark.streams.awaitAnyTermination()
s
