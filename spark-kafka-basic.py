# PySpark 3.3+ (желательно 3.5)
# Запуск:
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1 \
#   stream_basic_avro_router.py

import os, json, urllib.request
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.avro.functions import from_avro

# -----------------------------
# Конфигурация (ENV)
# -----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPICS    = os.getenv("INPUT_TOPICS", "raw-input").split(",")
ERRORS_TOPIC    = os.getenv("ERRORS_TOPIC", "errors")
ROUTE_TOPIC_PREFIX = os.getenv("ROUTE_TOPIC_PREFIX", "route")

# Apicurio (Confluent-compatible API, напр. http://apicurio:8080/apis/ccompat/v6)
REGISTRY_COMPAT_URL = os.getenv("REGISTRY_COMPAT_URL", "http://apicurio:8080/apis/ccompat/v6")
# Subject для ридер-схемы (обычно <topic>-value). Берём latest версию.
SCHEMA_SUBJECT = os.getenv("SCHEMA_SUBJECT", "raw-input-value")

PHONE_FIELD = os.getenv("PHONE_FIELD", "phone")

# JSON вида: {"^\\+7\\d{10}$":"ru","^\\+380\\d{9}$":"ua",".*":"rest"}
ROUTING_RULES = json.loads(os.getenv("ROUTING_RULES_JSON", '{"^\\+7\\d{10}$":"ru",".*":"rest"}'))

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/ckp/stream-basic-avro-router")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")  # либо "earliest"
OUTPUT_KAFKA_ACKS = os.getenv("OUTPUT_KAFKA_ACKS", "all")

# -----------------------------
# Инициализация Spark
# -----------------------------
spark = (
    SparkSession.builder
    .appName("basic-kafka-avro-router")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Достаём ридер-схему из Apicurio (один раз, stdlib)
# -----------------------------
def fetch_latest_reader_schema(compat_base_url: str, subject: str) -> str:
    url = compat_base_url.rstrip("/") + f"/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        # ответ формата {"subject": "...", "version": N, "schema": "<JSON string>"}
        return data["schema"]

READER_SCHEMA_STR = fetch_latest_reader_schema(REGISTRY_COMPAT_URL, SCHEMA_SUBJECT)

# -----------------------------
# UDF: снять wire-заголовок, вернуть payload/schema_id/ошибку
# -----------------------------
WireInfoType = T.StructType([
    T.StructField("payload",   T.BinaryType(), True),
    T.StructField("schema_id", T.IntegerType(), True),
    T.StructField("error",     T.StringType(), True),
])

@F.udf(WireInfoType)
def extract_wire_info(value: bytes):
    try:
        if value is None or len(value) == 0:
            return (None, None, "empty_value")
        b0 = value[0]
        if b0 == 0 and len(value) >= 5:
            # Confluent wire format: 0x00 + 4 bytes schema id + avro
            sid = int.from_bytes(value[1:5], byteorder="big", signed=False)
            return (value[5:], sid, None)
        # не wire: считаем, что весь value — это avro payload
        return (value, None, None)
    except Exception as e:
        # никаких исключений наружу — только текст ошибки
        return (None, None, f"wire_parse_error: {str(e)}")

# -----------------------------
# Читаем из Kafka
# -----------------------------
src = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", ",".join(INPUT_TOPICS))
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

# Разбор wire-заголовка
with_wire = src.select(
    F.col("key").alias("in_key"),
    F.col("value").alias("in_value"),
    F.col("headers").alias("in_headers"),
    extract_wire_info(F.col("value")).alias("wire")
).select(
    "in_key", "in_value", "in_headers",
    F.col("wire.payload").alias("payload"),
    F.col("wire.schema_id").alias("schema_id"),
    F.col("wire.error").alias("wire_error")
)

# Декод Avro с ридер-схемой (Spark builtin)
decoded = with_wire.select(
    "in_key", "in_value", "in_headers", "schema_id", "wire_error",
    from_avro(F.col("payload"), READER_SCHEMA_STR, {"mode": "PERMISSIVE"}).alias("obj")
)

# -----------------------------
# Маршрутизация по телефону (без UDF)
# -----------------------------
phone_col = F.col(f"obj.{PHONE_FIELD}").cast("string")

# Собираем CASE WHEN по regex-правилам
bucket_col = None
for pattern, bucket in ROUTING_RULES.items():
    cond = phone_col.rlike(pattern)
    bucket_col = F.when(cond, F.lit(bucket)) if bucket_col is None else bucket_col.when(cond, F.lit(bucket))
# если ни одно правило не сработало — оставим null, такие уйдут в errors как «no_route»
bucket_col = bucket_col.alias("route_bucket") if bucket_col is not None else F.lit(None).alias("route_bucket")

routed = decoded.select(
    "in_key", "in_value", "in_headers", "schema_id", "wire_error", "obj",
    phone_col.alias("parsed_phone"),
    bucket_col
)

# -----------------------------
# Разделяем на валидные / ошибки
# -----------------------------
# Ошибка, если:
#  - wire_error не пустой
#  - объект не распарсился (obj IS NULL)
#  - нет bucket (не попадает ни под одно правило)
error_reason = (
    F.when(F.col("wire_error").isNotNull(), F.concat(F.lit("wire_error: "), F.col("wire_error")))
     .when(F.col("obj").isNull(), F.lit("from_avro_failed"))
     .when(F.col("route_bucket").isNull(), F.lit("no_route_matched"))
)

errors_ds = routed.where(error_reason.isNotNull()).select(
    F.col("in_key").cast("binary").alias("key"),
    F.col("in_value").alias("value"),  # исходный бинарный Avro как пришёл
    F.array(
        F.struct(
            F.lit("error").cast("binary").alias("key"),
            error_reason.cast("binary").alias("value")
        )
    ).alias("headers"),
    F.lit(ERRORS_TOPIC).alias("topic")
)

valid_ds = routed.where(error_reason.isNull()).select(
    F.col("in_key").cast("binary").alias("key"),
    F.col("in_value").alias("value"),         # форвардим исходный value без перекодирования
    F.col("in_headers").alias("headers"),     # сохраняем входные headers (можно убрать)
    F.concat(F.lit(ROUTE_TOPIC_PREFIX + "_"), F.col("route_bucket")).alias("topic")
)

# -----------------------------
# Запись в Kafka (два sink'а)
# -----------------------------
def write_to_kafka(ds, checkpoint_dir):
    return (
        ds.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.acks", OUTPUT_KAFKA_ACKS)
        .option("checkpointLocation", os.path.join(CHECKPOINT_LOCATION, checkpoint_dir))
        .outputMode("append")
        .start()
    )

q_err = write_to_kafka(errors_ds, "errors")
q_ok  = write_to_kafka(valid_ds,  "valid")

spark.streams.awaitAnyTermination()
