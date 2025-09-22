# PySpark 3.3+ (лучше 3.5.x)
# Запуск:
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1 \
#   stream_basic_avro_router.py

import os, json, urllib.request
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.avro.functions import from_avro

# -----------------------------
# Конфигурация
# -----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPICS    = os.getenv("INPUT_TOPICS", "raw-input").split(",")
ERRORS_TOPIC    = os.getenv("ERRORS_TOPIC", "errors")
ROUTE_TOPIC_PREFIX = os.getenv("ROUTE_TOPIC_PREFIX", "route")

# Apicurio (Confluent-compatible API, напр. http://apicurio:8080/apis/ccompat/v6)
REGISTRY_COMPAT_URL = os.getenv("REGISTRY_COMPAT_URL", "http://apicurio:8080/apis/ccompat/v6")
SCHEMA_SUBJECT = os.getenv("SCHEMA_SUBJECT", "raw-input-value")

# Альтернатива без сети: положить схему прямо сюда
READER_SCHEMA_STR = os.getenv("READER_SCHEMA_STR", "")              # строка JSON Avro
READER_SCHEMA_FILE = os.getenv("READER_SCHEMA_FILE", "")            # путь к файлу со схемой

PHONE_FIELD = os.getenv("PHONE_FIELD", "phone")
ROUTING_RULES = json.loads(os.getenv("ROUTING_RULES_JSON", '{"^\\+7\\d{10}$":"ru",".*":"rest"}'))

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/ckp/stream-basic-avro-router")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")  # "earliest" при необходимости
OUTPUT_KAFKA_ACKS = os.getenv("OUTPUT_KAFKA_ACKS", "all")

# -----------------------------
# Spark
# -----------------------------
spark = (
    SparkSession.builder
    .appName("basic-kafka-avro-router")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Резолв ридер-схемы (локально > файл > Apicurio)
# -----------------------------
def resolve_reader_schema() -> str:
    # 1) из переменной
    if READER_SCHEMA_STR.strip():
        # валидация как JSON
        json.loads(READER_SCHEMA_STR)
        return READER_SCHEMA_STR
    # 2) из файла
    if READER_SCHEMA_FILE.strip():
        with open(READER_SCHEMA_FILE, "r", encoding="utf-8") as f:
            text = f.read()
        json.loads(text)
        return text
    # 3) Apicurio (внутренняя сеть)
    url = REGISTRY_COMPAT_URL.rstrip("/") + f"/subjects/{SCHEMA_SUBJECT}/versions/latest"
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        return data["schema"]

READER_AVRO_SCHEMA = resolve_reader_schema()

# -----------------------------
# UDF: снять confluent wire header (0x00 + 4 байта schema id)
# -----------------------------
WireInfoType = T.StructType([
    T.StructField("payload",   T.BinaryType(), True),
    T.StructField("schema_id", T.IntegerType(), True),
    T.StructField("error",     T.StringType(), True),
])

@F.udf(WireInfoType)
def extract_wire_info(v: bytes):
    try:
        if v is None or len(v) == 0:
            return (None, None, "empty_value")
        b0 = v[0]
        if b0 == 0 and len(v) >= 5:
            sid = int.from_bytes(v[1:5], byteorder="big", signed=False)
            return (v[5:], sid, None)
        # не-wire формат — считаем весь value полезной нагрузкой
        return (v, None, None)
    except Exception as e:
        return (None, None, f"wire_parse_error: {str(e)}")

# -----------------------------
# Источник: Kafka
# -----------------------------
src = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", ",".join(INPUT_TOPICS))
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("includeHeaders", "true")          # ВАЖНО: иначе headers может не прийти
    .load()
)

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

# Декод Avro «мягко»
decoded = with_wire.select(
    "in_key", "in_value", "in_headers", "schema_id", "wire_error",
    from_avro(F.col("payload"), READER_AVRO_SCHEMA, {"mode": "PERMISSIVE"}).alias("obj")
)

# -----------------------------
# Роутинг по телефону (без UDF, NULL-безопасно)
# -----------------------------
phone_col = F.coalesce(F.col(f"obj.{PHONE_FIELD}").cast("string"), F.lit(""))

# CASE ... WHEN по порядку правил
bucket_col = F.lit(None).cast("string")
for pattern, bucket in ROUTING_RULES.items():  # порядок сохранится
    bucket_col = F.when(phone_col.rlike(pattern), F.lit(bucket)).otherwise(bucket_col)
bucket_col = bucket_col.alias("route_bucket")

routed = decoded.select(
    "in_key", "in_value", "in_headers", "schema_id", "wire_error",
    "obj",
    phone_col.alias("parsed_phone"),
    bucket_col
)

# -----------------------------
# Классификация: ошибки / валидные
# -----------------------------
error_reason = (
    F.when(F.col("wire_error").isNotNull(), F.concat(F.lit("wire_error: "), F.col("wire_error")))
     .when(F.col("obj").isNull(), F.lit("from_avro_failed"))
     .when(F.col("route_bucket").isNull(), F.lit("no_route_matched"))
)

errors_ds = routed.where(error_reason.isNotNull()).select(
    F.col("in_key").cast("binary").alias("key"),
    F.col("in_value").alias("value"),  # исходный бинарник «как пришёл»
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
    F.col("in_value").alias("value"),         # форвардим ровно исходные байты
    F.col("in_headers").alias("headers"),
    F.concat(F.lit(ROUTE_TOPIC_PREFIX + "_"), F.col("route_bucket")).alias("topic")
)

# -----------------------------
# Синки: Kafka
# -----------------------------
def write_to_kafka(ds, checkpoint_dir):
    return (
        ds.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.acks", OUTPUT_KAFKA_ACKS)
        .option("kafka.max.request.size", "10485760")  # 10MB на всякий
        .option("checkpointLocation", os.path.join(CHECKPOINT_LOCATION, checkpoint_dir))
        .outputMode("append")
        .start()
    )

q_err = write_to_kafka(errors_ds, "errors")
q_ok  = write_to_kafka(valid_ds,  "valid")

spark.streams.awaitAnyTermination()
