# Базовый образ Spark с Python
FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

# ---------- Build args / versions ----------
ARG PYTHON_VERSION=3.11
ARG SPARK_VERSION=4.0.1
ARG SCALA_BINARY=2.13
ARG MSSQL_JDBC_VERSION=12.6.1.jre11
ARG SPARK_KAFKA_ARTIFACT_VERSION=4.0.1
ARG SPARK_AVRO_ARTIFACT_VERSION=4.0.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates tini bash \
    && rm -rf /var/lib/apt/lists/*

# ---------- Python deps ----------
RUN python3 -m pip install --no-cache-dir --upgrade pip \
 && python3 -m pip install --no-cache-dir \
      opentelemetry-sdk>=1.25.0 \
      opentelemetry-exporter-otlp>=1.25.0 \
      minio>=7.2.5

# ---------- Add Spark connectors ----------
ENV SPARK_HOME=/opt/spark
WORKDIR $SPARK_HOME

RUN set -eux; \
    cd $SPARK_HOME/jars; \
    curl -fSL -o spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_KAFKA_ARTIFACT_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_KAFKA_ARTIFACT_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_KAFKA_ARTIFACT_VERSION}.jar \
    && curl -fSL -o spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_KAFKA_ARTIFACT_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_KAFKA_ARTIFACT_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_KAFKA_ARTIFACT_VERSION}.jar \
    && curl -fSL -o spark-avro_${SCALA_BINARY}-${SPARK_AVRO_ARTIFACT_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_BINARY}/${SPARK_AVRO_ARTIFACT_VERSION}/spark-avro_${SCALA_BINARY}-${SPARK_AVRO_ARTIFACT_VERSION}.jar

# ---------- MSSQL JDBC ----------
RUN mkdir -p /opt/jars && \
    curl -fSL -o /opt/jars/mssql-jdbc.jar \
      https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar

# ---------- App code ----------
WORKDIR /app
COPY stream_kafka_router.py /app/stream_kafka_router.py

ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--conf", "spark.sql.shuffle.partitions=200", \
     "--jars", "/opt/jars/mssql-jdbc.jar", \
     "/app/stream_kafka_router.py" ]
