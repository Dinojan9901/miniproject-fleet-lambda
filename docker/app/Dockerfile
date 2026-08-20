# One image for the simulators, the speed layer and the serving API.
#
# They share almost all dependencies, and a single image means one build, one
# layer cache, and no risk of the producer and the Spark job disagreeing about
# the version of a shared module in common/.
#
# The Kafka connector jars are baked in rather than resolved at runtime with
# `--packages`: Ivy resolution on first run needs the network, takes minutes,
# and fails the whole job when Maven Central is slow. A pinned jar set makes the
# image reproducible and lets the pipeline start offline.

FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/project \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Spark <-> Kafka connector, matched to pyspark 3.5.1 / Scala 2.12.
ARG SPARK_VERSION=3.5.1
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

RUN SPARK_JARS="$(python -c 'import pyspark, pathlib; print(pathlib.Path(pyspark.__file__).parent / "jars")')" \
    && echo "installing connector jars into ${SPARK_JARS}" \
    && curl -fsSL -o "${SPARK_JARS}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
        "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_JARS}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
        "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_JARS}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
        "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_JARS}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
        "${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

# Source is bind-mounted at run time so an edit does not require a rebuild;
# this copy keeps the image usable on its own.
COPY . /opt/project

CMD ["python", "-c", "print('specify a command in docker-compose.yml')"]
