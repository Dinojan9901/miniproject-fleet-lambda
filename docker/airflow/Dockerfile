# Airflow, extended with a JVM and PySpark so the scheduler can spark-submit the
# batch layer directly.
#
# The alternative -- DockerOperator or a SparkSubmitOperator pointing at a
# separate cluster -- is what production would use. Running Spark in local mode
# inside the Airflow worker is the honest laptop compromise, and it is called out
# as such in the report's limitations.

FROM apache/airflow:2.8.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONPATH=/opt/project

USER airflow
RUN pip install --no-cache-dir \
        pyspark==3.5.1 \
        psycopg2-binary==2.9.9 \
        prometheus-client==0.21.0
