FROM apache/airflow:2.9.2-python3.11
USER root
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && apt-get clean
USER airflow
RUN pip install --no-cache-dir minio dbt-postgres pyspark kafka-python
