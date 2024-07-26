FROM apache/airflow:2.9.1-python3.9

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk-headless && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark google-cloud-storage google-cloud-bigquery pyyaml