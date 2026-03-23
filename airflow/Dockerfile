FROM apache/airflow:2.8.0-python3.11

USER root

# Install OpenJDK 17 and build tools for prophet and other C-extensions
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

USER airflow

# Upgrade pip
RUN pip install --upgrade pip

# Install PySpark, ML tools, data quality, PostgreSQL connector
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    pandas==2.1.4 \
    pyarrow==14.0.2 \
    pandera==0.18.0 \
    great_expectations==0.18.8 \
    mlflow==2.9.2 \
    prophet==1.1.5 \
    loguru==0.7.2 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-postgres==5.10.0
