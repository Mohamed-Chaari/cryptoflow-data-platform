FROM python:3.11-slim

# Install OpenJDK 17, build essentials
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless build-essential && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Keep the container running so Airflow can trigger bash commands inside it? No, airflow bash operator runs its own image. Wait. The requirements said:
# "Go with the simpler approach — install PySpark inside the Airflow image and use BashOperator. No need for a dedicated Spark cluster in the single compose setup. Document this choice clearly in the README."
# But there's a batch/Dockerfile in the tree requirement.
# Let's just create a basic batch dockerfile that does nothing but sleep so the volume is mounted or keep it standard. Actually if airflow is running it locally, the batch container doesn't strictly need to be a long-running service, or it can be a one-off. Wait, the docker-compose doesn't have a batch service!
# Ah, I see: docker-compose services list: zookeeper, kafka, postgres, airflow-webserver, airflow-scheduler, airflow-init, mlflow, ingestion (producers), streaming (spark), dashboard (streamlit). There is NO batch service in docker-compose.yml. The batch scripts are just code mapped into airflow and run by Airflow!
# I will just create batch/Dockerfile as a dummy to satisfy the directory structure or maybe it was meant for a separate job container. The spec said "Generate the following file/folder structure exactly: batch/Dockerfile, batch/requirements.txt". Okay!
# I'll make it capable of running python scripts.

CMD ["tail", "-f", "/dev/null"]
