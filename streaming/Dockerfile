FROM bde2020/spark-python-template:3.1.1-hadoop3.2-python3.8

# Switch to root to install missing apt dependencies
USER root
RUN apk add --no-cache gcc libc-dev g++ postgresql-dev

# Use python:3.11-slim as per spec instead
# ... wait let's write from a standard python image with java

FROM python:3.11-slim

# Install OpenJDK 11, curl, and required dev tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl gcc g++ libpq-dev && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

# Create working directory
WORKDIR /app

# Install pip requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Default command: entrypoint script or sleep for execution
CMD ["python", "-m", "jobs.run_all"]
