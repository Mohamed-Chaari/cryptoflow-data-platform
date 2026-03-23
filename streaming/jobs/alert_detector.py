"""Spark Streaming job to detect price spikes > 3% and produce to Kafka.
Author: Mohamed Chaari
"""
import os
import json
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from loguru import logger

def create_spark_session() -> SparkSession:
    """Creates and returns a SparkSession with necessary packages."""
    return SparkSession.builder \
        .appName("AlertDetector") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()

def get_schema() -> StructType:
    """Returns the schema for the incoming JSON data."""
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("volume_24h", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("price_change_1h", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

# PostgreSQL configuration
POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB', 'cryptoflow')}"
POSTGRES_USER = os.environ.get("POSTGRES_USER", "cryptoflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "cryptoflow_secure_password")
POSTGRES_DRIVER = "org.postgresql.Driver"

def process_batch(df, epoch_id):
    """Process batch of alerts and produce to Kafka topic 'alerts' and write to Postgres."""
    # Write to Postgres first
    df_to_postgres = df.withColumn(
        "alert_type",
        col("price_change_1h").cast("string") # placeholder, will replace with SQL expression or keep it simple
    )
    from pyspark.sql.functions import when
    df_to_postgres = df.withColumn(
        "alert_type",
        when(col("price_change_1h") > 3, "PUMP").otherwise("DUMP")
    ).withColumnRenamed("price_change_1h", "magnitude")

    df_to_postgres = df_to_postgres.select("symbol", "alert_type", "magnitude", "timestamp")

    try:
        df_to_postgres.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "alerts") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", POSTGRES_DRIVER) \
            .mode("append") \
            .save()
    except Exception as e:
        logger.error(f"Failed to write alerts to Postgres: {e}")

    # Produce to Kafka
    alerts = df.collect()
    if not alerts:
        return

    producer = Producer({'bootstrap.servers': 'kafka:9092'})

    for row in alerts:
        alert_data = {
            "symbol": row.symbol,
            "alert_type": "PUMP" if row.price_change_1h > 3 else "DUMP",
            "magnitude": row.price_change_1h,
            "timestamp": row.timestamp.isoformat() if row.timestamp else None
        }

        try:
            producer.produce(
                topic="alerts",
                key=row.symbol.encode('utf-8'),
                value=json.dumps(alert_data).encode('utf-8')
            )
            logger.info(f"Alert generated for {row.symbol}: {alert_data['alert_type']} ({alert_data['magnitude']}%)")
        except Exception as e:
            logger.error(f"Failed to produce alert: {e}")

    producer.flush()

def main():
    """Main execution function."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = get_schema()

    # Read from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "prices-raw") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON value
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Filter for alerts (magnitude > 3% or < -3%)
    df_alerts = df_parsed.filter((col("price_change_1h") > 3) | (col("price_change_1h") < -3))

    # Write stream to console/Kafka
    query = df_alerts.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
