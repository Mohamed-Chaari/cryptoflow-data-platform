"""Spark Structured Streaming job to aggregate 1-minute OHLCV data from Kafka and write to Postgres and Parquet."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, first, max, min, last, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# PostgreSQL configuration
POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB', 'cryptoflow')}"
POSTGRES_USER = os.environ.get("POSTGRES_USER", "cryptoflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "cryptoflow_secure_password")
POSTGRES_DRIVER = "org.postgresql.Driver"

def create_spark_session() -> SparkSession:
    """Creates and returns a SparkSession with necessary packages."""
    return SparkSession.builder \
        .appName("CryptoPriceAggregator") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
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

def process_batch(df, epoch_id):
    """Function to process each micro-batch. Writes to Postgres and Parquet."""
    if df.isEmpty():
        return

    # Write to PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "ohlcv_1min") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("append") \
        .save()

    # Write to Parquet partitioned by date (extracted from window_start)
    df_parquet = df.withColumn("date", df.window_start.cast("date"))
    df_parquet.write \
        .mode("append") \
        .partitionBy("date") \
        .parquet("/app/data/raw/ohlcv")

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

    # Add watermark to handle late data (2 minutes as per spec)
    df_watermarked = df_parsed.withWatermark("timestamp", "2 minutes")

    # Aggregate: 1-minute tumbling windows
    df_aggregated = df_watermarked.groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ).agg(
        first("price_usd").alias("open"),
        max("price_usd").alias("high"),
        min("price_usd").alias("low"),
        last("price_usd").alias("close"),
        avg("volume_24h").alias("avg_volume")
    ).select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("avg_volume")
    )

    # Write stream
    query = df_aggregated.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
