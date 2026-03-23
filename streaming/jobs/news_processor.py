"""Spark Streaming job to process news sentiment and write to PostgreSQL.
Author: Mohamed Chaari
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from textblob import TextBlob
from loguru import logger

# PostgreSQL configuration
POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB', 'cryptoflow')}"
POSTGRES_USER = os.environ.get("POSTGRES_USER", "cryptoflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "cryptoflow_secure_password")
POSTGRES_DRIVER = "org.postgresql.Driver"

def create_spark_session() -> SparkSession:
    """Creates and returns a SparkSession with necessary packages."""
    return SparkSession.builder \
        .appName("NewsProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()

def get_schema() -> StructType:
    """Returns the schema for the incoming JSON data."""
    return StructType([
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("source", StringType(), True),
        StructField("published_at", TimestampType(), True)
    ])

@udf(returnType=DoubleType())
def analyze_sentiment(text: str) -> float:
    """Analyze sentiment of text using TextBlob. Return polarity score."""
    if not text:
        return 0.0
    analysis = TextBlob(text)
    return analysis.sentiment.polarity

@udf(returnType=StringType())
def label_sentiment(score: float) -> str:
    """Label sentiment score."""
    if score > 0.1:
        return "Positive"
    elif score < -0.1:
        return "Negative"
    else:
        return "Neutral"

def process_batch(df, epoch_id):
    import os
    from textblob import TextBlob
    import sys

    """Function to process each micro-batch. Writes to Postgres."""
    if df.isEmpty():
        return

    df = df.withColumn("sentiment_score", analyze_sentiment(col("title")))
    df = df.withColumn("sentiment_label", label_sentiment(col("sentiment_score")))

    df_to_write = df.select(
        col("title"),
        col("sentiment_score"),
        col("sentiment_label"),
        col("published_at").alias("timestamp")
    )

    df_to_write.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "news_sentiment") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("append") \
        .save()

    logger.info(f"Processed batch {epoch_id} of news sentiment.")

def main():
    """Main execution function."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = get_schema()

    # Read from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "news-raw") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON value
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Write stream
    query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
