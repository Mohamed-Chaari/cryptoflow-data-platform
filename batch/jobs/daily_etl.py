"""Spark Batch job to clean and process raw Parquet files daily.
Author: Mohamed Chaari
"""
import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, when, isnan
from loguru import logger
import pandera as pa
import pandas as pd

def create_spark_session():
    return SparkSession.builder \
        .appName("DailyETL") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

# Define Pandera schema for data quality checks
schema = pa.DataFrameSchema({
    "symbol": pa.Column(str, checks=pa.Check.isin(["BTC", "ETH", "BNB", "SOL", "ADA"])),
    "window_start": pa.Column("datetime64[ns]"),
    "window_end": pa.Column("datetime64[ns]"),
    "open": pa.Column(float, checks=pa.Check.ge(0.0)),
    "high": pa.Column(float, checks=pa.Check.ge(0.0)),
    "low": pa.Column(float, checks=pa.Check.ge(0.0)),
    "close": pa.Column(float, checks=pa.Check.ge(0.0)),
    "avg_volume": pa.Column(float, checks=pa.Check.ge(0.0))
})

def validate_data(pdf: pd.DataFrame) -> pd.DataFrame:
    """Validate DataFrame using Pandera."""
    try:
        validated_df = schema.validate(pdf)
        return validated_df
    except pa.errors.SchemaError as e:
        logger.error(f"Schema validation failed: {e}")
        # In production, we might want to quarantine these records or raise
        # For this project, we'll log and drop invalid rows (if easily done) or raise
        raise

def main():
    spark = create_spark_session()

    # Calculate yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    raw_path = f"/opt/airflow/data/raw/ohlcv/date={yesterday}"
    processed_path = "/opt/airflow/data/processed/ohlcv"

    logger.info(f"Starting daily ETL for date: {yesterday}")

    if not os.path.exists(raw_path):
        logger.warning(f"No raw data found at {raw_path}. Exiting.")
        sys.exit(0)

    # Read raw data
    try:
        df = spark.read.parquet(raw_path)
    except Exception as e:
        logger.error(f"Failed to read parquet files: {e}")
        sys.exit(1)

    # Clean: Handle nulls, drop duplicates
    logger.info(f"Initial row count: {df.count()}")
    df_clean = df.dropDuplicates(["symbol", "window_start"]).dropna()
    logger.info(f"Row count after cleaning: {df_clean.count()}")

    if df_clean.count() == 0:
        logger.warning("No data left after cleaning. Exiting.")
        sys.exit(0)

    # Convert to Pandas for validation (since it's daily data partitioned, it should fit in memory)
    # If it's too large, we would use Great Expectations with Spark. For now, Pandera is requested.
    pdf_clean = df_clean.toPandas()

    logger.info("Running Pandera data quality checks...")
    try:
        pdf_validated = validate_data(pdf_clean)
        logger.info("Data quality checks passed.")
    except Exception as e:
        logger.error("Data quality checks failed. Pipeline halting.")
        sys.exit(1)

    # Convert back to Spark and add date partition column
    df_validated = spark.createDataFrame(pdf_validated)
    df_validated = df_validated.withColumn("date", to_date(col("window_start")))

    # Write to processed folder partitioned by symbol and date
    df_validated.write \
        .mode("append") \
        .partitionBy("symbol", "date") \
        .parquet(processed_path)

    logger.info(f"Successfully processed and wrote data to {processed_path}")
    spark.stop()

if __name__ == "__main__":
    main()
