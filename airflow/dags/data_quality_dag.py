"""Data Quality DAG to check nulls, schema drift, and freshness periodically."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from loguru import logger

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: logger.error(f"Task {context['task_instance'].task_id} failed in Data Quality Check!")
}

with DAG(
    'data_quality_checks',
    default_args=default_args,
    description='A DAG to periodically check data quality of processed datasets.',
    schedule_interval='0 */4 * * *', # Run every 4 hours
    catchup=False,
    tags=['cryptoflow', 'quality'],
) as dag:

    def check_nulls():
        """Check for nulls in yesterday's processed data."""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        processed_path = f"/opt/airflow/data/processed/ohlcv/date={yesterday}"
        try:
            df = pd.read_parquet(processed_path)
            null_counts = df.isnull().sum()
            total_nulls = null_counts.sum()
            if total_nulls > 0:
                logger.warning(f"Found {total_nulls} nulls in processed data for {yesterday}.")
                # Could optionally raise an error here
            else:
                logger.info(f"No nulls found in processed data for {yesterday}.")
        except FileNotFoundError:
            logger.warning(f"No processed data found for {yesterday}. Skipping check.")
        except Exception as e:
            logger.error(f"Error checking nulls: {e}")
            raise

    def check_schema_drift():
        """Check for schema drift against expected columns."""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        processed_path = f"/opt/airflow/data/processed/ohlcv/date={yesterday}"
        expected_columns = {"symbol", "window_start", "window_end", "open", "high", "low", "close", "avg_volume"}
        try:
            df = pd.read_parquet(processed_path)
            actual_columns = set(df.columns)
            missing = expected_columns - actual_columns
            if missing:
                logger.error(f"Schema drift detected! Missing columns: {missing}")
                raise ValueError(f"Missing columns: {missing}")
            logger.info("Schema matches expected structure.")
        except FileNotFoundError:
            logger.warning(f"No processed data found for {yesterday}. Skipping check.")
        except Exception as e:
            logger.error(f"Error checking schema drift: {e}")
            raise

    def check_freshness():
        """Check if streaming is active by verifying recent records in Postgres."""
        from sqlalchemy import create_engine
        import os

        # Connect to Postgres
        pg_user = os.environ.get("POSTGRES_USER", "cryptoflow")
        pg_password = os.environ.get("POSTGRES_PASSWORD", "cryptoflow_secure_password")
        pg_db = os.environ.get("POSTGRES_DB", "cryptoflow")
        engine = create_engine(f"postgresql://{pg_user}:{pg_password}@postgres:5432/{pg_db}")

        # Query max timestamp
        try:
            with engine.connect() as conn:
                result = conn.execute("SELECT MAX(window_start) FROM ohlcv_1min")
                max_time = result.fetchone()[0]

            if not max_time:
                logger.warning("No data in ohlcv_1min table.")
                return

            time_diff = datetime.now() - max_time
            if time_diff > timedelta(minutes=30):
                logger.error(f"Data staleness detected! Last record was {time_diff.total_seconds() / 60:.2f} minutes ago.")
                raise ValueError(f"Data staleness detected! Last record was {time_diff.total_seconds() / 60:.2f} minutes ago.")
            else:
                logger.info("Data is fresh.")
        except Exception as e:
            logger.error(f"Error checking freshness: {e}")
            raise

    # Define tasks
    t1 = PythonOperator(
        task_id='check_nulls',
        python_callable=check_nulls,
    )

    t2 = PythonOperator(
        task_id='check_schema_drift',
        python_callable=check_schema_drift,
    )

    t3 = PythonOperator(
        task_id='check_freshness',
        python_callable=check_freshness,
    )

    # All checks run in parallel
    [t1, t2, t3]
