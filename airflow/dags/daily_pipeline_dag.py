"""Daily DAG to orchestrate the ETL, Feature Engineering, and Model Training pipeline."""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from airflow.operators.python import PythonOperator
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
    'on_failure_callback': lambda context: logger.error(f"Task {context['task_instance'].task_id} failed!")
}

# Paths inside Airflow container mapped to batch volume
BATCH_DIR = "/opt/airflow/batch/jobs"
PYTHON_BIN = "python"

with DAG(
    'daily_crypto_pipeline',
    default_args=default_args,
    description='A daily pipeline to run ETL, feature engineering, and model training.',
    schedule_interval='0 2 * * *', # Run daily at 02:00 UTC
    catchup=False,
    tags=['cryptoflow'],
) as dag:

    # Task 1: Check data freshness (Sensor)
    # Checks if yesterday's data folder exists. BashSensor uses exit 0 to pass.
    check_data_freshness = BashSensor(
        task_id='check_data_freshness',
        bash_command='test -d /opt/airflow/data/raw/ohlcv/date=$(date -d "yesterday" +%Y-%m-%d)',
        timeout=60 * 60, # Wait up to 1 hour
        mode='poke',
        poke_interval=600 # Check every 10 mins
    )

    # Task 2: Run daily ETL script
    run_etl = BashOperator(
        task_id='run_etl',
        bash_command=f'{PYTHON_BIN} {BATCH_DIR}/daily_etl.py',
    )

    # Task 3: Run feature engineering
    run_feature_engineering = BashOperator(
        task_id='run_feature_engineering',
        bash_command=f'{PYTHON_BIN} {BATCH_DIR}/feature_engineering.py',
    )

    # Task 4: Run model training
    run_model_training = BashOperator(
        task_id='run_model_training',
        bash_command=f'{PYTHON_BIN} {BATCH_DIR}/model_training.py',
    )

    # Task 5: Notify success
    def log_success():
        logger.info("Daily pipeline completed successfully.")

    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=log_success
    )

    # Set task dependencies
    check_data_freshness >> run_etl >> run_feature_engineering >> run_model_training >> notify_success
