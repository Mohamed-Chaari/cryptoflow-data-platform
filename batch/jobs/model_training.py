"""Prophet model training job with MLflow tracking.
Author: Mohamed Chaari
"""
import os
import sys
import pickle
from datetime import datetime
import pandas as pd
from prophet import Prophet
import mlflow
import mlflow.prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
from loguru import logger

# Set MLflow tracking URI to the mlflow container
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("crypto_price_prediction")

def main():
    logger.info("Starting model training...")

    # Load feature-engineered Parquet for BTC
    # Since this is a single container setup with Airflow running the bash operator locally,
    # paths are relative to Airflow's mounted volume
    features_path = "/opt/airflow/data/features/ohlcv/symbol=BTC"

    try:
        df = pd.read_parquet(features_path)
    except Exception as e:
        logger.error(f"Failed to read features data: {e}")
        sys.exit(1)

    if df.empty:
        logger.warning("No data found for BTC. Exiting.")
        sys.exit(0)

    # Prepare data for Prophet: needs 'ds' (datetime) and 'y' (target)
    # We predict 'close' price
    prophet_df = df[["window_start", "close"]].rename(columns={"window_start": "ds", "close": "y"})

    # Sort and split data (last 24h as test set)
    prophet_df = prophet_df.sort_values(by="ds")

    # 24 hours of 1-minute data = 1440 rows
    test_size = 1440
    if len(prophet_df) <= test_size:
        logger.warning("Not enough data to train and test. Exiting.")
        sys.exit(0)

    train_df = prophet_df.iloc[:-test_size]
    test_df = prophet_df.iloc[-test_size:]

    logger.info(f"Training on {len(train_df)} rows, testing on {len(test_df)} rows.")

    with mlflow.start_run(run_name=f"prophet_btc_{datetime.now().strftime('%Y%m%d')}") as run:
        # Initialize Prophet model
        model = Prophet(
            yearly_seasonality=False,
            weekly_seasonality=True,
            daily_seasonality=True,
            changepoint_prior_scale=0.05
        )

        # Log parameters
        mlflow.log_params({
            "yearly_seasonality": False,
            "weekly_seasonality": True,
            "daily_seasonality": True,
            "changepoint_prior_scale": 0.05,
            "train_size": len(train_df),
            "test_size": len(test_df),
            "symbol": "BTC"
        })

        # Train
        logger.info("Fitting Prophet model...")
        model.fit(train_df)

        # Predict
        logger.info("Making predictions for test set...")
        future = model.make_future_dataframe(periods=len(test_df), freq='1min')
        forecast = model.predict(future)

        # Evaluate
        predictions = forecast.iloc[-test_size:]["yhat"].values
        actuals = test_df["y"].values

        mae = mean_absolute_error(actuals, predictions)
        rmse = mean_squared_error(actuals, predictions, squared=False)
        mape = mean_absolute_percentage_error(actuals, predictions)

        logger.info(f"Metrics - MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.4f}")

        # Log metrics
        mlflow.log_metrics({
            "mae": mae,
            "rmse": rmse,
            "mape": mape
        })

        # Log model
        mlflow.prophet.log_model(
            pr_model=model,
            artifact_path="prophet-model",
            registered_model_name="crypto-price-predictor"
        )

        # Save model to local volume mapped to ML models folder
        model_path = "/opt/airflow/ml/models/latest_prophet.pkl"
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        logger.info(f"Saved latest model to {model_path}")

if __name__ == "__main__":
    main()
