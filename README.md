# CryptoFlow

**CryptoFlow** is a complete, real-time cryptocurrency analytics platform designed to demonstrate a modern end-to-end Data Engineering pipeline. It ingests live market and news data, streams it for real-time aggregation and alerting, runs daily batch ETL and feature engineering, trains machine learning models to predict prices, and serves everything on a live Streamlit dashboard.

## 🌟 Features
- **Real-Time Data Ingestion:** Fetches live crypto prices from CoinGecko API and crypto news from NewsAPI.
- **Event Streaming (Kafka):** Raw data is published to Kafka topics.
- **Stream Processing (Spark Structured Streaming):** 1-minute OHLCV aggregation, price spike alerts (>3%), and real-time news sentiment analysis (TextBlob).
- **Data Lakehouse (Parquet + PostgreSQL):** Raw streaming data is saved to Parquet and Postgres.
- **Batch Processing (Spark & Airflow):** Daily ETL cleans data, runs quality checks (Pandera), computes technical indicators (RSI, MACD, Bollinger Bands), and stores it as features.
- **Machine Learning (Prophet + MLflow):** Trains a price prediction model for BTC and tracks experiments in MLflow.
- **Serving (Streamlit):** Interactive 4-page dashboard with live metrics, historical analysis, technical indicators, and ML forecasts.

## 🏗️ Architecture Stack
- **Languages:** Python 3.11
- **Streaming:** Apache Kafka, Zookeeper, Spark Structured Streaming
- **Batch:** Apache Spark 3.5 (PySpark)
- **Orchestration:** Apache Airflow 2.8 (LocalExecutor, PySpark running locally)
- **Storage:** PostgreSQL 15, Local Parquet
- **Machine Learning:** Facebook Prophet, MLflow Tracking Server
- **Serving/Dashboard:** Streamlit, Plotly
- **Infrastructure:** Docker & Docker Compose

## 🚀 Getting Started

### Prerequisites
- Docker and Docker Compose installed.
- **Minimum 8GB RAM recommended** (the stack runs Kafka, Spark, Postgres, Airflow, and MLflow locally).
- (Optional) NewsAPI Key for news sentiment (free tier).

### Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/cryptoflow.git
   cd cryptoflow
   ```

2. **Configure environment variables:**
   Copy the example config and fill in your details (specifically the `NEWS_API_KEY` if you want live news sentiment).
   ```bash
   cp .env.example .env
   ```

3. **Start the pipeline:**
   Using the provided Makefile, you can spin up the entire ecosystem with a single command:
   ```bash
   make up
   ```
   *(This runs `docker-compose up --build -d` and starts ingesting data within 60 seconds).*

### Accessing the Services
- **Streamlit Dashboard:** [http://localhost:8501](http://localhost:8501)
- **Airflow Webserver:** [http://localhost:8080](http://localhost:8080) (User: `admin`, Password: `admin`)
- **MLflow UI:** [http://localhost:5000](http://localhost:5000)

### Makefile Shortcuts
- `make up` - Start all services.
- `make down` - Stop all services.
- `make logs service=<name>` - View logs for a specific service (e.g., `make logs service=ingestion`).
- `make test` - Run the Pytest suite.
- `make reset-data` - Wipes all volumes, databases, and local Parquet files for a fresh start.

## 📂 Project Structure
- `ingestion/` - Kafka producers for APIs (CoinGecko, NewsAPI).
- `streaming/` - Spark Structured Streaming jobs (OHLCV, Alerts, Sentiment).
- `batch/` - PySpark batch jobs for ETL, Features, and Prophet training.
- `airflow/` - DAGs orchestrating the daily pipeline and data quality checks.
- `serving/` - Streamlit dashboard application.
- `data/` - Mapped volume for Raw, Processed, and Feature Parquet files.
- `ml/` - Saved MLflow model artifacts.
- `tests/` - Unit tests for the pipeline components.

## 🖼️ Screenshots
*(Placeholder for actual dashboard screenshots)*
- **Live Monitor:** Real-time prices and alerts.
- **Technical Indicators:** RSI, MACD, and Bollinger Bands.
- **ML Predictions:** 24h forecast with confidence intervals.
