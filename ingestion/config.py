"""Configuration models for the Ingestion module."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    """Configuration settings for Kafka producers."""
    kafka_broker: str = Field(default="localhost:9092", env="KAFKA_BROKER")
    news_api_key: str = Field(default="", env="NEWS_API_KEY")
    coingecko_api_url: str = Field(default="https://api.coingecko.com/api/v3")
    news_api_url: str = Field(default="https://newsapi.org/v2/everything")

    crypto_symbols: list[str] = ["bitcoin", "ethereum", "binancecoin", "solana", "cardano"]
    crypto_symbols_mapping: dict[str, str] = {
        "bitcoin": "BTC",
        "ethereum": "ETH",
        "binancecoin": "BNB",
        "solana": "SOL",
        "cardano": "ADA"
    }

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
