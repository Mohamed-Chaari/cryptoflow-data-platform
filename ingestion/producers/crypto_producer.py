"""CoinGecko API consumer to push crypto data to Kafka.
Author: Mohamed Chaari
"""
import time
import requests
from datetime import datetime, timezone
from loguru import logger
from pydantic import BaseModel
from producers.base_producer import BaseProducer

# Must import from ingestion.config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings


class CryptoPrice(BaseModel):
    """Schema for a single cryptocurrency price update."""
    symbol: str
    price_usd: float
    volume_24h: float
    market_cap: float
    price_change_1h: float
    timestamp: str


class CryptoProducer(BaseProducer):
    """Producer fetching from CoinGecko API."""

    def __init__(self):
        super().__init__(topic="prices-raw")
        self.url = f"{settings.coingecko_api_url}/coins/markets"
        self.params = {
            "vs_currency": "usd",
            "ids": ",".join(settings.crypto_symbols),
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "1h"
        }

    def fetch_data(self) -> list[CryptoPrice]:
        """Fetch real-time data from CoinGecko API with exponential backoff.

        Returns:
            list[CryptoPrice]: List of fetched price objects.
        """
        backoff = 1
        max_backoff = 64
        while True:
            try:
                response = requests.get(self.url, params=self.params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    prices = []
                    current_time = datetime.now(timezone.utc).isoformat()
                    for item in data:
                        symbol = settings.crypto_symbols_mapping.get(item['id'], item['symbol'].upper())

                        price = CryptoPrice(
                            symbol=symbol,
                            price_usd=float(item.get('current_price', 0.0) or 0.0),
                            volume_24h=float(item.get('total_volume', 0.0) or 0.0),
                            market_cap=float(item.get('market_cap', 0.0) or 0.0),
                            price_change_1h=float(item.get('price_change_percentage_1h_in_currency', 0.0) or 0.0),
                            timestamp=current_time
                        )
                        prices.append(price)
                    return prices
                elif response.status_code == 429:
                    logger.warning(f"Rate limited by CoinGecko API. Retrying in {backoff} seconds...")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                else:
                    logger.error(f"CoinGecko API returned status code {response.status_code}: {response.text}")
                    return []
            except Exception as e:
                logger.error(f"Failed to fetch data from CoinGecko API: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    def run(self):
        """Continuously fetch and produce data every 10 seconds."""
        logger.info("Starting Crypto Producer...")
        while True:
            prices = self.fetch_data()
            if prices:
                for price in prices:
                    self.produce(key=price.symbol, value=price.model_dump())
                    logger.debug(f"Produced: {price}")
            else:
                logger.warning("No prices fetched in this cycle.")

            # Wait 10 seconds before next poll
            time.sleep(10)


if __name__ == "__main__":
    producer = CryptoProducer()
    try:
        producer.run()
    except KeyboardInterrupt:
        logger.info("Stopping Crypto Producer...")
        producer.flush()
