"""NewsAPI consumer to push crypto news to Kafka."""
import time
import requests
from datetime import datetime, timezone
from loguru import logger
from pydantic import BaseModel

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from producers.base_producer import BaseProducer


class NewsArticle(BaseModel):
    """Schema for a single news article."""
    title: str
    description: str
    source: str
    published_at: str


class NewsProducer(BaseProducer):
    """Producer fetching from NewsAPI."""

    def __init__(self):
        super().__init__(topic="news-raw")
        self.url = settings.news_api_url
        self.api_key = settings.news_api_key
        self.query = " OR ".join(settings.crypto_symbols)

    def fetch_data(self) -> list[NewsArticle]:
        """Fetch latest crypto news from NewsAPI.

        Returns:
            list[NewsArticle]: List of fetched news articles.
        """
        if not self.api_key:
            logger.warning("No NewsAPI key provided. Skipping news fetch.")
            return []

        params = {
            "q": self.query,
            "apiKey": self.api_key,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 10
        }

        try:
            response = requests.get(self.url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                articles = []
                for item in data.get("articles", []):
                    article = NewsArticle(
                        title=item.get("title", ""),
                        description=item.get("description", ""),
                        source=item.get("source", {}).get("name", "Unknown"),
                        published_at=item.get("publishedAt", datetime.now(timezone.utc).isoformat())
                    )
                    articles.append(article)
                return articles
            else:
                logger.error(f"NewsAPI returned status code {response.status_code}: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Failed to fetch data from NewsAPI: {e}")
            return []

    def run(self):
        """Continuously fetch and produce data every 5 minutes."""
        logger.info("Starting News Producer...")
        while True:
            articles = self.fetch_data()
            if articles:
                for article in articles:
                    # Use title as a naive key to prevent duplicates
                    self.produce(key=article.title[:20], value=article.model_dump())
                    logger.debug(f"Produced news: {article.title}")
            else:
                logger.warning("No news fetched in this cycle.")

            # Wait 5 minutes before next poll to stay within free tier
            time.sleep(300)


if __name__ == "__main__":
    producer = NewsProducer()
    try:
        producer.run()
    except KeyboardInterrupt:
        logger.info("Stopping News Producer...")
        producer.flush()
