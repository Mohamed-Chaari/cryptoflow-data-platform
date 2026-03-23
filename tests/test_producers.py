"""
Author: Mohamed Chaari
"""
import pytest
import json
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../ingestion'))
from producers.base_producer import BaseProducer
from producers.crypto_producer import CryptoProducer, CryptoPrice
from producers.news_producer import NewsProducer, NewsArticle

def test_base_producer_init():
    """Test BaseProducer initialization."""
    with patch('producers.base_producer.Producer') as MockProducer:
        producer = BaseProducer("test-topic")
        assert producer.topic == "test-topic"
        MockProducer.assert_called_once()

def test_crypto_price_schema():
    """Test Pydantic schema for CryptoPrice."""
    data = {
        "symbol": "BTC",
        "price_usd": 50000.0,
        "volume_24h": 1000000.0,
        "market_cap": 900000000.0,
        "price_change_1h": 1.5,
        "timestamp": "2024-01-01T00:00:00Z"
    }
    obj = CryptoPrice(**data)
    assert obj.symbol == "BTC"
    assert obj.price_usd == 50000.0

def test_news_article_schema():
    """Test Pydantic schema for NewsArticle."""
    data = {
        "title": "Crypto goes up",
        "description": "Bitcoin hits new ATH",
        "source": "CoinDesk",
        "published_at": "2024-01-01T00:00:00Z"
    }
    obj = NewsArticle(**data)
    assert obj.title == "Crypto goes up"
    assert obj.source == "CoinDesk"
