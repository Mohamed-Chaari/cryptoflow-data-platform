"""Base Kafka producer shared class.
Author: Mohamed Chaari
"""
import json
from typing import Any, Dict
from loguru import logger
from confluent_kafka import Producer
from config import settings

class BaseProducer:
    """Base class for Kafka producers."""

    def __init__(self, topic: str):
        """Initialize the Kafka producer.

        Args:
            topic: The topic to produce messages to.
        """
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': settings.kafka_broker,
            'client.id': 'python-producer'
        })

    def delivery_report(self, err: Any, msg: Any) -> None:
        """Called once for each message produced to indicate delivery result.

        Args:
            err: Error object if delivery failed.
            msg: The message that was delivered or failed.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce(self, key: str, value: Dict[str, Any]) -> None:
        """Produce a message to Kafka.

        Args:
            key: The partition key for the message.
            value: The message payload as a dictionary.
        """
        try:
            payload = json.dumps(value).encode('utf-8')
            self.producer.produce(
                topic=self.topic,
                key=key.encode('utf-8') if key else None,
                value=payload,
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")

    def flush(self) -> None:
        """Wait for any outstanding messages to be delivered."""
        logger.info("Flushing producer...")
        self.producer.flush()
