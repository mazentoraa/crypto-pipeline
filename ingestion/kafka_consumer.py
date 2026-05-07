"""
ingestion/kafka_consumer.py
────────────────────────────
Kafka consumer that:
  1. Reads news headline messages from the 'crypto-news' topic.
  2. Classifies sentiment using TextBlob (positive / neutral / negative).
  3. Stores enriched records in the news_headlines PostgreSQL table.

Consumer group ID = "crypto-news-consumer-group"
  → Multiple consumer instances could run in parallel for scalability.

Run in its own terminal:
    python -m ingestion.kafka_consumer
"""

import json
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from textblob import TextBlob

from database.db import insert_news_headline
from utils.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from utils.logger import get_logger

logger = get_logger(__name__)


# ─── Sentiment helper ────────────────────────────────────────────────────────

def classify_sentiment(text: str) -> tuple[str, float]:
    """
    Analyse the sentiment polarity of `text` using TextBlob.

    Returns:
        (label, score)
        label  → "positive" | "neutral" | "negative"
        score  → float in [-1.0, +1.0]

    Thresholds:
        score >  0.1 → positive
        score < -0.1 → negative
        otherwise    → neutral
    """
    polarity: float = TextBlob(text).sentiment.polarity

    if polarity > 0.1:
        label = "positive"
    elif polarity < -0.1:
        label = "negative"
    else:
        label = "neutral"

    return label, round(polarity, 4)


# ─── Consumer factory ────────────────────────────────────────────────────────

def create_consumer(max_retries: int = 10) -> KafkaConsumer:
    """
    Create a KafkaConsumer with retry logic (same pattern as producer).
    """
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers     = KAFKA_BOOTSTRAP_SERVERS,
                # Deserialise JSON bytes → Python dict automatically
                value_deserializer    = lambda m: json.loads(m.decode("utf-8", errors="replace")),
                auto_offset_reset     = "earliest",    # Process from beginning if no committed offset
                enable_auto_commit    = True,           # Commit offsets automatically
                auto_commit_interval_ms = 5_000,
                group_id              = "crypto-news-consumer-group",
                session_timeout_ms    = 30_000,
                request_timeout_ms    = 40_000,
            )
            logger.info(
                "Kafka consumer connected",
                topic=KAFKA_TOPIC,
                group_id="crypto-news-consumer-group",
            )
            return consumer

        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available yet, retrying...",
                attempt=attempt,
            )
            time.sleep(5)

    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts")


# ─── Consume loop ────────────────────────────────────────────────────────────

def consume_news(consumer: KafkaConsumer) -> None:
    """
    Poll Kafka indefinitely. For each message:
      - Extract headline, source, published_at.
      - Run sentiment analysis.
      - Insert enriched record into PostgreSQL.
    """
    logger.info("Consumer loop started, waiting for messages...")

    for message in consumer:
        try:
            payload: dict = message.value

            headline     = payload.get("headline", "")
            source       = payload.get("source", "unknown")
            raw_ts       = payload.get("published_at", "")

            # Parse ISO timestamp; fall back to current time if malformed
            try:
                published_at = datetime.fromisoformat(raw_ts)
            except (ValueError, TypeError):
                published_at = datetime.now(timezone.utc)
                logger.warning("Could not parse published_at, using now", raw_ts=raw_ts)

            # Sentiment classification
            sentiment_label, sentiment_score = classify_sentiment(headline)

            # Persist to database
            insert_news_headline(
                headline        = headline,
                source          = source,
                published_at    = published_at,
                sentiment_label = sentiment_label,
                sentiment_score = sentiment_score,
            )

            logger.info(
                "Message consumed and stored",
                offset          = message.offset,
                partition       = message.partition,
                headline_preview= headline[:60],
                sentiment       = sentiment_label,
                score           = sentiment_score,
            )

        except Exception as exc:
            # Log the error but do NOT crash the consumer loop
            logger.error(
                "Error processing Kafka message",
                error   = str(exc),
                offset  = message.offset,
            )


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    consumer = create_consumer()
    try:
        consume_news(consumer)
    except KeyboardInterrupt:
        logger.info("Consumer stopped by keyboard interrupt")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed cleanly")