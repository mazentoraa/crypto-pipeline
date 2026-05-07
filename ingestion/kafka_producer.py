"""
ingestion/kafka_producer.py
────────────────────────────
Kafka producer that streams simulated crypto news headlines.

- Publishes one headline every ~3 seconds (configurable).
- Randomly selects from a realistic headline pool.
- Messages are JSON-serialised before being sent.
- Implements retry logic on connection failure.

Run this in its own terminal:
    python -m ingestion.kafka_producer
"""

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from utils.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from utils.logger import get_logger

logger = get_logger(__name__)

# ─── Simulated news pool ─────────────────────────────────────────────────────
# These represent the kind of headlines we'd get from a crypto news RSS feed.
SAMPLE_HEADLINES: list[dict] = [
    {"headline": "Bitcoin surges past $50,000 as institutional demand rises sharply", "source": "CryptoNews"},
    {"headline": "Ethereum's latest upgrade dramatically improves network transaction speeds", "source": "CoinDesk"},
    {"headline": "Regulatory uncertainty continues to weigh on cryptocurrency markets", "source": "Reuters"},
    {"headline": "Major global bank announces Bitcoin custody services for clients", "source": "Bloomberg"},
    {"headline": "Decentralised finance protocols record all-time high trading volumes", "source": "CoinTelegraph"},
    {"headline": "Solana network experiences brief outage but recovers within hours", "source": "CryptoNews"},
    {"headline": "SEC approves additional Bitcoin ETF applications from asset managers", "source": "Bloomberg"},
    {"headline": "Crypto adoption accelerating rapidly in emerging economies", "source": "Reuters"},
    {"headline": "NFT marketplace activity shows strong recovery signals after prolonged slump", "source": "CoinDesk"},
    {"headline": "Cardano completes major smart contract scalability upgrade on schedule", "source": "CoinTelegraph"},
    {"headline": "Litecoin halving event triggers renewed price speculation among traders", "source": "CryptoNews"},
    {"headline": "Central banks worldwide accelerating digital currency research and pilots", "source": "Reuters"},
    {"headline": "Crypto exchange reports record-breaking new user registrations this quarter", "source": "Bloomberg"},
    {"headline": "Bitcoin mining difficulty reaches all-time high as hash rate expands", "source": "CoinDesk"},
    {"headline": "Ethereum gas fees fall significantly following successful network upgrade", "source": "CoinTelegraph"},
    {"headline": "Investors cautious as crypto market faces macroeconomic headwinds", "source": "Reuters"},
    {"headline": "Solana breaks developer activity records with surge in new projects", "source": "CryptoNews"},
    {"headline": "Widespread crypto adoption grows as payment processors expand support", "source": "Bloomberg"},
    {"headline": "Market analysts warn of potential volatility ahead of Federal Reserve decision", "source": "CoinDesk"},
    {"headline": "Bitcoin on-chain metrics suggest strong accumulation by long-term holders", "source": "CoinTelegraph"},
]

# ─── Producer factory ────────────────────────────────────────────────────────

def create_producer(max_retries: int = 10) -> KafkaProducer:
    """
    Create a KafkaProducer with retry logic.
    Retries up to `max_retries` times with a 5-second delay between attempts.
    This is important because Kafka may not be ready immediately after Docker starts.
    """
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers  = KAFKA_BOOTSTRAP_SERVERS,
                # Serialise Python dict → JSON bytes automatically
                value_serializer   = lambda v: json.dumps(v).encode("utf-8"),
                # acks="all" waits for leader + replicas — safer delivery guarantee
                acks               = "all",
                retries            = 3,
                request_timeout_ms = 10_000,
            )
            logger.info(
                "Kafka producer connected",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                topic=KAFKA_TOPIC,
            )
            return producer

        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available yet, retrying...",
                attempt=attempt,
                max_retries=max_retries,
            )
            time.sleep(5)

    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts")


# ─── Produce loop ────────────────────────────────────────────────────────────

def produce_news(producer: KafkaProducer, interval_seconds: float = 3.0) -> None:
    """
    Continuously publish news headlines to Kafka.

    Each message payload:
        {
          "headline":     "Bitcoin surges...",
          "source":       "CryptoNews",
          "published_at": "2024-01-15T10:30:00+00:00",
          "msg_id":       4821
        }
    """
    logger.info(
        "Producer loop started",
        topic=KAFKA_TOPIC,
        interval_seconds=interval_seconds,
    )

    while True:
        try:
            # Pick a random headline and add metadata
            item = random.choice(SAMPLE_HEADLINES).copy()
            item["published_at"] = datetime.now(timezone.utc).isoformat()
            item["msg_id"]       = random.randint(10_000, 99_999)

            # Send to Kafka — .get() blocks until broker confirms receipt
            future   = producer.send(KAFKA_TOPIC, value=item)
            metadata = future.get(timeout=10)

            logger.info(
                "Message published",
                topic     = metadata.topic,
                partition = metadata.partition,
                offset    = metadata.offset,
                headline  = item["headline"][:60] + "...",
            )

        except Exception as exc:
            logger.error("Failed to publish message", error=str(exc))
            time.sleep(5)   # Brief pause before retrying

        time.sleep(interval_seconds)


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    producer = create_producer()
    try:
        produce_news(producer, interval_seconds=3.0)
    except KeyboardInterrupt:
        logger.info("Producer stopped by keyboard interrupt")
    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed cleanly")