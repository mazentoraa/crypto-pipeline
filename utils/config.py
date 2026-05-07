"""
utils/config.py
───────────────
Loads all environment variables from the .env file and exposes them
as typed Python constants. Import from this module throughout the project.
"""

import os
from dotenv import load_dotenv

# Load .env file if present (does nothing in Docker where env vars are injected)
load_dotenv()

# ─── PostgreSQL ───────────────────────────────────────────────────────────
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB", "crypto_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres123")

# ─── Kafka ────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "crypto-news")

# ─── CoinGecko API ───────────────────────────────────────────────────────
COINGECKO_API_URL = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3")

# Coins to track — these are CoinGecko's canonical IDs
COINS = ["bitcoin", "ethereum", "litecoin", "cardano", "solana"]

# Human-readable symbol map
COIN_SYMBOLS: dict[str, str] = {
    "bitcoin":  "BTC",
    "ethereum": "ETH",
    "litecoin": "LTC",
    "cardano":  "ADA",
    "solana":   "SOL",
}

# ─── Intervals ───────────────────────────────────────────────────────────
BATCH_INTERVAL_SECONDS = int(os.getenv("BATCH_INTERVAL_SECONDS", "300"))