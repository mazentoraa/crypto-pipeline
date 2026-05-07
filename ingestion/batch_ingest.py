"""
Batch ingestion module — fetches cryptocurrency prices from the
CoinGecko public API and inserts them into PostgreSQL.

Key behaviours:
- Requests up to 5 coins in a single API call (efficient).
- Retries up to 3 times with exponential back-off on network failures.
- Logs every step with structured JSON output.

Can be run standalone:
    python -m ingestion.batch_ingest

Or called from the Prefect orchestration flow.
"""

import time
from typing import Any

import requests

from database.db import insert_crypto_price
from utils.config import COINGECKO_API_URL, COINS, COIN_SYMBOLS
from utils.logger import get_logger

logger = get_logger(__name__)

# ─── Constants ───────────────────────────────────────────────────────────────
MAX_RETRIES    = 3
REQUEST_TIMEOUT = 15   # seconds before giving up on a single HTTP call


# ─── API fetch ───────────────────────────────────────────────────────────────

def fetch_crypto_data(coins: list[str]) -> dict[str, Any]:
    """
    Call the CoinGecko /simple/price endpoint for a list of coin IDs.

    Returns a dict like:
        {
          "bitcoin":  {"usd": 45000, "usd_market_cap": 850e9, ...},
          "ethereum": {"usd": 2500,  ...},
          ...
        }

    Raises RuntimeError if all retry attempts fail.
    """
    url = f"{COINGECKO_API_URL}/simple/price"
    params = {
        "ids":               ",".join(coins),
        "vs_currencies":     "usd",
        "include_market_cap":  "true",
        "include_24hr_vol":    "true",
        "include_24hr_change": "true",
    }

    logger.info("Calling CoinGecko API", coins=coins, url=url)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()          # Raise for 4xx / 5xx responses
            data: dict = response.json()
            logger.info("API response received", coin_count=len(data), attempt=attempt)
            return data

        except requests.exceptions.HTTPError as exc:
            # 429 = rate limited; back off longer
            wait = 2 ** attempt if response.status_code != 429 else 60
            logger.warning(
                "HTTP error from CoinGecko",
                status_code=response.status_code,
                attempt=attempt,
                retry_in=wait,
            )
        except requests.exceptions.RequestException as exc:
            wait = 2 ** attempt
            logger.warning(
                "Network error fetching crypto data",
                error=str(exc),
                attempt=attempt,
                retry_in=wait,
            )

        if attempt < MAX_RETRIES:
            time.sleep(wait)

    raise RuntimeError(f"CoinGecko API unreachable after {MAX_RETRIES} attempts")


# ─── Processing ──────────────────────────────────────────────────────────────

def process_and_store(api_response: dict[str, Any]) -> int:
    """
    Iterate over the API response dict, extract fields, and write to PostgreSQL.

    Returns the number of rows successfully inserted.
    """
    inserted = 0
    for coin_id, values in api_response.items():
        try:
            insert_crypto_price(
                coin_id          = coin_id,
                symbol           = COIN_SYMBOLS.get(coin_id, coin_id.upper()),
                price_usd        = values.get("usd"),
                market_cap       = values.get("usd_market_cap"),
                volume_24h       = values.get("usd_24h_vol"),
                price_change_24h = values.get("usd_24h_change"),
            )
            inserted += 1
        except Exception as exc:
            # Log failure for this coin but continue with the others
            logger.error(
                "Failed to insert coin data",
                coin_id=coin_id,
                error=str(exc),
            )

    return inserted


# ─── Entry point ─────────────────────────────────────────────────────────────

def run_batch_ingestion() -> int:
    """
    Main entry point called by the Prefect flow and by direct invocation.
    Fetches data for all configured coins and stores them.
    Returns the number of rows inserted.
    """
    logger.info("Batch ingestion started", coins=COINS)

    api_data   = fetch_crypto_data(COINS)
    rows_saved = process_and_store(api_data)

    logger.info("Batch ingestion completed", rows_inserted=rows_saved)
    return rows_saved


if __name__ == "__main__":
    run_batch_ingestion()