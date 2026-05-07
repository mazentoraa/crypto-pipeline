"""
database/db.py
──────────────
Database connection helpers and data-access functions.

Design decisions:
- Each function opens and closes its own connection (simple & safe for small scale).
- RealDictCursor returns rows as dict-like objects, not plain tuples.
- The context manager automatically commits on success and rolls back on error.
- All functions use parameterised queries to prevent SQL injection.
"""

from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from utils.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
)
from utils.logger import get_logger

logger = get_logger(__name__)


# ─── Connection ──────────────────────────────────────────────────────────────

def get_connection() -> psycopg2.extensions.connection:
    """Open a new database connection using environment-configured credentials."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )


@contextmanager
def get_db_cursor() -> Generator[RealDictCursor, None, None]:
    """
    Context manager that yields a database cursor.
    Automatically commits on success, rolls back on any exception,
    and always closes the connection.

    Usage:
        with get_db_cursor() as cur:
            cur.execute("SELECT 1")
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            yield cursor
        conn.commit()
    except Exception as exc:
        conn.rollback()
        logger.error("Database transaction rolled back", error=str(exc))
        raise
    finally:
        conn.close()


# ─── Write helpers ───────────────────────────────────────────────────────────

def insert_crypto_price(
    coin_id: str,
    symbol: str,
    price_usd: float | None,
    market_cap: float | None,
    volume_24h: float | None,
    price_change_24h: float | None,
) -> None:
    """Insert one row into the crypto_prices table."""
    sql = """
        INSERT INTO crypto_prices
            (coin_id, symbol, price_usd, market_cap, volume_24h, price_change_24h)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    with get_db_cursor() as cur:
        cur.execute(sql, (coin_id, symbol, price_usd, market_cap, volume_24h, price_change_24h))

    logger.info(
        "Crypto price inserted",
        coin_id=coin_id,
        price_usd=price_usd,
    )


def insert_news_headline(
    headline: str,
    source: str,
    published_at: datetime,
    sentiment_label: str,
    sentiment_score: float,
) -> None:
    """Insert one row into the news_headlines table."""
    sql = """
        INSERT INTO news_headlines
            (headline, source, published_at, sentiment_label, sentiment_score)
        VALUES (%s, %s, %s, %s, %s)
    """
    with get_db_cursor() as cur:
        cur.execute(sql, (headline, source, published_at, sentiment_label, sentiment_score))

    logger.info(
        "News headline inserted",
        source=source,
        sentiment_label=sentiment_label,
        score=sentiment_score,
    )


def upsert_price_aggregation(
    coin_id: str,
    date: Any,
    avg_price: float,
    max_price: float,
    min_price: float,
    avg_volume: float,
    data_points: int,
) -> None:
    """
    Insert a daily aggregation row.
    If a row already exists for (coin_id, date), update it in place.
    """
    sql = """
        INSERT INTO price_aggregations
            (coin_id, date, avg_price, max_price, min_price, avg_volume, data_points)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (coin_id, date) DO UPDATE
            SET avg_price   = EXCLUDED.avg_price,
                max_price   = EXCLUDED.max_price,
                min_price   = EXCLUDED.min_price,
                avg_volume  = EXCLUDED.avg_volume,
                data_points = EXCLUDED.data_points,
                computed_at = NOW()
    """
    with get_db_cursor() as cur:
        cur.execute(sql, (coin_id, date, avg_price, max_price, min_price, avg_volume, data_points))


# ─── Read helpers ────────────────────────────────────────────────────────────

def fetch_crypto_prices(limit: int = 500) -> list[dict]:
    """Return the most recent `limit` rows from crypto_prices."""
    sql = "SELECT * FROM crypto_prices ORDER BY fetched_at DESC LIMIT %s"
    with get_db_cursor() as cur:
        cur.execute(sql, (limit,))
        return [dict(row) for row in cur.fetchall()]


def fetch_latest_prices() -> list[dict]:
    """Return the single most-recent row per coin (DISTINCT ON)."""
    sql = """
        SELECT DISTINCT ON (coin_id)
            coin_id, symbol, price_usd, market_cap, volume_24h,
            price_change_24h, fetched_at
        FROM crypto_prices
        ORDER BY coin_id, fetched_at DESC
    """
    with get_db_cursor() as cur:
        cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]


def fetch_news_headlines(limit: int = 100) -> list[dict]:
    """Return the most recent `limit` news headlines."""
    sql = "SELECT * FROM news_headlines ORDER BY created_at DESC LIMIT %s"
    with get_db_cursor() as cur:
        cur.execute(sql, (limit,))
        return [dict(row) for row in cur.fetchall()]


def fetch_price_aggregations(limit: int = 200) -> list[dict]:
    """Return the most recent price aggregation rows."""
    sql = "SELECT * FROM price_aggregations ORDER BY date DESC LIMIT %s"
    with get_db_cursor() as cur:
        cur.execute(sql, (limit,))
        return [dict(row) for row in cur.fetchall()]