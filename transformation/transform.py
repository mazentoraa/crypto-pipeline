"""
transformation/transform.py
"""

from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone

from database.db import (
    fetch_crypto_prices,
    fetch_news_headlines,
    upsert_price_aggregation,
)
from utils.logger import get_logger

logger = get_logger(__name__)

# Columns coming from PostgreSQL as decimal.Decimal — cast these first, always
_PRICE_NUMERIC_COLS = ["price_usd", "market_cap", "volume_24h", "price_change_24h"]
_NEWS_NUMERIC_COLS  = ["sentiment_score"]


def _cast_decimals(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Convert psycopg2 decimal.Decimal columns to float64 in-place.
    Must be called BEFORE handle_null_values, which uses select_dtypes
    and would otherwise treat Decimal columns as generic 'object' columns,
    calling fillna("unknown") on them and corrupting the numeric values.
    """
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def remove_duplicates(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    original_len = len(df)
    df = df.drop_duplicates(subset=subset, keep="last").reset_index(drop=True)
    removed = original_len - len(df)
    if removed > 0:
        logger.info("Duplicates removed", removed=removed, remaining=len(df), subset=subset)
    else:
        logger.info("No duplicates found", subset=subset)
    return df


def handle_null_values(df: pd.DataFrame) -> pd.DataFrame:
    null_summary = df.isnull().sum()
    null_cols = null_summary[null_summary > 0].to_dict()
    if null_cols:
        logger.info("Null values detected before cleaning", null_columns=null_cols)

    numeric_cols = df.select_dtypes(include=["float64", "int64", "float32", "int32"]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    string_cols = df.select_dtypes(include=["object"]).columns
    df[string_cols] = df[string_cols].fillna("unknown")

    remaining_nulls = df[list(numeric_cols) + list(string_cols)].isnull().sum().sum()
    logger.info("Null cleaning completed", remaining_nulls=remaining_nulls)
    return df


def compute_price_aggregations(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("No price data to aggregate")
        return pd.DataFrame()

    df = df.copy()
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
    df["date"] = df["fetched_at"].dt.date

    agg = (
        df.groupby(["coin_id", "date"])
        .agg(
            avg_price   = ("price_usd",  "mean"),
            max_price   = ("price_usd",  "max"),
            min_price   = ("price_usd",  "min"),
            avg_volume  = ("volume_24h", "mean"),
            data_points = ("price_usd",  "count"),
        )
        .reset_index()
    )

    # Guaranteed float64 — groupby can preserve object dtype in edge cases
    for col in ["avg_price", "max_price", "min_price", "avg_volume"]:
        agg[col] = agg[col].astype(float)

    agg["price_range"]    = agg["max_price"] - agg["min_price"]
    agg["volatility_pct"] = (
        (agg["price_range"] / agg["avg_price"].replace(0, float("nan"))) * 100
    ).fillna(0).round(4)

    logger.info("Price aggregations computed", rows=len(agg), unique_coins=agg["coin_id"].nunique())
    return agg


def compute_sentiment_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        logger.warning("No news data for sentiment metrics")
        return {}

    total  = len(df)
    counts = df["sentiment_label"].value_counts().to_dict()

    metrics = {
        "total_headlines":     total,
        "positive_count":      int(counts.get("positive", 0)),
        "neutral_count":       int(counts.get("neutral",  0)),
        "negative_count":      int(counts.get("negative", 0)),
        "avg_sentiment_score": float(df["sentiment_score"].mean()),
        "computed_at":         datetime.now(timezone.utc).isoformat(),
    }
    for key in ("positive", "neutral", "negative"):
        metrics[f"{key}_pct"] = round(metrics[f"{key}_count"] / total * 100, 2)

    logger.info("Sentiment metrics computed", **metrics)
    return metrics


def save_aggregations(agg_df: pd.DataFrame) -> int:
    if agg_df.empty:
        logger.warning("No aggregation data to save")
        return 0

    saved = 0
    for _, row in agg_df.iterrows():
        try:
            upsert_price_aggregation(
                coin_id     = row["coin_id"],
                date        = row["date"],
                avg_price   = float(row["avg_price"]),
                max_price   = float(row["max_price"]),
                min_price   = float(row["min_price"]),
                avg_volume  = float(row["avg_volume"]),
                data_points = int(row["data_points"]),
            )
            saved += 1
        except Exception as exc:
            logger.error("Failed to save aggregation row", coin_id=row["coin_id"], error=str(exc))

    logger.info("Aggregations saved to database", rows_saved=saved)
    return saved


def run_transformations() -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Transformation pipeline started")

    # ── Crypto prices ─────────────────────────────────────────────────────────
    raw_prices = fetch_crypto_prices(limit=1000)
    prices_df  = pd.DataFrame(raw_prices) if raw_prices else pd.DataFrame()

    if not prices_df.empty:
        # cast Decimals to float BEFORE handle_null_values runs
        prices_df = _cast_decimals(prices_df, _PRICE_NUMERIC_COLS)
        prices_df = remove_duplicates(prices_df, subset=["coin_id", "fetched_at"])
        prices_df = handle_null_values(prices_df)
        agg_df    = compute_price_aggregations(prices_df)
        save_aggregations(agg_df)
    else:
        logger.warning("crypto_prices table is empty — skipping aggregation")

    # ── News headlines ────────────────────────────────────────────────────────
    raw_news = fetch_news_headlines(limit=500)
    news_df  = pd.DataFrame(raw_news) if raw_news else pd.DataFrame()

    if not news_df.empty:
        news_df = _cast_decimals(news_df, _NEWS_NUMERIC_COLS)
        news_df = remove_duplicates(news_df, subset=["headline", "published_at"])
        news_df = handle_null_values(news_df)
        compute_sentiment_metrics(news_df)
    else:
        logger.warning("news_headlines table is empty — skipping sentiment metrics")

    logger.info(
        "Transformation pipeline completed",
        price_rows=len(prices_df),
        news_rows=len(news_df),
    )
    return prices_df, news_df


if __name__ == "__main__":
    run_transformations()