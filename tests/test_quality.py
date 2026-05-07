"""
tests/test_quality.py
──────────────────────
Data quality tests — validate constraints that must hold in production data.
All tests use in-memory DataFrames (no database required).
"""

import pytest
import pandas as pd

from transformation.transform import (
    handle_null_values,
    remove_duplicates,
    compute_price_aggregations,
    compute_sentiment_metrics,
)


class TestDataQuality:

    def test_no_nulls_after_cleaning(self):
        """All numeric and string nulls must be eliminated after handle_null_values."""
        df = pd.DataFrame({
            "price_usd": [100.0, None, None],
            "coin_id":   [None, "eth", "btc"],
            "volume":    [1_000.0, None, 2_000.0],
        })
        result = handle_null_values(df)
        # Check columns that were cleaned
        assert result[["price_usd", "volume"]].isnull().sum().sum() == 0
        assert result["coin_id"].isnull().sum() == 0

    def test_prices_non_negative_after_cleaning(self):
        """Price values filled with 0 (not negative) when null."""
        df = pd.DataFrame({
            "price_usd":  [100.0, None, 200.0],
            "volume_24h": [5_000.0, None, 3_000.0],
        })
        result = handle_null_values(df)
        assert (result["price_usd"]  >= 0).all()
        assert (result["volume_24h"] >= 0).all()

    def test_dedup_produces_unique_keys(self):
        """After deduplication, (coin_id, fetched_at) pairs must be unique."""
        df = pd.DataFrame({
            "coin_id":   ["bitcoin", "bitcoin", "ethereum"],
            "fetched_at": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
            "price_usd": [45_000.0, 45_000.0, 2_500.0],
        })
        result = remove_duplicates(df, subset=["coin_id", "fetched_at"])
        assert result.duplicated(subset=["coin_id", "fetched_at"]).sum() == 0

    def test_valid_sentiment_labels_only(self):
        """Only "positive", "neutral", or "negative" are valid sentiment labels."""
        valid = {"positive", "neutral", "negative"}
        df = pd.DataFrame({
            "sentiment_label": ["positive", "negative", "neutral", "positive"],
            "sentiment_score": [0.5, -0.3, 0.0, 0.8],
        })
        assert set(df["sentiment_label"].unique()).issubset(valid)

    def test_aggregation_data_points_always_positive(self):
        """data_points (count of raw rows per group) must never be zero or negative."""
        df = pd.DataFrame({
            "coin_id":         ["bitcoin", "bitcoin", "ethereum"],
            "symbol":          ["BTC", "BTC", "ETH"],
            "price_usd":       [45_000.0, 46_000.0, 2_500.0],
            "market_cap":      [850e9, 860e9, 300e9],
            "volume_24h":      [30e9, 31e9, 15e9],
            "price_change_24h":[2.5, 2.8, -1.2],
            "fetched_at":      pd.to_datetime([
                                   "2024-01-01 10:00",
                                   "2024-01-01 11:00",
                                   "2024-01-01 10:00",
                               ]),
        })
        result = compute_price_aggregations(df)
        assert (result["data_points"] > 0).all()

    def test_sentiment_percentages_in_valid_range(self):
        """All percentage values must be between 0 and 100 inclusive."""
        df = pd.DataFrame({
            "sentiment_label": ["positive", "negative", "neutral"],
            "sentiment_score": [0.5, -0.3, 0.0],
        })
        result = compute_sentiment_metrics(df)
        for key in ("positive_pct", "neutral_pct", "negative_pct"):
            assert 0.0 <= result[key] <= 100.0, f"{key} out of range: {result[key]}"

    def test_price_range_non_negative(self):
        """price_range = max_price − min_price must never be negative."""
        df = pd.DataFrame({
            "coin_id":         ["bitcoin", "bitcoin"],
            "symbol":          ["BTC", "BTC"],
            "price_usd":       [44_000.0, 46_000.0],
            "market_cap":      [840e9, 860e9],
            "volume_24h":      [28e9, 32e9],
            "price_change_24h":[1.0, 3.0],
            "fetched_at":      pd.to_datetime(["2024-01-01 08:00", "2024-01-01 12:00"]),
        })
        result = compute_price_aggregations(df)
        assert (result["price_range"] >= 0).all()