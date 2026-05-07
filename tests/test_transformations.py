"""
tests/test_transformations.py
──────────────────────────────
Unit tests for every transformation function.
These tests use only in-memory DataFrames — no database connection required.
"""

import pytest
import pandas as pd

from transformation.transform import (
    remove_duplicates,
    handle_null_values,
    compute_price_aggregations,
    compute_sentiment_metrics,
)


# ─── Shared fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def prices_with_duplicates() -> pd.DataFrame:
    """Four rows where rows 3 and 4 are exact duplicates (same coin + timestamp)."""
    return pd.DataFrame({
        "id":              [1,          2,           3,           3],
        "coin_id":         ["bitcoin",  "ethereum",  "bitcoin",   "bitcoin"],
        "symbol":          ["BTC",      "ETH",       "BTC",       "BTC"],
        "price_usd":       [45_000.0,   2_500.0,     45_100.0,    45_100.0],
        "market_cap":      [850e9,      300e9,       852e9,       852e9],
        "volume_24h":      [30e9,       15e9,        31e9,        31e9],
        "price_change_24h":[2.5,        -1.2,        2.8,         2.8],
        "fetched_at":      pd.to_datetime([
                               "2024-01-01 10:00",
                               "2024-01-01 10:00",
                               "2024-01-01 11:00",
                               "2024-01-01 11:00",
                           ]),
    })


@pytest.fixture
def prices_no_duplicates() -> pd.DataFrame:
    """Three unique rows — one per coin."""
    return pd.DataFrame({
        "coin_id":  ["bitcoin", "ethereum", "litecoin"],
        "price_usd": [45_000.0, 2_500.0, 75.0],
        "fetched_at": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
    })


@pytest.fixture
def prices_with_nulls() -> pd.DataFrame:
    return pd.DataFrame({
        "price_usd":  [100.0, None, 200.0],
        "coin_id":    ["bitcoin", None, "ethereum"],
        "volume_24h": [None, 5_000.0, 6_000.0],
    })


@pytest.fixture
def prices_multiday() -> pd.DataFrame:
    """Two coins over two days — useful for aggregation tests."""
    return pd.DataFrame({
        "coin_id":         ["bitcoin", "bitcoin", "bitcoin", "ethereum", "ethereum"],
        "symbol":          ["BTC", "BTC", "BTC", "ETH", "ETH"],
        "price_usd":       [44_000.0, 45_000.0, 46_000.0, 2_400.0, 2_600.0],
        "market_cap":      [840e9, 850e9, 860e9, 290e9, 310e9],
        "volume_24h":      [28e9, 30e9, 32e9, 14e9, 16e9],
        "price_change_24h":[1.0, 2.5, 3.0, -1.0, 2.0],
        "fetched_at":      pd.to_datetime([
                               "2024-01-01 08:00",
                               "2024-01-01 12:00",
                               "2024-01-02 08:00",
                               "2024-01-01 08:00",
                               "2024-01-01 12:00",
                           ]),
    })


@pytest.fixture
def news_df() -> pd.DataFrame:
    return pd.DataFrame({
        "id":              [1, 2, 3, 4, 5],
        "headline":        [
            "Bitcoin surges to new highs",
            "Crypto market faces regulatory pressure",
            "Ethereum upgrade successful",
            "Market remains stable today",
            "DeFi protocols report security issues",
        ],
        "source":          ["CryptoNews", "Reuters", "CoinDesk", "Bloomberg", "CoinTelegraph"],
        "sentiment_label": ["positive", "negative", "positive", "neutral", "negative"],
        "sentiment_score": [0.5, -0.3, 0.6, 0.0, -0.4],
        "created_at":      pd.to_datetime(["2024-01-01 10:00"] * 5),
    })


# ─── Test: remove_duplicates ─────────────────────────────────────────────────

class TestRemoveDuplicates:

    def test_removes_exact_duplicates(self, prices_with_duplicates):
        """Should reduce 4 rows to 3 by removing the duplicate bitcoin@11:00 row."""
        result = remove_duplicates(prices_with_duplicates, subset=["coin_id", "fetched_at"])
        assert len(result) == 3

    def test_no_rows_removed_when_no_duplicates(self, prices_no_duplicates):
        """DataFrame with unique rows should remain unchanged."""
        result = remove_duplicates(prices_no_duplicates, subset=["coin_id"])
        assert len(result) == 3

    def test_keeps_last_occurrence(self, prices_with_duplicates):
        """
        The 'keep=last' strategy means if there are two rows with the same key,
        the later-indexed row survives. Both duplicate rows have the same values
        so the assertion checks the count, not a specific value.
        """
        result = remove_duplicates(prices_with_duplicates, subset=["coin_id", "fetched_at"])
        btc_rows = result[result["coin_id"] == "bitcoin"]
        assert len(btc_rows) == 2   # one at 10:00, one at 11:00

    def test_returns_dataframe(self, prices_with_duplicates):
        result = remove_duplicates(prices_with_duplicates, subset=["coin_id"])
        assert isinstance(result, pd.DataFrame)


# ─── Test: handle_null_values ────────────────────────────────────────────────

class TestHandleNullValues:

    def test_no_nulls_remain_in_numeric_columns(self, prices_with_nulls):
        result = handle_null_values(prices_with_nulls)
        assert result["price_usd"].isnull().sum() == 0
        assert result["volume_24h"].isnull().sum() == 0

    def test_numeric_nulls_filled_with_zero(self, prices_with_nulls):
        result = handle_null_values(prices_with_nulls)
        assert result["price_usd"].iloc[1] == 0.0
        assert result["volume_24h"].iloc[0] == 0.0

    def test_string_nulls_filled_with_unknown(self, prices_with_nulls):
        result = handle_null_values(prices_with_nulls)
        assert result["coin_id"].isnull().sum() == 0
        assert result["coin_id"].iloc[1] == "unknown"

    def test_no_nulls_in_already_clean_dataframe(self, prices_no_duplicates):
        result = handle_null_values(prices_no_duplicates)
        # No exception should be raised and no nulls should appear
        assert result.isnull().sum().sum() == 0


# ─── Test: compute_price_aggregations ────────────────────────────────────────

class TestComputePriceAggregations:

    def test_returns_expected_columns(self, prices_multiday):
        result = compute_price_aggregations(prices_multiday)
        for col in ["coin_id", "date", "avg_price", "max_price", "min_price",
                    "avg_volume", "data_points", "price_range", "volatility_pct"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_max_gte_min(self, prices_multiday):
        result = compute_price_aggregations(prices_multiday)
        assert (result["max_price"] >= result["min_price"]).all()

    def test_data_points_positive(self, prices_multiday):
        result = compute_price_aggregations(prices_multiday)
        assert (result["data_points"] > 0).all()

    def test_empty_dataframe_returns_empty(self):
        result = compute_price_aggregations(pd.DataFrame())
        assert result.empty

    def test_bitcoin_aggregation_values(self, prices_multiday):
        """Bitcoin has 3 price points: 44000, 45000, 46000 across two days."""
        result = compute_price_aggregations(prices_multiday)
        btc = result[result["coin_id"] == "bitcoin"]
        # Total data points across all days should be 3
        assert btc["data_points"].sum() == 3
        # Max price should be 46000
        assert btc["max_price"].max() == 46_000.0


# ─── Test: compute_sentiment_metrics ─────────────────────────────────────────

class TestComputeSentimentMetrics:

    def test_correct_total_count(self, news_df):
        result = compute_sentiment_metrics(news_df)
        assert result["total_headlines"] == 5

    def test_correct_label_counts(self, news_df):
        result = compute_sentiment_metrics(news_df)
        assert result["positive_count"] == 2
        assert result["negative_count"] == 2
        assert result["neutral_count"]  == 1

    def test_percentages_sum_to_100(self, news_df):
        result = compute_sentiment_metrics(news_df)
        total_pct = result["positive_pct"] + result["negative_pct"] + result["neutral_pct"]
        assert abs(total_pct - 100.0) < 0.01   # floating point tolerance

    def test_avg_score_is_float(self, news_df):
        result = compute_sentiment_metrics(news_df)
        assert isinstance(result["avg_sentiment_score"], float)

    def test_empty_dataframe_returns_empty_dict(self):
        result = compute_sentiment_metrics(pd.DataFrame())
        assert result == {}