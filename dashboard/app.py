"""
dashboard/app.py
─────────────────
Streamlit analytics dashboard for the crypto pipeline.

Sections:
  1. Live price KPI cards (latest price per coin with 24h delta)
  2. Price history line chart (filterable by coin)
  3. Market cap bar chart comparison
  4. Sentiment distribution pie chart
  5. Latest headlines list with colour-coded sentiment
  6. Daily aggregations table

Access at: http://localhost:8501
Data refreshes every 60 seconds (Streamlit cache TTL).
"""

import sys
import os

# Ensure project root is on the path when running via `streamlit run`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime

from database.db import (
    fetch_crypto_prices,
    fetch_latest_prices,
    fetch_news_headlines,
    fetch_price_aggregations,
)
from utils.logger import get_logger

logger = get_logger(__name__)

# ─── Page configuration ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto & News Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card { padding: 1rem; border-radius: 8px; background: #1e1e2e; }
    .stMetric label { font-size: 0.85rem !important; color: #aaa; }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ─── Cached data loaders ──────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def load_prices() -> pd.DataFrame:
    rows = fetch_crypto_prices(limit=1000)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_latest() -> pd.DataFrame:
    rows = fetch_latest_prices()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_news() -> pd.DataFrame:
    rows = fetch_news_headlines(limit=200)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_agg() -> pd.DataFrame:
    rows = fetch_price_aggregations(limit=300)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ─── Header ───────────────────────────────────────────────────────────────────
st.title("📊 Crypto & News Analytics Dashboard")
st.caption(
    "Live data from CoinGecko API (batch) and simulated news stream (Kafka). "
    "Refreshes every 60 seconds."
)

# Manual refresh button
col_r, col_ts = st.columns([1, 5])
with col_r:
    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()
with col_ts:
    st.markdown(f"*Last page load: {datetime.now().strftime('%H:%M:%S')}*")

st.divider()

# ─── Load all data ────────────────────────────────────────────────────────────
with st.spinner("Loading data..."):
    prices_df = load_prices()
    latest_df = load_latest()
    news_df   = load_news()
    agg_df    = load_agg()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: LIVE PRICE KPIs
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("💰 Live Prices")

if latest_df.empty:
    st.info("⏳ No price data yet. Run the batch ingestion first:\n\n`python -m ingestion.batch_ingest`")
else:
    kpi_cols = st.columns(len(latest_df))
    for i, (_, row) in enumerate(latest_df.iterrows()):
        change = float(row.get("price_change_24h") or 0)
        with kpi_cols[i]:
            st.metric(
                label  = f"{row['symbol']} · {row['coin_id'].capitalize()}",
                value  = f"${float(row['price_usd']):,.2f}",
                delta  = f"{change:+.2f}%",
            )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 & 3: PRICE HISTORY + MARKET CAP
# ─────────────────────────────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.subheader("📈 Price History")
    if prices_df.empty:
        st.info("No historical price data available.")
    else:
        prices_df["fetched_at"] = pd.to_datetime(prices_df["fetched_at"], utc=True)
        coin_options = sorted(prices_df["coin_id"].unique().tolist())
        selected_coin = st.selectbox("Select coin", coin_options, key="history_coin")
        coin_hist = prices_df[prices_df["coin_id"] == selected_coin].sort_values("fetched_at")

        fig = px.line(
            coin_hist,
            x      = "fetched_at",
            y      = "price_usd",
            title  = f"{selected_coin.capitalize()} (USD)",
            labels = {"fetched_at": "Time", "price_usd": "Price (USD)"},
        )
        fig.update_layout(margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("🏦 Market Cap Comparison")
    if latest_df.empty:
        st.info("No market cap data available.")
    else:
        fig2 = px.bar(
            latest_df,
            x      = "symbol",
            y      = "market_cap",
            color  = "symbol",
            title  = "Market Capitalisation by Coin",
            labels = {"symbol": "Coin", "market_cap": "Market Cap (USD)"},
            text_auto = ".2s",
        )
        fig2.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 & 5: SENTIMENT ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("📰 News Sentiment Analysis")

if news_df.empty:
    st.info(
        "⏳ No news data yet. Start the Kafka pipeline:\n\n"
        "```\npython -m ingestion.kafka_producer   # terminal 1\n"
        "python -m ingestion.kafka_consumer   # terminal 2\n```"
    )
else:
    sentiment_counts = news_df["sentiment_label"].value_counts()
    total_news = len(news_df)

    # KPI row
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("Total Headlines", total_news)
    with k2:
        pos = int(sentiment_counts.get("positive", 0))
        st.metric("🟢 Positive", pos, f"{pos/total_news*100:.1f}%")
    with k3:
        neu = int(sentiment_counts.get("neutral", 0))
        st.metric("⚪ Neutral", neu, f"{neu/total_news*100:.1f}%")
    with k4:
        neg = int(sentiment_counts.get("negative", 0))
        st.metric("🔴 Negative", neg, f"{neg/total_news*100:.1f}%")

    pie_col, headlines_col = st.columns([1, 2])

    with pie_col:
        fig3 = px.pie(
            values = sentiment_counts.values,
            names  = sentiment_counts.index,
            title  = "Sentiment Distribution",
            color  = sentiment_counts.index,
            color_discrete_map = {
                "positive": "#2ecc71",
                "neutral":  "#95a5a6",
                "negative": "#e74c3c",
            },
            hole = 0.35,   # Donut chart
        )
        fig3.update_layout(margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig3, use_container_width=True)

    with headlines_col:
        st.markdown("**Latest Headlines**")
        emoji_map = {"positive": "🟢", "neutral": "⚪", "negative": "🔴"}
        for _, row in news_df.head(12).iterrows():
            emoji = emoji_map.get(str(row.get("sentiment_label")), "⚪")
            score = float(row.get("sentiment_score") or 0)
            headline_text = str(row.get("headline", ""))[:90]
            st.markdown(
                f"{emoji} {headline_text}  "
                f"<span style='color:gray;font-size:0.8em'>score: {score:.2f} · "
                f"{str(row.get('source',''))}</span>",
                unsafe_allow_html=True,
            )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6: DAILY AGGREGATIONS TABLE
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("📋 Daily Price Aggregations")

if agg_df.empty:
    st.info(
        "⏳ No aggregation data yet. Run the transformation pipeline:\n\n"
        "`python -m transformation.transform`"
    )
else:
    # Format numeric columns for readability
    display_agg = agg_df.copy()
    for col in ["avg_price", "max_price", "min_price"]:
        if col in display_agg.columns:
            display_agg[col] = display_agg[col].apply(
                lambda x: f"${float(x):,.2f}" if x is not None else "-"
            )
    if "avg_volume" in display_agg.columns:
        display_agg["avg_volume"] = display_agg["avg_volume"].apply(
            lambda x: f"${float(x):,.0f}" if x is not None else "-"
        )

    st.dataframe(display_agg, use_container_width=True, hide_index=True)

# ─── Volume over time chart ────────────────────────────────────────────────
if not prices_df.empty:
    st.subheader("📊 24h Trading Volume Over Time")
    coin_opts2 = sorted(prices_df["coin_id"].unique().tolist())
    vol_coin = st.selectbox("Select coin", coin_opts2, key="vol_coin")
    vol_df = prices_df[prices_df["coin_id"] == vol_coin].sort_values("fetched_at")

    fig4 = px.area(
        vol_df,
        x      = "fetched_at",
        y      = "volume_24h",
        title  = f"{vol_coin.capitalize()} — 24h Volume",
        labels = {"fetched_at": "Time", "volume_24h": "Volume (USD)"},
        color_discrete_sequence=["#3498db"],
    )
    fig4.update_layout(margin=dict(t=40, b=0, l=0, r=0))
    st.plotly_chart(fig4, use_container_width=True)

# ─── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "**Crypto & News Analytics Pipeline** · Built with Python, PostgreSQL, Kafka, Prefect & Streamlit  \n"
    "Data source: [CoinGecko API](https://www.coingecko.com/en/api) (free tier) · "
    f"Dashboard rendered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
)