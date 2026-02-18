"""
btc_history.py
--------------
Fetches Bitcoin historical price and on-chain volume data
from the Blockchain.info Charts API.
No API key required.

Data collected:
- Market price (USD)
- Estimated transaction volume (USD)
- Transaction count per day
- Hash rate (network security proxy)

Usage:
    python scripts/btc_history.py
"""

import requests
import pandas as pd
import os
import time
from datetime import datetime

BASE_URL = "https://api.blockchain.info/charts"

# ── Chart types to fetch ───────────────────────────────────────────────────
# Full list: https://www.blockchain.com/explorer/api/charts_api
CHARTS = {
    "market_price_usd":     "market-price",           # BTC price in USD
    "tx_volume_usd":        "estimated-transaction-volume-usd",  # On-chain volume
    "tx_count":             "n-transactions",          # Daily transaction count
    "hash_rate":            "hash-rate",               # Network hash rate
    "avg_tx_value_usd":     "estimated-transaction-volume", # Avg tx value
}

# Timespan options: "1year", "2years", "5years", "all"
TIMESPAN = "2years"


def fetch_chart(chart_name: str, chart_type: str, timespan: str = TIMESPAN) -> pd.DataFrame:
    """
    Fetch a single chart's data from Blockchain.info.
    Returns a DataFrame with columns: date, <chart_name>
    """
    url = f"{BASE_URL}/{chart_type}?timespan={timespan}&format=json&cors=true"

    print(f"  Fetching {chart_name}...")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        values = data.get("values", [])
        if not values:
            print(f"  No values returned for {chart_name}")
            return pd.DataFrame()

        records = []
        for point in values:
            records.append({
                "date":       datetime.utcfromtimestamp(point["x"]).strftime("%Y-%m-%d"),
                chart_name:   point["y"]
            })

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        print(f"  Got {len(df)} data points")
        return df

    except requests.exceptions.RequestException as e:
        print(f"  Request failed for {chart_name}: {e}")
        return pd.DataFrame()


def fetch_all_charts() -> pd.DataFrame:
    """Fetch all charts and merge into one DataFrame keyed by date."""
    merged = None

    for chart_name, chart_type in CHARTS.items():
        df = fetch_chart(chart_name, chart_type)
        time.sleep(1)  # Be polite to the API

        if df.empty:
            continue

        if merged is None:
            merged = df
        else:
            merged = pd.merge(merged, df, on="date", how="outer")

    if merged is not None:
        merged = merged.sort_values("date").reset_index(drop=True)

    return merged


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add useful derived columns for analysis."""
    if "market_price_usd" in df.columns:
        # Daily price change %
        df["price_change_pct"] = df["market_price_usd"].pct_change() * 100
        df["price_change_pct"] = df["price_change_pct"].round(4)

        # 7-day and 30-day rolling average price
        df["price_7d_avg"]  = df["market_price_usd"].rolling(7,  min_periods=1).mean().round(2)
        df["price_30d_avg"] = df["market_price_usd"].rolling(30, min_periods=1).mean().round(2)

    if "tx_volume_usd" in df.columns:
        # 7-day rolling average volume
        df["volume_7d_avg"] = df["tx_volume_usd"].rolling(7, min_periods=1).mean().round(2)

    # Year and month columns for grouping
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month

    return df


def main():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    print(f"Fetching BTC historical data from Blockchain.info (timespan: {TIMESPAN})...")
    print(f"Charts: {', '.join(CHARTS.keys())}\n")

    df = fetch_all_charts()

    if df is None or df.empty:
        print("No data fetched. Check your internet connection.")
        return

    # Save raw
    raw_path = "data/raw/btc_history_raw.csv"
    df.to_csv(raw_path, index=False)
    print(f"\nRaw data saved → {raw_path}")

    # Add derived columns and save processed
    df = add_derived_columns(df)
    processed_path = "data/processed/btc_history_processed.csv"
    df.to_csv(processed_path, index=False)
    print(f"Processed data saved → {processed_path}")

    # Summary
    print(f"\nDataset: {len(df)} days of data")
    print(f"Date range: {df['date'].min().date()} → {df['date'].max().date()}")

    if "market_price_usd" in df.columns:
        print(f"\nBTC Price:")
        print(f"  Min:     ${df['market_price_usd'].min():,.2f}")
        print(f"  Max:     ${df['market_price_usd'].max():,.2f}")
        print(f"  Current: ${df['market_price_usd'].iloc[-1]:,.2f}")


if __name__ == "__main__":
    main()