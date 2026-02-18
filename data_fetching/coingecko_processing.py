import pandas as pd
import numpy as np
import os

INPUT_FILE = "data/raw/coingecko_raw.csv"
OUTPUT_FILE = "data/processed/coingecko_processed.csv"


def load_data(filepath):
    df = pd.read_csv(filepath, parse_dates=["date"])
    print(f"Loaded {len(df)} rows, {df['coin'].nunique()} coins")
    return df


def clean_data(df):
    # Drop duplicates
    df = df.drop_duplicates(subset=["date", "coin"])

    # Drop rows with missing values
    df = df.dropna(subset=["price", "market_cap", "volume"])

    # Sort by coin and date
    df = df.sort_values(["coin", "date"]).reset_index(drop=True)

    return df


def engineer_features(df):
    # Daily return (% change in price)
    df["daily_return"] = df.groupby("coin")["price"].pct_change() * 100

    # 7-day and 30-day rolling average price
    df["price_7d_avg"] = df.groupby("coin")["price"].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    )
    df["price_30d_avg"] = df.groupby("coin")["price"].transform(
        lambda x: x.rolling(30, min_periods=1).mean()
    )

    # 7-day rolling volatility (std of daily returns)
    df["volatility_7d"] = df.groupby("coin")["daily_return"].transform(
        lambda x: x.rolling(7, min_periods=1).std()
    )

    # Flag high volume days (volume > 2x the coin's median volume)
    df["high_volume"] = df.groupby("coin")["volume"].transform(
        lambda x: x > x.median() * 2
    )

    # Flag large price swings (absolute daily return > 5%)
    df["large_swing"] = df["daily_return"].abs() > 5

    return df


def summarize(df):
    print("\n--- Summary ---")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"Total rows: {len(df)}")
    print(f"\nCoins: {df['coin'].unique().tolist()}")
    print("\nMissing values:")
    print(df.isnull().sum())
    print("\nSample stats per coin:")
    print(df.groupby("coin")["price"].agg(["min", "max", "mean"]).round(2))


def main():
    os.makedirs("data/processed", exist_ok=True)

    df = load_data(INPUT_FILE)
    df = clean_data(df)
    df = engineer_features(df)
    summarize(df)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nCleaned data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()