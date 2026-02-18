"""
world_bank.py
-------------
Fetches country-level macroeconomic data from the World Bank API.
No API key required.

Data collected:
- GDP growth (annual %)
- Inflation rate (consumer prices, annual %)
- Remittance inflows (% of GDP)

Usage:
    python scripts/world_bank.py
"""

import requests
import pandas as pd
import os
import time

# ── Countries to track ────────────────────────────────────────────────────
# Focused on countries with notable crypto adoption or economic stress
COUNTRIES = [
    "US", "NG", "VE", "TR", "AR",
    "SV", "UA", "VN", "IN", "BR",
    "RU", "CN", "ZA", "PH", "KE"
]

# ── World Bank indicator codes ─────────────────────────────────────────────
INDICATORS = {
    "gdp_growth":   "NY.GDP.MKTP.KD.ZG",   # GDP growth (annual %)
    "inflation":    "FP.CPI.TOTL.ZG",       # Inflation, consumer prices (annual %)
    "remittance":   "BX.TRF.PWKR.DT.GD.ZS" # Remittance inflows (% of GDP)
}

BASE_URL = "https://api.worldbank.org/v2/country"


def fetch_indicator(indicator_code: str, indicator_name: str,
                    countries: list, year_start=2018, year_end=2023) -> pd.DataFrame:
    """
    Fetch a single indicator for all countries over a date range.
    Returns a DataFrame with columns: country, iso_code, year, <indicator_name>
    """
    country_str = ";".join(countries)
    url = (
        f"{BASE_URL}/{country_str}/indicator/{indicator_code}"
        f"?date={year_start}:{year_end}&format=json&per_page=500"
    )

    print(f"  Fetching {indicator_name}...")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        # World Bank returns [metadata, results]
        if len(data) < 2 or not data[1]:
            print(f"  No data returned for {indicator_name}")
            return pd.DataFrame()

        records = []
        for entry in data[1]:
            if entry.get("value") is not None:
                records.append({
                    "country":      entry["country"]["value"],
                    "iso_code":     entry["countryiso3code"],
                    "year":         int(entry["date"]),
                    indicator_name: round(float(entry["value"]), 4)
                })

        df = pd.DataFrame(records)
        print(f"  Got {len(df)} records")
        return df

    except requests.exceptions.RequestException as e:
        print(f"  Request failed: {e}")
        return pd.DataFrame()


def fetch_all_indicators() -> pd.DataFrame:
    """
    Fetch all indicators and merge into a single DataFrame.
    """
    merged = None

    for indicator_name, indicator_code in INDICATORS.items():
        df = fetch_indicator(indicator_code, indicator_name, COUNTRIES)
        time.sleep(1)  # Be polite to the API

        if df.empty:
            continue

        if merged is None:
            merged = df
        else:
            merged = pd.merge(
                merged, df,
                on=["country", "iso_code", "year"],
                how="outer"
            )

    return merged


def add_classifications(df: pd.DataFrame) -> pd.DataFrame:
    """Add human-readable classification columns."""
    if "inflation" in df.columns:
        df["inflation_class"] = pd.cut(
            df["inflation"],
            bins=[-999, 10, 30, 100, 9999],
            labels=["Stable", "Moderate", "High", "Hyperinflation"]
        )

    if "gdp_growth" in df.columns:
        df["gdp_trend"] = df["gdp_growth"].apply(
            lambda x: "Growing" if x > 2 else ("Stagnant" if x >= 0 else "Shrinking")
            if pd.notna(x) else None
        )

    return df


def main():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    print("Fetching World Bank macroeconomic data...")
    print(f"Countries: {', '.join(COUNTRIES)}")
    print(f"Indicators: {', '.join(INDICATORS.keys())}\n")

    df = fetch_all_indicators()

    if df is None or df.empty:
        print("No data fetched. Check your internet connection.")
        return

    # Save raw
    raw_path = "data/raw/world_bank_raw.csv"
    df.to_csv(raw_path, index=False)
    print(f"\nRaw data saved → {raw_path}")

    # Add classifications and save processed
    df = add_classifications(df)
    processed_path = "data/processed/world_bank_processed.csv"
    df.to_csv(processed_path, index=False)
    print(f"Processed data saved → {processed_path}")

    # Quick summary
    print(f"\nDataset: {len(df)} rows | {df['country'].nunique()} countries | "
          f"Years: {df['year'].min()}–{df['year'].max()}")
    print("\nSample (most recent year per country):")
    latest = df.sort_values("year").groupby("country").last().reset_index()
    print(latest[["country", "year", "gdp_growth", "inflation", "remittance"]].to_string(index=False))


if __name__ == "__main__":
    main()