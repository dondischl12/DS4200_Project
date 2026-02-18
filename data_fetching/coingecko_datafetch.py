import requests
import pandas as pd
import time

BASE_URL = "https://api.coingecko.com/api/v3"
OUTPUT_FILE = "data/raw/coingecko_raw.csv"


def get_price_history(coin_id="bitcoin", currency="usd", days=365):
    """Fetch historical price, market cap, and volume for a coin."""
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": currency,
        "days": days,
        "interval": "daily"
    }
    response = requests.get(url, params=params).json()

    if "prices" not in response:
        print(f"Rate limited or error for {coin_id}: {response}")
        return pd.DataFrame()

    prices = pd.DataFrame(response["prices"], columns=["timestamp", "price"])
    market_caps = pd.DataFrame(response["market_caps"], columns=["timestamp", "market_cap"])
    volumes = pd.DataFrame(response["total_volumes"], columns=["timestamp", "volume"])

    df = prices.merge(market_caps, on="timestamp").merge(volumes, on="timestamp")
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.drop(columns=["timestamp"], inplace=True)
    df["coin"] = coin_id

    return df


def get_global_adoption():
    """Fetch global crypto market data."""
    url = f"{BASE_URL}/global"
    raw = requests.get(url).json()

    if "data" not in raw:
        print(f"Unexpected response: {raw}")
        return {}

    response = raw["data"]
    return {
        "total_market_cap_usd": response["total_market_cap"]["usd"],
        "total_volume_usd": response["total_volume"]["usd"],
        "market_cap_change_24h": response["market_cap_change_percentage_24h_usd"],
        "active_cryptocurrencies": response["active_cryptocurrencies"],
        "btc_dominance": response["market_cap_percentage"]["btc"],
        "eth_dominance": response["market_cap_percentage"]["eth"],
    }


def get_multiple_coins(coin_ids, currency="usd", days=365):
    """Fetch price history for multiple coins and combine into one DataFrame."""
    dfs = []
    for coin in coin_ids:
        print(f"Fetching {coin}...")
        df = get_price_history(coin, currency, days)
        dfs.append(df)
        time.sleep(15)  # respect rate limit
    return pd.concat(dfs, ignore_index=True)


def main():
    # Coins to track
    coins = ["bitcoin", "ethereum", "tether", "binancecoin", "solana"]

    # Fetch price history for all coins
    print("Fetching price history...")
    df = get_multiple_coins(coins, days=365)
    print(f"\nTotal rows fetched: {len(df)}")
    print(df.head())

    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nData saved to {OUTPUT_FILE}")

    # Fetch and print global market stats
    print("\nFetching global market data...")
    global_data = get_global_adoption()
    for k, v in global_data.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()