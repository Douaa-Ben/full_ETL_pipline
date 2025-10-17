import requests
import json
import time


# List of top 20 cryptocurrencies by market cap
COINS = [
    "bitcoin", "ethereum", "tether", "binancecoin", "usd-coin",
    "xrp", "cardano", "solana", "dogecoin", "polkadot",
    "tron", "avalanche-2", "litecoin", "chainlink", "uniswap",
    "wrapped-bitcoin", "bitcoin-cash", "stellar", "cosmos", "monero"
]

def fetch_coin_history(coin_id, days=365):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history?vs_currency=usd&days={days}"
    response = requests.get(url)
    if response.status_code == 200:  # Correct status code
        data = response.json()
        return data.get("market_data", [])
    return []

coin_history = {}
for i, coin in enumerate(COINS):
    print(f"Fetching history for {coin} ({i+1}/{len(COINS)})...")
    history = fetch_coin_history(coin)
    coin_history[coin] = history
    time.sleep(1)  # Avoid rate limits

# Save data to JSON file
with open("crypto_history.json", "w", encoding="utf-8") as f:
    json.dump(coin_history, f, indent=2)

print("\nCrypto historical data saved to 'crypto_history.json'")
