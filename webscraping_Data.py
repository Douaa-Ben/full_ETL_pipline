import os
import time
import requests
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from psycopg2 import Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL credentials from .env
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

# Scraper parameters
SCRAPE_BATCH_SIZE = int(os.getenv("SCRAPE_BATCH_SIZE", "5"))
SCRAPE_SLEEP_SECONDS = int(os.getenv("SCRAPE_SLEEP_SECONDS", "10"))

# Updated COIN_MAPPING
COIN_MAPPING = {
    "bitcoin": "bitcoin",
    "ethereum": "ethereum",
    "tether": "tether",
    "ripple": "xrp",
    "binancecoin": "bnb",
    "solana": "solana",
    "usd-coin": "usd-coin",
    "dogecoin": "dogecoin",
    "tron": "tron",
    "cardano": "cardano",
    "staked-ether": "lido-dao",
    "wrapped-bitcoin": "wrapped-bitcoin",
    "hyperliquid": "hyperliquid",
    "sui": "sui",
    "wrapped-steth": "axelar-wrapped-wsteth",
    "chainlink": "chainlink",
    "avalanche-2": "avalanche",
    "leo-token": "unus-sed-leo",
    "bitcoin-cash": "bitcoin-cash",
    "stellar": "stellar"
}


def scrape_coinmarketcap_info(coin_slug):
    """Scrape metadata for a single coin."""
    url = f"https://coinmarketcap.com/currencies/{coin_slug}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        print(f"Requesting {url}...")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Description
        about_section = soup.find("div", class_=lambda x: x and "about" in x.lower())
        description_elem = about_section.find("p") if about_section else soup.find("p")
        description = description_elem.text.strip() if description_elem else "N/A"

        # Category
        category_elem = soup.find("a", class_="cmc-link")
        category = category_elem.text.strip() if category_elem else "N/A"

        # Price
        price_elem = soup.find("span", {"data-test": "text-cdp-price-display"})
        if not price_elem:
            price_elem = soup.find("span", string=lambda x: x and "$" in x)
        price = price_elem.text.strip() if price_elem else "N/A"

        print(f"✅ Successfully scraped {coin_slug}")
        return {"description": description, "category": category, "price": price}

    except Exception as e:
        print(f"⚠️ Error scraping {coin_slug}: {e}")
        return {"description": "N/A", "category": "N/A", "price": "N/A"}


def main():
    """Main ETL pipeline for CoinMarketCap metadata."""
    coin_metadata = []

    try:
        # Connect to PostgreSQL using credentials from .env
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("✅ Connected to PostgreSQL")

        # No table creation here — assume it's already created manually

        for i in range(0, len(COIN_MAPPING), SCRAPE_BATCH_SIZE):
            batch = list(COIN_MAPPING.items())[i:i + SCRAPE_BATCH_SIZE]
            for coin_key, coin_slug in batch:
                print(f"🔍 Scraping {coin_key}...")
                metadata = scrape_coinmarketcap_info(coin_slug)
                metadata["coin"] = coin_key
                coin_metadata.append(metadata)

                # Insert into existing table
                try:
                    price_value = (
                        float(metadata["price"].replace("$", "").replace(",", ""))
                        if metadata["price"] != "N/A" else None
                    )
                    cur.execute("""
                        INSERT INTO crypto_metadata (coin, description, category, price)
                        VALUES (%s, %s, %s, %s)
                    """, (coin_key, metadata["description"], metadata["category"], price_value))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f"❌ DB insert error for {coin_key}: {e}")

            print(f"⏳ Sleeping {SCRAPE_SLEEP_SECONDS}s to avoid rate limits...")
            time.sleep(SCRAPE_SLEEP_SECONDS)

        # Save to CSV
        df = pd.DataFrame(coin_metadata)
        df.to_csv("crypto_metadata.csv", index=False)
        print("💾 Saved metadata to crypto_metadata.csv")

        # Display data sample from the existing table
        cur.execute("SELECT coin, category, price FROM crypto_metadata LIMIT 5;")
        for row in cur.fetchall():
            print(row)

    except Error as e:
        print(f"❌ Database error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("🔒 Connection closed.")


if __name__ == "__main__":
    main()
