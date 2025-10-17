import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import psycopg2
from psycopg2 import Error
from prefect import flow, task, get_run_logger
from datetime import datetime


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


@task(retries=2, retry_delay_seconds=10)
def scrape_coinmarketcap_info(coin_slug: str, coin_key: str):
    logger = get_run_logger()
    url = f"https://coinmarketcap.com/currencies/{coin_slug}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    }
    try:
        logger.info(f"Requesting {url}...")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Received response for {coin_key}...")

        soup = BeautifulSoup(response.text, "html.parser")

        about_section = soup.find("div", class_=lambda x: x and "about" in x.lower()) or \
                        soup.find("div", class_=lambda x: x and "content" in x.lower())
        description_elem = about_section.find("p") if about_section else soup.find("p", class_="sc-1eb5slv-0")
        description = description_elem.text.strip() if description_elem else "N/A"

        category_section = soup.find("div", class_=lambda x: x and ("tags" in x.lower() or "category" in x.lower() or "details" in x.lower()))
        category_elem = category_section.find("a", class_="cmc-link") if category_section else None
        if not category_elem and coin_slug == "leo":
            category_elem = soup.find("div", class_="sc-16r8icm-0")
        if not category_elem:
            category_keywords = ["layer", "smart", "defi", "interoperability", "ecosystem", "contract"]
            category_elem = soup.find("a", class_="cmc-link", string=lambda x: x and any(kw in x.lower() for kw in category_keywords) and "historical" not in x.lower())
        category = category_elem.text.strip() if category_elem else "N/A"

        price_elem = soup.find("span", {"data-test": "text-cdp-price-display"})
        if not price_elem:
            price_elem = soup.find("span", string=lambda x: x and "$" in x)
        price = price_elem.text.strip() if price_elem and hasattr(price_elem, "text") else "N/A"

        logger.info(f"Successfully scraped {coin_key}.")
        return {
            "coin": coin_key,
            "description": description,
            "category": category,
            "price": price
        }
    except Exception as e:
        logger.error(f"Error scraping {coin_key}: {e}. Returning N/A values.")
        return {
            "coin": coin_key,
            "description": "N/A",
            "category": "N/A",
            "price": "N/A"
        }


@task
def store_to_db(records: list[dict]):
    logger = get_run_logger()
    try:
        conn = psycopg2.connect(
            dbname="CryptoMarket",
            user="postgres",
            password="SYS",  # Replace with your password
            host="localhost"
        )
        cur = conn.cursor()

        logger.info("Dropping and creating table crypto_metadata...")
        cur.execute("""
            DROP TABLE IF EXISTS crypto_metadata;
            CREATE TABLE crypto_metadata (
                id SERIAL PRIMARY KEY,
                coin TEXT NOT NULL,
                description TEXT,
                category TEXT,
                price NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        for row in records:
            price_value = None
            if row['price'] != "N/A":
                try:
                    price_value = float(row['price'].replace('$', '').replace(',', ''))
                except Exception:
                    price_value = None

            cur.execute("""
                INSERT INTO crypto_metadata (coin, description, category, price)
                VALUES (%s, %s, %s, %s)
            """, (row['coin'], row['description'], row['category'], price_value))

        conn.commit()
        logger.info("✅ Data successfully inserted into crypto_metadata!")

        # Optional: fetch and log rows to verify
        cur.execute("SELECT * FROM crypto_metadata;")
        rows = cur.fetchall()
        for row in rows:
            logger.info(row)

    except Error as e:
        logger.error(f"❌ Database error: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


@flow(name="crypto_scraper_flow")
def crypto_scraper_flow():
    logger = get_run_logger()
    coin_metadata = []

    coin_items = list(COIN_MAPPING.items())

    # Scrape in batches of 5 with a 10 sec pause between batches
    batch_size = 5
    for i in range(0, len(coin_items), batch_size):
        batch = coin_items[i:i + batch_size]

        logger.info(f"Scraping batch: {[c[0] for c in batch]}")

        for coin_key, coin_slug in batch:
            metadata = scrape_coinmarketcap_info(coin_slug, coin_key)
            coin_metadata.append(metadata)

        logger.info("Sleeping for 10 seconds before next batch...")
        time.sleep(10)  # This sleep is OK here for demo purposes

    store_to_db(coin_metadata)

    # Optional: save to CSV locally
    df = pd.DataFrame(coin_metadata)
    df.to_csv("crypto_metadata.csv", index=False)
    logger.info("Saved data to crypto_metadata.csv")


if __name__ == "__main__":
    crypto_scraper_flow()
