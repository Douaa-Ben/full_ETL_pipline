import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import psycopg2
from psycopg2 import Error

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


def scrape_coinmarketcap_info(coin):
    url = f"https://coinmarketcap.com/currencies/{coin}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    }
    try:
        print(f"Requesting {url}...")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        print(f"Received response for {coin}...")
        soup = BeautifulSoup(response.text, "html.parser")

        about_section = soup.find("div", class_=lambda x: x and "about" in x.lower()) or \
                        soup.find("div", class_=lambda x: x and "content" in x.lower())
        description_elem = about_section.find("p") if about_section else soup.find("p", class_="sc-1eb5slv-0")
        description = description_elem.text.strip() if description_elem else "N/A"

        category_section = soup.find("div", class_=lambda x: x and ("tags" in x.lower() or "category" in x.lower() or "details" in x.lower()))
        category_elem = category_section.find("a", class_="cmc-link") if category_section else None
        if not category_elem and coin == "leo":
            category_elem = soup.find("div", class_="sc-16r8icm-0")
        if not category_elem:
            category_keywords = ["layer", "smart", "defi", "interoperability", "ecosystem", "contract"]
            category_elem = soup.find("a", class_="cmc-link", string=lambda x: x and any(kw in x.lower() for kw in category_keywords) and "historical" not in x.lower())
        category = category_elem.text.strip() if category_elem else "N/A"

        price_elem = soup.find("span", {"data-test": "text-cdp-price-display"})
        if not price_elem:
            price_elem = soup.find("span", string=lambda x: x and "$" in x)
        price = price_elem.text.strip() if price_elem and hasattr(price_elem, "text") else "N/A"

        print(f"Successfully scraped {coin}.")
        return {
            "coin": coin,
            "description": description,
            "category": category,
            "price": price
        }
    except requests.exceptions.HTTPError as e:
        print(f"HTTPError for {coin}: {e}. Skipping this coin.")
        return {
            "coin": coin,
            "description": "N/A",
            "category": "N/A",
            "price": "N/A"
        }
    except requests.exceptions.Timeout as e:
        print(f"Timeout for {coin}: {e}. Skipping this coin.")
        return {
            "coin": coin,
            "description": "N/A",
            "category": "N/A",
            "price": "N/A"
        }
    except Exception as e:
        print(f"Error scraping {coin}: {e}. Skipping this coin.")
        return {
            "coin": coin,
            "description": "N/A",
            "category": "N/A",
            "price": "N/A"
        }



# Store data in PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="CryptoMarket",
        user="postgres",
        password="SYS",  # Replace with your password
        host="localhost"
    )
    cur = conn.cursor()

    # Drop and Create Table
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

    coin_metadata = []
    for i in range(0, len(COIN_MAPPING), 5):
        batch = list(COIN_MAPPING.items())[i:i + 5]
        for coin_key, coin_slug in batch:
            print(f"Scraping {coin_key} (slug: {coin_slug})...")
            metadata = scrape_coinmarketcap_info(coin_slug)
            metadata["coin"] = coin_key
            coin_metadata.append(metadata)
        time.sleep(10)

    df = pd.DataFrame(coin_metadata)

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO crypto_metadata (coin, description, category, price)
            VALUES (%s, %s, %s, %s)
        """, (
            row['coin'],
            row['description'],
            row['category'],
            float(row['price'].replace('$', '').replace(',', '')) if row['price'] != "N/A" else None
        ))

    conn.commit()
    print("✅ Data successfully inserted into crypto_metadata!")

    # Display from DB
    cur.execute("SELECT * FROM crypto_metadata;")
    rows = cur.fetchall()
    for row in rows:
        print(row)

except Error as e:
    print(f"❌ Database error: {e}")

finally:
    if conn:
        cur.close()
        conn.close()

# Save to CSV (optional)
df.to_csv("crypto_metadata.csv", index=False)
print(df)
