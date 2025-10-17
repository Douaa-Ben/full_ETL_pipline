import os
import time
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_batch
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL credentials
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

# API parameters
VS_CURRENCY = os.getenv("VS_CURRENCY", "usd")
TOP_COINS = int(os.getenv("TOP_COINS", "20"))
FETCH_DAYS = int(os.getenv("FETCH_DAYS", "365"))

# Initialize CoinGecko client
cg = CoinGeckoAPI()


def fetch_top_coins(limit=TOP_COINS):
    """Fetch top N coins by market cap."""
    print(f"Fetching top {limit} coins by market cap...")
    top_coins = cg.get_coins_markets(
        vs_currency=VS_CURRENCY,
        order="market_cap_desc",
        per_page=limit,
        page=1
    )
    return {coin["id"]: coin["id"] for coin in top_coins}


def fetch_historical_data(coin_id):
    """Fetch historical price data for a specific coin."""
    print(f"Fetching historical data for {coin_id}...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=FETCH_DAYS)

    data = cg.get_coin_market_chart_range_by_id(
        id=coin_id,
        vs_currency=VS_CURRENCY,
        from_timestamp=int(start_date.timestamp()),
        to_timestamp=int(end_date.timestamp())
    )

    return [
        (coin_id, price[0], price[1], datetime.utcfromtimestamp(price[0] / 1000).date())
        for price in data.get("prices", [])
    ]


def insert_data_to_db(records, coin_id, cursor, conn):
    """Insert fetched data into PostgreSQL."""
    if not records:
        print(f"No data found for {coin_id}.")
        return

    print(f"Inserting {len(records)} records for {coin_id}...")

    try:
        execute_batch(cursor, """
            INSERT INTO historical_prices (coin, timestamp, price, price_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (coin, timestamp) DO NOTHING;
        """, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data for {coin_id}: {e}")


def main():
    top_coins = fetch_top_coins()

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("✅ Connected to PostgreSQL successfully.\n")

        for idx, coin_id in enumerate(top_coins):
            data = fetch_historical_data(coin_id)
            insert_data_to_db(data, coin_id, cur, conn)

            if (idx + 1) % 5 == 0:
                print("⏳ Pausing 10 seconds to avoid API rate limits...")
                time.sleep(10)

        cur.execute("SELECT coin, price_date, price FROM historical_prices LIMIT 10;")
        print("\n📊 Sample records:")
        for row in cur.fetchall():
            print(row)

    except Error as e:
        print(f"❌ Database Error: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
            print("\n🔒 Connection closed.")


if __name__ == "__main__":
    main()
