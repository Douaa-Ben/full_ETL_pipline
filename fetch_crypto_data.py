from prefect import flow
import pandas as pd
import time
import psycopg2
from psycopg2 import Error
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timedelta

@flow(name="Daily Crypto Data Fetch")
def fetch_crypto_flow():
    cg = CoinGeckoAPI()
    top_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=20, page=1)
    COIN_MAPPING = {coin['id']: coin['id'] for coin in top_coins}

    def fetch_historical_data(coin_id):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        data = cg.get_coin_market_chart_range_by_id(
            id=coin_id,
            vs_currency="usd",
            from_timestamp=int(start_date.timestamp()),
            to_timestamp=int(end_date.timestamp())
        )
        return [
            (coin_id, price[0], price[1], datetime.utcfromtimestamp(price[0] / 1000).date())
            for price in data.get('prices', [])
        ]

    try:
        conn = psycopg2.connect(dbname="CryptoMarket", user="postgres", password="SYS", host="localhost")
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS historical_prices;")
        cur.execute("""
            CREATE TABLE historical_prices (
                id SERIAL PRIMARY KEY,
                coin TEXT NOT NULL,
                timestamp BIGINT,
                price NUMERIC,
                price_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (coin, timestamp)
            );
        """)

        for idx, coin_id in enumerate(COIN_MAPPING):
            hist = fetch_historical_data(coin_id)
            for record in hist:
                try:
                    cur.execute("""
                        INSERT INTO historical_prices (coin, timestamp, price, price_date)
                        VALUES (%s, %s, %s, %s)
                    """, record)
                except psycopg2.IntegrityError:
                    conn.rollback()
                    continue
            conn.commit()
            if (idx + 1) % 5 == 0:
                time.sleep(10)

        cur.execute("SELECT coin, price_date, price FROM historical_prices LIMIT 10;")
        for row in cur.fetchall():
            print(row)

    except Error as e:
        print(f"Error: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()
