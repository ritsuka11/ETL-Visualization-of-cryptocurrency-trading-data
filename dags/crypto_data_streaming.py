import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from kafka.errors import KafkaError
import requests
import json
from kafka import KafkaProducer
import time
import logging
import ccxt
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import backoff

default_args = {"owner": "admin", "start_date": datetime(2024, 6, 13)}


def get_crypto_data(**kwargs):
    exchanges = [
        "binance",
        "bybit",
        "htx",
        "coinbase",
        "gate.io",
        "okx",
        "kraken",
        "kucoin",
        "hashkey"
        "bitfinex",
    ]
    top_coins = {
        "BTC": "Bitcoin",
        "ETH": "Ethereum",
        "USDT": "Tether",
        "BNB": "Binance Coin",
        "XRP": "Ripple",
        "USDC": "USD Coin",
        "ADA": "Cardano",
        "DOGE": "Dogecoin",
        "SOL": "Solana",
        "TRX": "TRON",
        "DOT": "Polkadot",
        "MATIC": "Polygon",
        "LTC": "Litecoin",
        "BUSD": "Binance USD",
        "SHIB": "Shiba Inu",
        "DAI": "Dai",
        "WBTC": "Wrapped Bitcoin",
        "AVAX": "Avalanche",
        "LEO": "LEO Token",
        "UNI": "Uniswap",
    }
    coins = []
    price_data = []
    ohlc_data = []
    for exchange_id in exchanges:
        try:
            exchange = getattr(ccxt, exchange_id)()
            markets = exchange.load_markets()

            for symbol in exchange.symbols:
                base_currency = symbol.split("/")[0]
                if base_currency in top_coins and symbol.endswith("/USDT"):
                    ticker = exchange.fetch_ticker(symbol)
                    ohlcv_1h = exchange.fetch_ohlcv(symbol, "1h", limit=1)[0]
                    ohlcv_1d = exchange.fetch_ohlcv(symbol, "1d", limit=1)[0]
                    ohlcv_1w = exchange.fetch_ohlcv(symbol, "1w", limit=1)[0]
                    ohlcv_1m = exchange.fetch_ohlcv(symbol, "1M", limit=1)[0]

                    coin_id = f"{exchange_id}_{base_currency}"
                    coins.append(
                            {
                                "id": coin_id,
                                "name": top_coins[base_currency],
                                "symbol": base_currency,
                                "exchange": exchange_id,
                            }
                        )

                    price_data.append(
                        {
                            "coin_id": coin_id,
                            "exchange": exchange_id,
                            "timestamp": datetime.utcnow().isoformat(),
                            "price": ticker["last"],
                            "volume": ticker["quoteVolume"],
                            "change_1h": (
                                ohlcv_1h[4] / ohlcv_1h[1] - 1 if ohlcv_1h[1] else None
                            ),
                            "change_1d": ticker["percentage"],
                            "change_1w": (
                                ohlcv_1w[4] / ohlcv_1w[1] - 1 if ohlcv_1w[1] else None
                            ),
                            "change_1m": (
                                ohlcv_1m[4] / ohlcv_1m[1] - 1 if ohlcv_1m[1] else None
                            ),
                        }
                    )

                    ohlc_data.append(
                        {
                            "coin_id": coin_id,
                            "exchange": exchange_id,
                            "timestamp": datetime.utcnow().isoformat(),
                            "open": ohlcv_1d[1],
                            "high": ohlcv_1d[2],
                            "low": ohlcv_1d[3],
                            "close": ohlcv_1d[4],
                        }
                    )

            time.sleep(exchange.rateLimit / 1000)

        except ccxt.NetworkError as e:
            logging.error(f"Network error when fetching data from {exchange_id}: {e}")
        except ccxt.ExchangeError as e:
            logging.error(f"Exchange error when fetching data from {exchange_id}: {e}")
        except Exception as e:
            logging.error(
                    f"Unexpected error when fetching data from {exchange_id}: {e}"
                )

    try:
        global_url = "https://api.coingecko.com/api/v3/global"
        global_res = requests.get(global_url)
        global_res.raise_for_status()
        global_data = global_res.json()
        coin_market_cap = [
            {
                "coin_symbol": coin_symbol,
                "market_cap_percentage": percentage,
            }
            for coin_symbol, percentage in global_data["data"][
                "market_cap_percentage"
            ].items()
        ]
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve global crypto data: {e}")
        coin_market_cap = []

    combined_data = {
        "coins": coins,
        "price_data": price_data,
        "ohlc_data": ohlc_data,
        "coin_market_cap": coin_market_cap,
    }

    kwargs["ti"].xcom_push(key="raw_crypto_data", value=combined_data)
    return combined_data


@backoff.on_exception(backoff.expo, (KafkaError, NoBrokersAvailable), max_tries=5)
def create_topic_if_not_exists(admin_client, topic):
    try:
        topics = admin_client.list_topics()
        if topic not in topics:
            new_topic = NewTopic(name=topic, num_partitions=3, replication_factor=1)
            admin_client.create_topics([new_topic])
            logging.info(f"Topic '{topic}' created successfully.")
    except TopicAlreadyExistsError:
        logging.warning(f"Topic '{topic}' already exists.")
    except Exception as e:
        logging.error(f"Failed to create topic '{topic}': {e}")
        raise


@backoff.on_exception(backoff.expo, KafkaError, max_tries=5)
def send_to_kafka(producer, topic, data):
    future = producer.send(topic, value=data)
    future.get(timeout=60)


def load_crypto_data_to_kafka(**kwargs):
    logging.info("Starting to load crypto data to Kafka")
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=["broker-1:29092", "broker-2:29093", "broker-3:29094"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )

        admin_client = KafkaAdminClient(
            bootstrap_servers=["broker-1:29092", "broker-2:29093", "broker-3:29094"]
        )

        data = kwargs["ti"].xcom_pull(
            task_ids="get_crypto_data_task", key="raw_crypto_data"
        )
        if not data:
            raise AirflowException("No data retrieved from XCom")

        logging.info(f"Retrieved data: {data}")

        topic = "cryptoAllData"
        create_topic_if_not_exists(admin_client, topic)

        all_data = {
            "coins": data["coins"],
            "price_data": data["price_data"],
            "ohlc_data": data["ohlc_data"],
            "coin_market_cap": data["coin_market_cap"],
        }

        send_to_kafka(producer, topic, all_data)

        logging.info("All data sent to Kafka successfully")

    except Exception as e:
        logging.error(f"Error in load_crypto_data_to_kafka: {e}")
        raise AirflowException(f"Error in load_crypto_data_to_kafka: {e}")

    finally:
        if producer:
            producer.flush(timeout=60)
            producer.close(timeout=60)
            logging.info("Kafka producer closed")


with DAG(
    "crypto_data_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    get_crypto_data_task = PythonOperator(
        task_id="get_crypto_data_task",
        python_callable=get_crypto_data,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(seconds=10),
        dag=dag,
    )

    load_data_to_kafka_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_crypto_data_to_kafka,
        provide_context=True,
        dag=dag,
    )

    get_crypto_data_task >> load_data_to_kafka_task
