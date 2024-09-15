from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    explode,
    to_timestamp,
    current_timestamp,
    avg,
    expr,
    udf,
    pandas_udf,
    collect_list,
    PandasUDFType,
)
from cassandra.policies import RoundRobinPolicy
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
    ArrayType,
)
import numpy as np
from cassandra.cluster import Cluster
import logging
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)

spark = (
    SparkSession.builder.appName("CryptoDataStreaming")
    .config("spark.cassandra.connection.host", "cassandra")
    .getOrCreate()
)


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS crypto_analysis
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
    """
    )
    logging.info("Keyspace created successfully!")


def create_tables(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_analysis.coins (
            id TEXT PRIMARY KEY,
            name TEXT,
            symbol TEXT,
            exchange TEXT
        )
    """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_analysis.price_data (
            coin_id TEXT,
            exchange TEXT,
            timestamp TIMESTAMP,
            price DECIMAL,
            volume DECIMAL,
            change_1h DECIMAL,
            change_1d DECIMAL,
            change_1w DECIMAL,
            change_1m DECIMAL,
            PRIMARY KEY ((coin_id, exchange), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_analysis.ohlc_data (
            coin_id TEXT,
            exchange TEXT,
            timestamp TIMESTAMP,
            open DECIMAL,
            high DECIMAL,
            low DECIMAL,
            close DECIMAL,
            PRIMARY KEY ((coin_id, exchange), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_analysis.technical_indicators (
            coin_id TEXT,
            exchange TEXT,
            timestamp TIMESTAMP,
            sma_20 DECIMAL,
            ema_20 DECIMAL,
            rsi_14 DECIMAL,
            macd DECIMAL,
            PRIMARY KEY ((coin_id, exchange), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_analysis.coin_market_cap (
            coin_symbol TEXT,
            timestamp TIMESTAMP,
            market_cap_percentage DECIMAL,
            PRIMARY KEY ((coin_symbol), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """
    )
    logging.info("Tables created successfully!")


def write_to_cassandra(df, epoch_id):
    table_name = ""
    if "id" in df.columns:
        table_name = "coins"
    elif "price" in df.columns:
        table_name = "price_data"
    elif "open" in df.columns:
        table_name = "ohlc_data"
    elif "sma_20" in df.columns:
        table_name = "technical_indicators"
    elif "market_cap_percentage" in df.columns:
        table_name = "coin_market_cap"
    elif "ema_20" in df.columns:
        table_name = "technical_indicators"

    if table_name:
        try:
            df.write.format("org.apache.spark.sql.cassandra").option(
                "spark.cassandra.output.consistency.level", "ONE"
            ).mode("append").options(
                table=table_name, keyspace="crypto_analysis"
            ).option(
                "spark.cassandra.connection.host", "cassandra"
            ).save()
            logging.info(f"Batch {epoch_id} written to Cassandra table {table_name}")
        except Exception as e:
            logging.error(
                f"Error writing to Cassandra table {table_name} in batch {epoch_id}: {e}"
            )
    else:
        logging.error(f"Unknown data format in batch {epoch_id}")


technical_indicators_schema = StructType(
    [
        StructField("coin_id", StringType()),
        StructField("exchange", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("sma_20", DoubleType()),
        StructField("ema_20", DoubleType()),
        StructField("rsi_14", DoubleType()),
        StructField("macd", DoubleType()),
    ]
)

@pandas_udf(technical_indicators_schema, PandasUDFType.GROUPED_MAP)
def calculate_technical_indicators_udf(df):
    pdf = df.copy()
    pdf.dropna()
    pdf = pdf.sort_values("timestamp")

    pdf["sma_20"] = pdf["price"].rolling(window=20).mean()

    pdf["ema_20"] = pdf["price"].ewm(span=20, adjust=False).mean()

    delta = pdf["price"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    pdf["rsi_14"] = 100 - (100 / (1 + rs))

    exp1 = pdf["price"].ewm(span=12, adjust=False).mean()
    exp2 = pdf["price"].ewm(span=26, adjust=False).mean()
    pdf["macd"] = exp1 - exp2

    result = pdf[
        ["coin_id", "exchange", "timestamp", "sma_20", "ema_20", "rsi_14", "macd"]
    ]

    return result


def calculate_technical_indicators(price_data_df):
    return price_data_df.groupBy("coin_id", "exchange").apply(
        calculate_technical_indicators_udf
    )


def create_cassandra_connection():
    try:
        cluster = Cluster(
            ["cassandra"], 
            port=9042,
            load_balancing_policy=RoundRobinPolicy(),
        )
        session = cluster.connect()
        logging.info("Cassandra connection created successfully")
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

def main():
    session = create_cassandra_connection()
    create_keyspace(session)
    create_tables(session)
    # Define schemas
    coins_schema = StructType(
        [
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("symbol", StringType()),
            StructField("exchange", StringType()),
        ]
    )

    price_data_schema = StructType(
        [
            StructField("coin_id", StringType()),
            StructField("exchange", StringType()),
            StructField("timestamp", StringType()),
            StructField("price", DoubleType()),
            StructField("volume", DoubleType()),
            StructField("change_1h", DoubleType()),
            StructField("change_1d", DoubleType()),
            StructField("change_1w", DoubleType()),
            StructField("change_1m", DoubleType()),
        ]
    )

    ohlc_data_schema = StructType(
        [
            StructField("coin_id", StringType()),
            StructField("exchange", StringType()),
            StructField("timestamp", StringType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
        ]
    )

    coin_market_cap_schema = StructType(
        [
            StructField("coin_symbol", StringType()),
            StructField("market_cap_percentage", DoubleType()),
        ]
    )

    kafka_servers = "broker-1:29092,broker-2:29093,broker-3:29094"

    all_data_schema = StructType(
        [
            StructField("coins", ArrayType(coins_schema)),
            StructField("price_data", ArrayType(price_data_schema)),
            StructField("ohlc_data", ArrayType(ohlc_data_schema)),
            StructField("coin_market_cap", ArrayType(coin_market_cap_schema)),
        ]
    )

    all_data_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "cryptoAllData")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss","false")
        .load()
        .select(from_json(col("value").cast("string"), all_data_schema).alias("data"))
        .select("data.*")
    )

    coins_df = all_data_df.select(explode("coins").alias("coin")).select("coin.*")
    price_data_df = (
        all_data_df.select(explode("price_data").alias("price"))
        .select("price.*")
        .withColumn("timestamp", to_timestamp("timestamp"))
    )
    ohlc_data_df = (
        all_data_df.select(explode("ohlc_data").alias("ohlc"))
        .select("ohlc.*")
        .withColumn("timestamp", to_timestamp("timestamp"))
    )
    coin_market_cap_df = (
        all_data_df.select(explode("coin_market_cap").alias("market_cap"))
        .select("market_cap.*")
        .withColumn("timestamp", current_timestamp())
    )

    coins_query = (
        coins_df.writeStream.foreachBatch(write_to_cassandra)
        .outputMode("update")
        .start()
    )
    price_data_query = (
        price_data_df.writeStream.foreachBatch(write_to_cassandra)
        .outputMode("append")
        .start()
    )
    ohlc_data_query = (
        ohlc_data_df.writeStream.foreachBatch(write_to_cassandra)
        .outputMode("append")
        .start()
    )
    coin_market_cap_query = (
        coin_market_cap_df.writeStream.foreachBatch(write_to_cassandra)
        .outputMode("append")
        .start()
    )

    price_data_with_indicators_df = calculate_technical_indicators(price_data_df)
    technical_indicators_query = (
        price_data_with_indicators_df.writeStream.foreachBatch(write_to_cassandra)
        .outputMode("update")
        .start()
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
