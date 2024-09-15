from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, request
import plotly
import plotly.graph_objs as go
import json
from cassandra.cluster import Cluster
import pandas as pd
import geopandas as gpd

app = Flask(__name__)


def get_cassandra_session():
    cluster = Cluster(["cassandra"])
    session = cluster.connect("crypto_analysis")
    return session


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/price_chart")
def price_chart():
    session = get_cassandra_session()
    coin_symbol = request.args.get("coin_symbol", "BTC")
    time_range = request.args.get("time_range", "1h")

    time_ranges = {
        "1h": 1,
        "1d": 24,
        "1w": 24 * 7,
        "1m": 24 * 30,
        "1y": 24 * 365,
    }

    hours_back = time_ranges.get(time_range, 1)
    cutoff_date = datetime.now() - timedelta(hours=hours_back)

    query = """
    SELECT coin_id, exchange, timestamp, price 
    FROM price_data
    WHERE timestamp > %s
    ALLOW FILTERING
    """
    rows = session.execute(query, (cutoff_date,))

    data = {}
    for row in rows:
        if row.coin_id.endswith(f"_{coin_symbol}"):
            exchange = row.exchange
            if exchange not in data:
                data[exchange] = {"timestamp": [], "price": []}
            data[exchange]["timestamp"].append(row.timestamp)
            data[exchange]["price"].append(float(row.price))

    traces = []
    for exchange, exchange_data in data.items():
        trace = go.Scatter(
            x=exchange_data["timestamp"],
            y=exchange_data["price"],
            mode="lines",
            name=f"{exchange}",
        )
        traces.append(trace)

    layout = go.Layout(
        title=f"{coin_symbol} Price Across Exchanges ({time_range})",
        xaxis=dict(title="Time"),
        yaxis=dict(title="Price"),
    )

    fig = go.Figure(data=traces, layout=layout)
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route("/ohlc_chart")
def ohlc_chart():
    session = get_cassandra_session()
    coin_symbol = request.args.get("coin_symbol", "BTC")
    time_range = request.args.get("time_range", "1m")

    time_ranges = {
        "1h": 1,
        "1d": 24,
        "1w": 24 * 7,
        "1m": 24 * 30,
        "1y": 24 * 365,
    }

    hours_back = time_ranges.get(time_range, 1)
    cutoff_date = datetime.now() - timedelta(hours=hours_back)

    query = f"""
    SELECT coin_id, timestamp, open, high, low, close
    FROM ohlc_data
    WHERE timestamp > %s
    ALLOW FILTERING
    """
    rows = session.execute(query, (cutoff_date,))

    timestamps = []
    opens = []
    highs = []
    lows = []
    closes = []

    for row in rows:
        if row.coin_id.endswith(f"_{coin_symbol}"):
            timestamps.append(row.timestamp)
            opens.append(float(row.open))
            highs.append(float(row.high))
            lows.append(float(row.low))
            closes.append(float(row.close))

    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
        }
    )

    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["timestamp"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            )
        ]
    )

    fig.update_layout(title=f"{coin_symbol.capitalize()} OHLC Chart ({time_range})")
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route("/technical_indicators")
def technical_indicators():
    session = get_cassandra_session()
    coin_symbol = request.args.get("coin_symbol", "BTC")

    query = """
    SELECT coin_id, timestamp, sma_20, ema_20, rsi_14, macd 
    FROM technical_indicators
    LIMIT 1000
    ALLOW FILTERING
    """
    rows = session.execute(query)

    timestamps = []
    sma_20 = []
    ema_20 = []
    rsi_14 = []
    macd = []

    for row in rows:
        if row.coin_id.endswith(f"_{coin_symbol}"):
            timestamps.append(row.timestamp)
            sma_20.append(float(row.sma_20) if row.sma_20 else None)
            ema_20.append(float(row.ema_20) if row.ema_20 else None)
            rsi_14.append(float(row.rsi_14) if row.rsi_14 else None)
            macd.append(float(row.macd) if row.macd else None)

    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "sma_20": sma_20,
            "ema_20": ema_20,
            "rsi_14": rsi_14,
            "macd": macd,
        }
    )

    sma_trace = go.Scatter(
        x=df["timestamp"], y=df["sma_20"], mode="lines", name="SMA 20"
    )
    ema_trace = go.Scatter(
        x=df["timestamp"], y=df["ema_20"], mode="lines", name="EMA 20"
    )
    rsi_trace = go.Scatter(
        x=df["timestamp"], y=df["rsi_14"], mode="lines", name="RSI 14", yaxis="y2"
    )
    macd_trace = go.Scatter(
        x=df["timestamp"], y=df["macd"], mode="lines", name="MACD", yaxis="y3"
    )

    layout = go.Layout(
        title=f"{coin_symbol.capitalize()} Technical Indicators",
        yaxis=dict(title="Price"),
        yaxis2=dict(title="RSI", overlaying="y", side="right"),
        yaxis3=dict(title="MACD", overlaying="y", side="right"),
    )

    fig = go.Figure(data=[sma_trace, ema_trace, rsi_trace, macd_trace], layout=layout)
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route("/coin_market_cap")
def coin_market_cap():
    session = get_cassandra_session()

    query_max_timestamp = "SELECT MAX(timestamp) AS max_timestamp FROM coin_market_cap"
    rows_max_timestamp = session.execute(query_max_timestamp)
    max_timestamp = rows_max_timestamp.one().max_timestamp

    query_data = f"""
    SELECT coin_symbol, market_cap_percentage
    FROM coin_market_cap
    WHERE timestamp = %s
    ALLOW FILTERING
    """
    rows_data = session.execute(query_data, (max_timestamp,))

    data = {"coin_symbol": [], "market_cap_percentage": []}
    for row in rows_data:
        data["coin_symbol"].append(row.coin_symbol)
        data["market_cap_percentage"].append(row.market_cap_percentage)

    fig = go.Figure(
        data=[go.Pie(labels=data["coin_symbol"], values=data["market_cap_percentage"])]
    )
    fig.update_layout(title="Global Crypto Market Cap Distribution")

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


'''
@app.route("/exchange_map")
def exchange_map():
    session = get_cassandra_session()

    query = "SELECT * FROM exchange_by_country"
    rows = session.execute(query)
    df = pd.DataFrame(rows)

    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
    world = world.merge(df, how="left", left_on=["name"], right_on=["country"])
    world["num_exchanges"] = world["num_exchanges"].fillna(0)

    fig = go.Figure(
        data=go.Choropleth(
            locations=world["iso_a3"],
            z=world["num_exchanges"],
            text=world["name"],
            colorscale="Viridis",
            autocolorscale=False,
            reversescale=True,
            marker_line_color="darkgray",
            marker_line_width=0.5,
            colorbar_title="Number of Exchanges",
        )
    )

    fig.update_layout(
        title_text="Number of Crypto Exchanges by Country",
        geo=dict(
            showframe=False, showcoastlines=False, projection_type="equirectangular"
        ),
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

'''
@app.route("/coin_table")
def coin_table():
    session = get_cassandra_session()

    coins_query = "SELECT id, name, symbol, exchange FROM coins"
    coins_rows = session.execute(coins_query)
    coins_df = pd.DataFrame(coins_rows)

    price_query = (
        "SELECT coin_id, exchange, price, volume, change_1h, change_1d, change_1w, change_1m, timestamp FROM price_data"
    )
    price_rows = session.execute(price_query)
    price_df = pd.DataFrame(price_rows)

    ohlc_query = (
        "SELECT coin_id, exchange, open, high, low, close, timestamp FROM ohlc_data"
    )
    ohlc_rows = session.execute(ohlc_query)
    ohlc_df = pd.DataFrame(ohlc_rows)

    tech_query = "SELECT coin_id, exchange, sma_20, ema_20, rsi_14, macd, timestamp FROM technical_indicators"
    tech_rows = session.execute(tech_query)
    tech_df = pd.DataFrame(tech_rows)

    df = coins_df.merge(
        price_df, left_on=["id", "exchange"], right_on=["coin_id", "exchange"]
    )
    df = df.merge(ohlc_df, on=["coin_id", "exchange"])
    df = df.merge(tech_df, on=["coin_id", "exchange"])

    df = df.sort_values("timestamp_x").groupby(["id", "exchange"]).last().reset_index()

    return render_template("coin_table.html", data=df.to_dict("records"))



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
