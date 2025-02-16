import typer
import duckdb
import os

from extract_data.download import get_ticker_df, get_all_tickers_data
from index.index_setup import init_or_update_data
app = typer.Typer()

@app.command()
def init_data(num_tickers: int = 500):
    os.remove("market_data.duckdb")
    conn = duckdb.connect(database = "market_data.duckdb", read_only = False)
    get_all_tickers_data(conn, num_tickers)

    conn.execute("""
        CREATE TABLE index_composition (
            Date TIMESTAMP WITH TIME ZONE,  -- Stores timestamp in IST
            Ticker VARCHAR,                 -- Stock ticker symbol
            Open_Price DOUBLE,              -- Opening price of the stock
            Close_Price DOUBLE,             -- Closing price of the stock
            Open_Quantity DOUBLE,           -- Quantity at market open
            Close_Quantity DOUBLE,          -- Quantity at market close
            Open_Value DOUBLE,              -- Open price * Open quantity
            Close_Value DOUBLE              -- Close price * Close quantity
        )
    """)

    conn.execute("""
        CREATE TABLE index_data (
            Date TIMESTAMP WITH TIME ZONE,  -- Stores timestamp in IST
            Ticker VARCHAR,                 -- Stock ticker symbol
            Open_Price DOUBLE,              -- Opening price of the stock
            Close_Price DOUBLE,             -- Closing price of the stock
        )
    """)

@app.command()
def init_or_update_index(index_ticker: str, top_x: int):
    conn = duckdb.connect(database = "market_data.duckdb", read_only = False)
    distinct_tickers = conn.execute("select DISTINCT Ticker from market_data").fetchall()
    if len(distinct_tickers) < top_x:
        raise ValueError(f"top_x stocks in index cannot be greater than number of distinct stocks which is {len(distinct_tickers)!r}")
    init_or_update_data(conn, index_ticker, top_x)

if __name__ == "__main__":
    app()
