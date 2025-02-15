import typer
from extract_data.download import get_ticker_df, get_all_tickers_data
import duckdb

app = typer.Typer()

@app.command()
def hello(name: str):
    print(f"Hello {name}")

@app.command()
def init_data():
    conn = duckdb.connect(database = "market_data.duckdb", read_only = False)
    get_all_tickers_data(conn)
    print(conn.execute("select * from market_data"))
    print(conn.fetchone())

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

if __name__ == "__main__":
    app()
