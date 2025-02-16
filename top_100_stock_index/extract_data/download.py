import yfinance as yf
import pandas as pd
from tqdm import tqdm
import duckdb

from .ticker_symbols import get_sp_500_tickers

def get_ticker_df(ticker, period, **kwargs):
    df_with_market_cap = pd.DataFrame()

    stock = yf.Ticker(ticker)
    hist = stock.history(period=period)
    hist['Date'] = hist.index

    shares_outstanding = stock.info.get('sharesOutstanding', None)

    hist['Market Cap'] = hist['Close'] * shares_outstanding if shares_outstanding else 0
    hist['Ticker'] = ticker
    hist['Period'] = period
    df_with_market_cap = pd.concat([df_with_market_cap, hist])
    return df_with_market_cap

def get_all_tickers_data(duckdb_conn, num_tickers):
    complete_data_df = pd.DataFrame()
    tickers = get_sp_500_tickers()
    # TODO: remove 10 limit
    for ticker in tqdm(tickers[:num_tickers]):
        ticker_df = get_ticker_df(ticker, "1mo")
        complete_data_df = pd.concat([complete_data_df, ticker_df])
    try:
        duckdb_conn.execute("INSERT INTO market_data SELECT * FROM complete_data_df")
    except duckdb.duckdb.CatalogException as e:
        if "Table with name market_data does not exist!" in repr(e):
            duckdb_conn.execute("CREATE TABLE market_data AS SELECT * FROM complete_data_df")
