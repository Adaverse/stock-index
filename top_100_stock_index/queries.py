import duckdb

conn = duckdb.connect(database = "market_data.duckdb", read_only = False)
# create_index_composition_table = """
#     CREATE TABLE index_composition (
#         Date TIMESTAMP WITH TIME ZONE,  -- Stores timestamp in IST
#         Ticker VARCHAR,                 -- Stores stock ticker symbol
#         Price DOUBLE,                    -- Stores price of the stock
#         Prev Quantity DOUBLE,                 -- Stores quantity of stocks
#         Value DOUBLE                     -- Stores calculated value (Price * Quantity)
#     )
# """
# conn.execute(create_index_composition_table)

top_5_tickers_query = """
    WITH top_5 AS (
        SELECT Date, Ticker
        FROM market_data
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY 'Market Cap' DESC) <= 5
    )
    SELECT * FROM top_5
"""
print(conn.execute(top_5_tickers_query).fetchall())