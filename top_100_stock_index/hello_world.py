import duckdb, datetime
conn = duckdb.connect(database = "market_data.duckdb", read_only = False)
schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()

# ables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
# print(ables)
# table_name="market_data"

# query = f"""
# SELECT column_name, data_type
# FROM information_schema.columns
# WHERE table_name = '{table_name}'
# """
# schema = conn.execute(query).fetchall()
# for column in schema:
#     print(f"- {column[0]} ({column[1]})")

a = conn.execute("select * from market_data").df()
print(a.head())
print(a.describe())

index_value = 100
total_securities = 5
weightage = index_value / total_securities
base_date = conn.execute("SELECT MIN(Date) AS earliest_date FROM market_data").fetchall()[0][0]
next_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
# print(base_date, next_date)
dates = conn.execute("""
    SELECT DISTINCT Date 
    FROM market_data 
    ORDER BY Date ASC
""").fetchall()

is_base = True
# Print sorted dates
conn.execute("DELETE FROM index_data")
conn.execute("DELETE FROM index_composition")

for i in range(len(dates)):
    base_date = dates[i][0]
    base_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
    next_date = base_date + datetime.timedelta(days=1)
    if i > 0:
        # rebalance?
        prev_date = dates[i-1][0].replace(hour=0, minute=0, second=0, microsecond=0)
        open_index_price = conn.execute("""
            SELECT Close_Price FROM index_data
            WHERE Date >= ? AND Date < ?
        """, [prev_date, base_date]).fetchall()[0][0]
        joined_df = conn.execute(f"""
            SELECT md.*, ic.*
            FROM (
                SELECT *
                FROM market_data
                WHERE Date >= ? AND Date < ?
            ) AS md
            JOIN (
                SELECT *
                FROM index_composition
                WHERE Date >= ? AND Date < ?
            ) AS ic
            ON md.Ticker = ic.Ticker
        """, [base_date, next_date, prev_date, base_date]).df()
        joined_df["new_open_price"] = joined_df["Close_Price"]
        joined_df["new_open_value"] = joined_df["Close_Value"]
        joined_df["new_open_quantity"] = joined_df["Close_Quantity"]
        joined_df["new_close_price"] = joined_df["Close"]
        joined_df["new_close_value"] = joined_df["Close"] * joined_df["new_open_quantity"]
        new_index_value = joined_df["new_close_value"].sum()
        processed_df = conn.execute("SELECT * FROM index_composition WHERE 1=2").df()
        conn.execute("""
            INSERT INTO index_data (Date, Ticker, Open_Price, Close_Price) 
            VALUES (?, ?, ?, ?)
        """, [base_date, "T100", open_index_price, new_index_value])
        new_stock_value = new_index_value/total_securities
        joined_df["rebalanced_close_value"] = new_stock_value
        joined_df["rebalanced_close_quantity"] = new_stock_value / joined_df["new_close_price"]
        processed_df = joined_df[
            ['Date', 'Ticker', 'new_open_price', 'new_open_value', 'new_open_quantity', 'new_close_price', 'rebalanced_close_value', 'rebalanced_close_quantity']
        ]
        conn.execute("""
            INSERT INTO index_composition (Date, Ticker, Open_Price, Close_Price, Open_Quantity, Close_Quantity, Open_Value, Close_Value)
            SELECT
                Date,
                Ticker,
                new_open_price as Open_Price,
                new_close_price as Close_Price,
                new_open_quantity as Open_Quantity,
                rebalanced_close_quantity as Close_Quantity,
                new_open_value as Open_Value,
                rebalanced_close_value as Close_Value
            FROM joined_df
        """)

    else:
        conn.execute(f"""
            INSERT INTO index_composition (Date, Ticker, Open_Price, Close_Price, Open_Quantity, Close_Quantity, Open_Value, Close_Value)
            SELECT 
                Date, 
                Ticker, 
                Open AS Open_Price, 
                Close AS Close_Price, 
                0 AS Open_Quantity,  
                {index_value} / ({total_securities} * Close) AS Close_Quantity,  
                0 AS Open_Value,  
                ({index_value} / ({total_securities} * Close)) * Close AS Close_Value  
            FROM market_data 
            WHERE Date >= ? AND Date < ? 
            ORDER BY "Market Cap" DESC LIMIT {total_securities}
        """, [base_date, next_date])
        conn.execute("""
            INSERT INTO index_data (Date, Ticker, Open_Price, Close_Price) 
            VALUES (?, ?, ?, ?)
        """, [base_date, "T100", 0, 100])

    # print(date[0])

print(conn.execute("select * from index_composition").fetchall())
print(conn.execute("select * from index_data").fetchall())

# index_composition = conn.execute("""
# SELECT * FROM index_composition WHERE Date >= ? AND Date < ?
# """)

# start of day value
# end of day value of index

