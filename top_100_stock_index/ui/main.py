import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

from top_100_stock_index.utility import to_excel, to_csv, to_pdf

conn = duckdb.connect("market_data.duckdb")

query = "SELECT * FROM index_data"
df = conn.execute(query).fetchdf()

query_index_composition = "SELECT * FROM index_composition"
df_composition = conn.execute(query_index_composition).fetchdf()

st.title("Stock Price Visualization")

# Sidebar for Ticker Selection
tickers = df['Ticker'].unique()
selected_ticker = st.sidebar.selectbox("Select a Ticker", tickers)

df['Date'] = pd.to_datetime(df['Date'])
df_composition['Date'] = pd.to_datetime(df_composition['Date'])

# Filter data
filtered_df = df[df['Ticker'] == selected_ticker]

if filtered_df.empty:
    st.warning("No data available for the selected ticker.")
    st.stop()

# st.write(filtered_df)

# Plot using Plotly
fig = px.line(filtered_df, x='Date', y=['Close_Price'], 
              title=f"Stock Prices for {selected_ticker}",
              labels={'Date': 'Date', 'value': 'Close_Price'},
              markers=True)

st.plotly_chart(fig)

st.subheader("Stock Composition Data")
available_dates = df_composition['Date'].dt.date.unique()
selected_date = st.selectbox("Select a Date", sorted(available_dates, reverse=True))

filtered_composition_df = df_composition[df_composition['Date'].dt.date == selected_date]
st.dataframe(filtered_composition_df)

st.sidebar.subheader("Download Options")

excel_data = to_excel(filtered_df)
st.sidebar.download_button(
    label="📥 Download Excel File",
    data=excel_data,
    file_name=f"{selected_ticker}_stock_data.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

csv_data = to_csv(filtered_df)
st.sidebar.download_button(
    label="📥 Download CSV File",
    data=csv_data,
    file_name=f"{selected_ticker}_stock_data.csv",
    mime="text/csv"
)

pdf_data = to_pdf(filtered_df, selected_ticker)
st.sidebar.download_button(
    label="📥 Download PDF File",
    data=pdf_data,
    file_name=f"{selected_ticker}_stock_data.pdf",
    mime="application/pdf"
)
