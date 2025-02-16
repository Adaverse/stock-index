from io import BytesIO
import pandas as pd
from fpdf import FPDF

def to_excel(df):
    output = BytesIO()

    if 'Date' in df.columns:
        df['Date'] = df['Date'].dt.tz_localize(None) 
        
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
    processed_data = output.getvalue()
    return processed_data

def to_csv(df):
    return df.to_csv(index=False).encode("utf-8")

def to_pdf(df, ticker):
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    # Title
    pdf.cell(200, 10, txt=f"Stock Data for {ticker}", ln=True, align="C")
    pdf.ln(10)  # Add some space

    # Table Header
    pdf.set_font("Arial", style="B", size=10)
    pdf.cell(40, 10, "Date", border=1)
    pdf.cell(40, 10, "Open Price", border=1)
    pdf.cell(40, 10, "Close Price", border=1)
    pdf.ln()

    # Table Data
    pdf.set_font("Arial", size=10)
    for _, row in df.iterrows():
        pdf.cell(40, 10, row["Date"].strftime("%Y-%m-%d"), border=1)
        pdf.cell(40, 10, f"{row['Open_Price']:.2f}", border=1)
        pdf.cell(40, 10, f"{row['Close_Price']:.2f}", border=1)
        pdf.ln()
        
    return pdf.output(dest="S").encode("latin1")
