# pages/reports.py

import streamlit as st
import pandas as pd
from db import get_db_connection

def run():
    st.header("📊 Live Inventory Report")

    conn = get_db_connection()
    query = """
        SELECT * FROM current_inventory
        WHERE location IN (
            SELECT location_code FROM locations WHERE warehouse = 'VV'
        )
        ORDER BY item_code, location
    """
    df = pd.read_sql(query, conn)
    st.dataframe(df)

    pivot_df = df.pivot_table(
        index="item_code",
        columns="location",
        values="quantity",
        fill_value=0
    ).reset_index()

    csv = pivot_df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download CSV Report", csv, "inventory_report.csv", "text/csv")
