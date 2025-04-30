import streamlit as st
import pandas as pd
from db import get_db_connection

def run():
    st.header("📊 Live Inventory Report")

    conn = get_db_connection()

    # 1) build warehouse filter options
    wh_df = pd.read_sql(
        "SELECT DISTINCT warehouse FROM locations ORDER BY warehouse", conn
    )
    options = ["All"] + wh_df["warehouse"].tolist()
    selection = st.selectbox("🔎 Filter by warehouse", options)

    # 2) pull in every location + its warehouse, left-join to current_inventory
    base_query = """
    SELECT
      l.warehouse,
      l.location_code AS location,
      ci.item_code,
      COALESCE(ci.quantity, 0) AS quantity
    FROM locations AS l
    LEFT JOIN current_inventory AS ci
      ON ci.location = l.location_code
    """
    if selection != "All":
        base_query += f" WHERE l.warehouse = '{selection}'"
    # ensure correct ORDER BY column names
    base_query += " ORDER BY l.warehouse, l.location_code, ci.item_code"

    df = pd.read_sql(base_query, conn)

    # display full dataframe
    st.dataframe(df, use_container_width=True)

    # 3) pivot so each location becomes a column, grouped by warehouse+item
    pivot_df = df.pivot_table(
        index=["warehouse", "item_code"],
        columns="location",
        values="quantity",
        fill_value=0
    ).reset_index()

    # download button for CSV export
    csv_bytes = pivot_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download CSV Report",
        csv_bytes,
        "inventory_report.csv",
        "text/csv"
    )
