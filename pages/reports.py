import streamlit as st
import pandas as pd
from db import get_db_cursor


def run():
    st.header("📊 Live Inventory Report")

    # 1) build warehouse filter options
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT DISTINCT warehouse FROM locations ORDER BY warehouse"
        )
        wh_list = [row[0] for row in cursor.fetchall()]
    options = ["All"] + wh_list
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
    params = []
    if selection != "All":
        base_query += " WHERE l.warehouse = %s"
        params.append(selection)
    base_query += " ORDER BY l.warehouse, l.location_code, ci.item_code"

    with get_db_cursor() as cursor:
        cursor.execute(base_query, tuple(params))
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=cols)

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
