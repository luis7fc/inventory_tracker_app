import streamlit as st
import pandas as pd
from db import get_db_cursor


def run():
    st.header("📊 Live Inventory Report")

    # 1) Build filter options
    with get_db_cursor() as cursor:
        cursor.execute("SELECT DISTINCT warehouse FROM locations ORDER BY warehouse")
        wh_list = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT item_code FROM current_inventory ORDER BY item_code")
        item_list = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT location_code FROM locations ORDER BY location_code")
        loc_list = [row[0] for row in cursor.fetchall()]

    wh_selection = st.selectbox("🔎 Filter by warehouse", ["All"] + wh_list)
    item_selection = st.multiselect("🎯 Filter by item code(s)", item_list)
    loc_selection = st.multiselect("📍 Filter by location(s)", loc_list)

    # 2) Build dynamic query
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
    where_clauses = []
    params = []

    if wh_selection != "All":
        where_clauses.append("l.warehouse = %s")
        params.append(wh_selection)
    if item_selection:
        where_clauses.append("ci.item_code = ANY(%s)")
        params.append(item_selection)
    if loc_selection:
        where_clauses.append("l.location_code = ANY(%s)")
        params.append(loc_selection)

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)

    base_query += " ORDER BY l.warehouse, l.location_code, ci.item_code"

    # 3) Fetch and display data
    with get_db_cursor() as cursor:
        cursor.execute(base_query, tuple(params))
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=cols)
    st.dataframe(df, use_container_width=True)

    # 4) Pivot for grouped summary view
    pivot_df = df.pivot_table(
        index=["warehouse", "item_code"],
        columns="location",
        values="quantity",
        fill_value=0
    ).reset_index()

    csv_bytes = pivot_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download CSV Report",
        csv_bytes,
        "inventory_report.csv",
        "text/csv"
    )
