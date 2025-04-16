#pages/scan_lookup.py
import streamlit as st
import pandas as pd
from datetime import datetime
from db import get_db_connection
import psycopg2.extras

def run():
    st.header("🔎 Scan Lookup and Export")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now().date())
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date())

    warehouse_input = st.text_input("Warehouse Initials (e.g. VV, SAC, FNO)")
    job_filter = st.text_input("Job Number (optional)")
    lot_filter = st.text_input("Lot Number (optional)")
    transaction_filter = st.selectbox("Transaction Type (optional)", [
        "", "Receiving", "Job Issue", "Return", "Internal Movement", "Manual Adjustment"
    ])

    if st.button("Run Scan Query"):
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                query = """
                    SELECT scan_time, scan_id, item_code, job_number, lot_number, location, transaction_type, warehouse
                    FROM scan_verifications
                    WHERE scan_time BETWEEN %s AND %s
                      AND (%s IS NULL OR job_number = %s)
                      AND (%s IS NULL OR lot_number = %s)
                      AND (%s IS NULL OR warehouse = %s)
                      AND (%s = '' OR transaction_type = %s)
                    ORDER BY scan_time DESC
                """
                params = (
                    start_date, end_date,
                    job_filter or None, job_filter or None,
                    lot_filter or None, lot_filter or None,
                    warehouse_input or None, warehouse_input or None,
                    transaction_filter, transaction_filter
                )

                cursor.execute(query, params)
                rows = cursor.fetchall()

                if rows:
                    df = pd.DataFrame(rows, columns=[
                        "scan_time", "scan_id", "item_code",
                        "job_number", "lot_number", "location",
                        "transaction_type", "warehouse"
                    ])
                    st.dataframe(df, use_container_width=True)

                    csv_data = df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label="Download CSV",
                        data=csv_data,
                        file_name="scan_export.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("No scan data found for the selected criteria.")
