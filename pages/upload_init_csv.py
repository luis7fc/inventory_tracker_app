import streamlit as st
import pandas as pd
from db import (
    get_db_connection,
    insert_location_if_not_exists,
    insert_inventory_init_row,
    upsert_current_inventory
)

def run():
    st.header("📥 Upload Inventory Init CSV")
    file = st.file_uploader("Upload CSV", type="csv")

    if file:
        try:
            df = pd.read_csv(file)
            st.dataframe(df)

            conn = get_db_connection()

            for _, row in df.iterrows():
                item_code = row['item_code']
                location = row['location']
                warehouse = row.get('warehouse', 'VV')  # fallback default
                quantity = int(row['quantity'])
                scan_id = row['scan_id']

                insert_location_if_not_exists(conn, location, warehouse)
                insert_inventory_init_row(conn, item_code, location, quantity, scan_id)
                upsert_current_inventory(conn, item_code, location, quantity)

            conn.commit()
            st.success("✅ Inventory successfully initialized.")

        except Exception as e:
            st.error(f"❌ Error: {e}")
            conn.rollback()
        finally:
            conn.close()
