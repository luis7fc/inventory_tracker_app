import streamlit as st
import pandas as pd
from db import get_db_cursor


def run():
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.header("📥 Upload Inventory Init CSV")
    file = st.file_uploader("Upload CSV", type="csv")

    if file:
        try:
            df = pd.read_csv(file)
            st.dataframe(df)

            with get_db_cursor() as cursor:
                for _, row in df.iterrows():
                    item_code = row['item_code']
                    location  = row['location']
                    warehouse = row.get('warehouse', 'VV')
                    quantity  = int(row['quantity'])
                    scan_id   = row.get('scan_id')

                    # Ensure location exists
                    cursor.execute(
                        "INSERT INTO locations (location_code, warehouse) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (location, warehouse)
                    )
                    # Log inventory init
                    cursor.execute(
                        "INSERT INTO inventory_init (item_code, location, quantity, scan_id) VALUES (%s, %s, %s, %s)",
                        (item_code, location, quantity, scan_id)
                    )
                    # Upsert into current_inventory
                    cursor.execute(
                        """
                        INSERT INTO current_inventory (item_code, location, quantity)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (item_code, location) DO UPDATE
                        SET quantity = current_inventory.quantity + EXCLUDED.quantity
                        """,
                        (item_code, location, quantity)
                    )
            st.success("✅ Inventory successfully initialized.")
        except Exception as e:
            st.error(f"❌ Error: {e}")
