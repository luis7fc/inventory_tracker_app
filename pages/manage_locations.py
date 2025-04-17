# pages/manage_locations.py

import streamlit as st
import pandas as pd
from db import get_db_connection
from config import STAGING_LOCATIONS

def run():
    st.header("Manage Warehouse Locations")

    # --- Add/Edit Location Form ---
    st.subheader("Create or Edit Location")
    with st.form("create_location"):
        new_loc = st.text_input("New Location Code")
        description = st.text_input("Description (optional)")
        warehouse = st.text_input("Warehouse", value="VV")
        multi_item_allowed = st.checkbox("Allow multiple item types in this location?")
        submit_loc = st.form_submit_button("Save")

    if submit_loc and new_loc:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO locations (location_code, description, warehouse, multi_item_allowed)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (location_code) DO UPDATE
                    SET description = EXCLUDED.description,
                        warehouse = EXCLUDED.warehouse,
                        multi_item_allowed = EXCLUDED.multi_item_allowed
                """, (new_loc, description, warehouse, multi_item_allowed))
                conn.commit()
        st.success("Location saved.")

    # --- Reset or Delete Location ---
    st.subheader("Reset or Delete Location")
    with get_db_connection() as conn:
        loc_list = pd.read_sql("SELECT location_code FROM locations ORDER BY location_code", conn)["location_code"]
        loc_to_clear = st.selectbox("Select Location", loc_list)

        if st.button("Reset Location Inventory"):
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM current_inventory WHERE location = %s", (loc_to_clear,))
                conn.commit()
            st.success(f"Inventory reset for location: {loc_to_clear}")

        if st.button("Delete Location"):
            with conn.cursor() as cursor:
                cursor.execute("SELECT SUM(quantity) FROM current_inventory WHERE location = %s", (loc_to_clear,))
                total_qty = cursor.fetchone()[0]
                if not total_qty:
                    cursor.execute("DELETE FROM locations WHERE location_code = %s", (loc_to_clear,))
                    conn.commit()
                    st.success(f"Location {loc_to_clear} deleted.")
                else:
                    st.warning("Cannot delete a location that still has inventory.")

    # --- View & Export Locations Table ---
    st.subheader("📋 All Locations (Live)")
    with get_db_connection() as conn:
        loc_df = pd.read_sql("SELECT * FROM locations ORDER BY location_code", conn)
    st.dataframe(loc_df, use_container_width=True)

    # --- Export to CSV ---
    csv = loc_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name="warehouse_locations.csv",
        mime="text/csv"
    )
