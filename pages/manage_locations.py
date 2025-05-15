import streamlit as st
import pandas as pd
from db import get_db_cursor, get_all_locations


def run():
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )

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
        with get_db_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO locations (location_code, description, warehouse, multi_item_allowed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (location_code) DO UPDATE
                SET description = EXCLUDED.description,
                    warehouse = EXCLUDED.warehouse,
                    multi_item_allowed = EXCLUDED.multi_item_allowed
                """,
                (new_loc, description, warehouse, multi_item_allowed)
            )
        st.success("Location saved.")

    # --- Reset or Delete Location ---
    st.subheader("Reset or Delete Location")
    loc_list = get_all_locations()
    if loc_list:
        loc_to_clear = st.selectbox("Select Location", sorted(loc_list))

        if st.button("Reset Location Inventory"):
            with get_db_cursor() as cursor:
                cursor.execute(
                    "DELETE FROM current_inventory WHERE location = %s",
                    (loc_to_clear,)
                )
            st.success(f"Inventory reset for location: {loc_to_clear}")

        if st.button("Delete Location"):
            with get_db_cursor() as cursor:
                cursor.execute(
                    "SELECT SUM(quantity) FROM current_inventory WHERE location = %s",
                    (loc_to_clear,)
                )
                total_qty = cursor.fetchone()[0]
                if not total_qty:
                    cursor.execute(
                        "DELETE FROM locations WHERE location_code = %s",
                        (loc_to_clear,)
                    )
                    st.success(f"Location {loc_to_clear} deleted.")
                else:
                    st.warning("Cannot delete a location that still has inventory.")
    else:
        st.info("No locations found.")

    # --- View & Export Locations Table ---
    st.subheader("📋 All Locations (Live)")
    with get_db_cursor() as cursor:
        cursor.execute("SELECT * FROM locations ORDER BY location_code")
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
    loc_df = pd.DataFrame(rows, columns=cols)
    st.dataframe(loc_df, use_container_width=True)

    # --- Export to CSV ---
    csv = loc_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name="warehouse_locations.csv",
        mime="text/csv"
    )
