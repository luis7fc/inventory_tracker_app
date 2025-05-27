import streamlit as st
from datetime import datetime
from uuid import uuid4
from db import get_db_cursor, validate_location_exists

# --- Helper DB functions ---
def get_scan_location(scan_id):
    with get_db_cursor() as cursor:
        cursor.execute("SELECT scan_id, item_code, location FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        result = cursor.fetchone()
        if result:
            return {"scan_id": result[0], "item_code": result[1], "location": result[2]}
        return None

def scan_id_exists(scan_id):
    with get_db_cursor() as cursor:
        cursor.execute("SELECT 1 FROM scan_verifications WHERE scan_id = %s", (scan_id,))
        return cursor.fetchone() is not None

def location_to_warehouse(location):
    with get_db_cursor() as cursor:
        cursor.execute("SELECT warehouse FROM locations WHERE location_code = %s", (location,))
        result = cursor.fetchone()
        return result[0] if result else "UNKNOWN"

def insert_verification(scan_id, item_code, location, transaction_type, scanned_by):
    with get_db_cursor() as cursor:
        cursor.execute("""
            INSERT INTO scan_verifications (
                id, item_code, job_number, lot_number, scan_time, scan_id,
                location, transaction_type, warehouse, scanned_by
            ) VALUES (%s, %s, NULL, NULL, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid4()), item_code, datetime.now(), scan_id,
            location, transaction_type, location_to_warehouse(location), scanned_by
        ))

def delete_scan_location(scan_id):
    with get_db_cursor() as cursor:
        cursor.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (scan_id,))

def insert_scan_location(scan_id, item_code, location):
    with get_db_cursor() as cursor:
        cursor.execute("""
            INSERT INTO current_scan_location (scan_id, item_code, location, updated_at)
            VALUES (%s, %s, %s, %s)
        """, (scan_id, item_code, location, datetime.now()))

def run():
    st.title("🔄 Pallet Decomposition / Recomposition Tool")

    if st.button("Reset Page"):
        st.experimental_rerun()

    item_code = st.text_input("Item Code")
    action_type = st.radio("Action", ["Decompose", "Recompose"])
    scanned_by = st.text_input("Username", "admin")

    if action_type == "Decompose":
        pallet_id = st.text_input("Pallet Scan ID")
        qty = st.number_input("How many unit scan_ids?", min_value=1, step=1)

        if st.button("Validate Pallet ID"):
            pallet_row = get_scan_location(pallet_id)
            if not pallet_row:
                st.error("Pallet Scan ID not found.")
            else:
                location = pallet_row["location"]
                with st.form("decompose_form"):
                    new_ids = [st.text_input(f"Scan ID #{i+1}") for i in range(qty)]
                    submitted = st.form_submit_button("Decompose")
                    if submitted:
                        if any(scan_id_exists(sid) for sid in new_ids):
                            st.error("One or more scan_ids already exist.")
                        else:
                            delete_scan_location(pallet_id)
                            insert_verification(pallet_id, item_code, location, "Decomposed", scanned_by)
                            for scan_id in new_ids:
                                insert_scan_location(scan_id, item_code, location)
                                insert_verification(scan_id, item_code, location, "Decomposed Product", scanned_by)
                            st.success(f"Pallet {pallet_id} decomposed into {qty} scans.")

    elif action_type == "Recompose":
        qty = st.number_input("How many unit scan_ids?", min_value=1, step=1, key="recompose_qty")
        new_pallet_id = st.text_input("New Pallet Scan ID")
        with st.form("recompose_form"):
            scan_ids = [st.text_input(f"Scan ID #{i+1}", key=f"recompose_scan_{i}") for i in range(qty)]
            submitted = st.form_submit_button("Recompose")
            if submitted:
                if scan_id_exists(new_pallet_id):
                    st.error("Pallet ID already exists.")
                else:
                    rows = [get_scan_location(sid) for sid in scan_ids]
                    if not all(rows):
                        st.error("One or more scan_ids not found.")
                    else:
                        location = rows[0]["location"]
                        if any(r["location"] != location or r["item_code"] != item_code for r in rows):
                            st.error("Inconsistent metadata across scan_ids.")
                        else:
                            for sid in scan_ids:
                                delete_scan_location(sid)
                                insert_verification(sid, item_code, location, "Recomposed Assets", scanned_by)
                            insert_scan_location(new_pallet_id, item_code, location)
                            insert_verification(new_pallet_id, item_code, location, "Recomposed", scanned_by)
                            st.success(f"New pallet {new_pallet_id} created from {qty} scans.")
