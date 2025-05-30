import streamlit as st
from datetime import datetime
from uuid import uuid4
from db import get_db_cursor

# --- Helper DB functions ---
def get_scan_location(scan_id):
    with get_db_cursor() as cursor:
        cursor.execute("SELECT scan_id, item_code, location FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        result = cursor.fetchone()
        return {"scan_id": result[0], "item_code": result[1], "location": result[2]} if result else None

def scan_id_exists(scan_id):
    with get_db_cursor() as cursor:
        cursor.execute("SELECT 1 FROM scan_verifications WHERE scan_id = %s", (scan_id,))
        return cursor.fetchone() is not None

def location_to_warehouse(location):
    with get_db_cursor() as cursor:
        cursor.execute("SELECT warehouse FROM locations WHERE location_code = %s", (location,))
        result = cursor.fetchone()
        return result[0] if result else "UNKNOWN"

def insert_verification(scan_id, item_code, location, transaction_type, scanned_by, parent_id=None):
    with get_db_cursor() as cursor:
        cursor.execute("""
            INSERT INTO scan_verifications (
                item_code, job_number, lot_number, scan_time, scan_id,
                location, transaction_type, warehouse, scanned_by, pulltag_id
            ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, NULL)
        """, (
            item_code, parent_id, datetime.now(), scan_id,
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
    st.title("\U0001F501 Pallet Decomposition Tool")

    if st.button("Reset Page"):
        for key in ["validated_pallet", "decompose_scans"]:
            st.session_state.pop(key, None)
        st.rerun()

    item_code = st.text_input("Item Code").strip()
    location = st.text_input("Expected Location").strip()
    scanned_by = st.text_input("Username", "admin")
    pallet_id = st.text_input("Pallet Scan ID")
    qty = st.number_input("How many unit scan_ids?", min_value=1, step=1)

    if st.button("Validate Pallet ID"):
        pallet_row = get_scan_location(pallet_id)
        if not pallet_row:
            st.error("❌ Pallet ID not found.")
            st.session_state.pop("validated_pallet", None)
        elif pallet_row["item_code"] != item_code or pallet_row["location"] != location:
            st.error("❌ Pallet scan_id does not match the provided item_code and/or location.")
            st.session_state.pop("validated_pallet", None)
        else:
            st.session_state["validated_pallet"] = pallet_row
            st.success(f"✅ Pallet {pallet_id} is valid. Location: {location}")

    if "validated_pallet" in st.session_state:
        with st.form("decompose_form"):
            raw_input = st.text_area("Paste scan IDs (comma or newline separated):", key="decompose_scans")
            submitted = st.form_submit_button("Decompose")

            if submitted:
                new_ids = [s.strip() for s in raw_input.replace(",", "\n").splitlines() if s.strip()]

                if len(new_ids) != qty:
                    st.error(f"❌ Expected {qty} scan IDs but got {len(new_ids)}.")
                    return

                dupes = [sid for sid in new_ids if scan_id_exists(sid)]
                if dupes:
                    st.error(f"❌ These scan_ids already exist: {', '.join(dupes)}")
                    return

                try:
                    delete_scan_location(pallet_id)
                    insert_verification(pallet_id, item_code, location, "Decomposed", scanned_by)

                    for sid in new_ids:
                        insert_scan_location(sid, item_code, location)
                        insert_verification(sid, item_code, location, "Decomposed Product", scanned_by, parent_id=pallet_id)

                    st.success(f"✅ Decomposed pallet {pallet_id} into {qty} scans.")
                    st.session_state.pop("validated_pallet", None)

                except Exception as e:
                    st.error(f"❌ Decomposition failed: {e}")
                    st.stop()
