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

def check_existing_scan_ids(scan_ids):
    with get_db_cursor() as cursor:
        cursor.execute("SELECT scan_id FROM scan_verifications WHERE scan_id = ANY(%s)", (scan_ids,))
        return {row[0] for row in cursor.fetchall()}

def location_to_warehouse(location):
    with get_db_cursor() as cursor:
        cursor.execute("SELECT warehouse FROM locations WHERE location_code = %s", (location,))
        result = cursor.fetchone()
        return result[0] if result else "UNKNOWN"

def insert_verification(cursor, scan_id, item_code, location, transaction_type, scanned_by, job_number):
    cursor.execute("""
        INSERT INTO scan_verifications (
            id, item_code, job_number, lot_number, scan_time, scan_id,
            location, transaction_type, warehouse, scanned_by
        ) VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s)
    """, (
        str(uuid4()), item_code, job_number, datetime.now(), scan_id,
        location, transaction_type, location_to_warehouse(location), scanned_by
    ))

def delete_scan_location(cursor, scan_id):
    cursor.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (scan_id,))

def insert_scan_location(cursor, scan_id, item_code, location):
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
    pallet_id = st.text_input("Pallet Scan ID")
    qty = st.number_input("How many unit scan_ids?", min_value=1, step=1)

    if location and location_to_warehouse(location) == "UNKNOWN":
        st.warning(f"⚠️ Location '{location}' not found in system. Please confirm spelling or register it.")

    if st.button("Validate Pallet ID"):
        pallet_row = get_scan_location(pallet_id)
        if not pallet_row:
            st.error(f"❌ Pallet ID '{pallet_id}' not found in current_scan_location.")
            st.session_state.pop("validated_pallet", None)
        elif pallet_row["item_code"] != item_code:
            st.error(f"❌ Item code mismatch: Pallet has '{pallet_row['item_code']}' but you entered '{item_code}'.")
            st.session_state.pop("validated_pallet", None)
        elif pallet_row["location"] != location:
            st.error(f"❌ Location mismatch: Pallet is in '{pallet_row['location']}' but you entered '{location}'.")
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

                existing_ids = check_existing_scan_ids(new_ids)
                if existing_ids:
                    st.error("❌ One or more scan_ids already exist.")
                    st.code("\n".join(existing_ids), language="text")
                    return

                warehouse = location_to_warehouse(location)
                if warehouse == "UNKNOWN":
                    st.error(f"❌ Invalid location: {location}. Please verify it exists in the system.")
                    return

                scanned_by = st.session_state.get("username", "system")

                try:
                    with get_db_cursor() as cursor:
                        cursor.execute("BEGIN")
                        delete_scan_location(cursor, pallet_id)
                        insert_verification(cursor, pallet_id, item_code, location, "Decomposed", scanned_by, pallet_id)
                        for sid in new_ids:
                            insert_scan_location(cursor, sid, item_code, location)
                            insert_verification(cursor, sid, item_code, location, "Decomposed Product", scanned_by, pallet_id)
                        cursor.execute("COMMIT")
                        st.success(f"✅ Decomposed pallet {pallet_id} into {qty} scans.")
                        st.session_state.pop("validated_pallet", None)
                except Exception as e:
                    try:
                        cursor.execute("ROLLBACK")
                    except Exception:
                        pass
                    st.error(f"❌ Transaction failed: {e}")
