# --- Submit Transaction Tab ---
import streamlit as st
from datetime import datetime
from config import STAGING_LOCATIONS
from db import get_db_connection, insert_transaction, insert_scan_verification
import psycopg2

# --- Helper Function ---
def get_target_location(transaction_type, from_loc, to_loc):
    if transaction_type in ["Receiving", "Return", "Manual Adjustment"]:
        return to_loc
    elif transaction_type == "Internal Movement":
        return to_loc
    elif transaction_type == "Job Issue":
        return from_loc
    return None

def run():
    st.header("🎞️ Submit Inventory Transaction")

    conn = get_db_connection()
    cursor = conn.cursor()

    if "scan_inputs" not in st.session_state:
        st.session_state.scan_inputs = []
    if "review_mode" not in st.session_state:
        st.session_state.review_mode = False

    transaction_type = st.selectbox("Transaction Type", ["Receiving", "Internal Movement", "Job Issue", "Return", "Manual Adjustment"])

    item_code = st.text_input("Item Code")
    total_qty = st.number_input("Total Quantity", step=1)
    job_number = lot_number = po_number = from_location = to_location = note = ""
    pallet_qty = 1
    warehouse = "VV"

    if transaction_type != "Manual Adjustment":
        pallet_qty = st.number_input("Pallet Quantity", min_value=1, value=1, step=1)

    if transaction_type in ["Job Issue", "Return"]:
        job_number = st.text_input("Job Number")
        lot_number = st.text_input("Lot Number")

    if transaction_type == "Receiving":
        po_number = st.text_input("PO Number")
        to_location = st.text_input("Receiving Location")

    elif transaction_type == "Internal Movement":
        from_location = st.text_input("From Location")
        to_location = st.text_input("To Location")

    elif transaction_type == "Manual Adjustment":
        total_qty = st.number_input("Total Quantity (+/-)", step=1, value=0)
        to_location = st.text_input("Location")
        note = st.text_area("Adjustment Note")

    elif transaction_type == "Return":
        to_location = st.text_input("Return To Location")
        if not to_location:
            st.error("Return transactions require a return location.")
            st.stop()

    elif transaction_type == "Job Issue":
        from_location = st.text_input("Issue From Location")
        warehouse = st.text_input("Warehouse Initials (e.g. VV, SAC, FNO)", value="VV")

    # Scan Inputs
    if transaction_type != "Manual Adjustment":
        expected_scans = total_qty // max(pallet_qty, 1)
        st.write(f"**Expected Scans:** {expected_scans}")
        st.session_state.scan_inputs = []
        for i in range(expected_scans):
            scan_val = st.text_input(f"Scan {i+1}", key=f"scan_{i}")
            st.session_state.scan_inputs.append(scan_val)

    # Review & Confirm Flow
    if not st.session_state.review_mode:
        if st.button("Review Transaction"):
            if transaction_type != "Manual Adjustment" and len(st.session_state.scan_inputs) != expected_scans:
                st.error("Scan count must match expected scan count based on total quantity and pallet quantity.")
                st.stop()
            st.session_state.review_mode = True
            st.rerun()

    else:
        st.subheader("🔎 Review Summary")
        st.write("**Transaction Type:**", transaction_type)
        st.write("**Item Code:**", item_code)
        st.write("**Quantity:**", total_qty)
        st.write("**Job / Lot:**", job_number, lot_number)
        st.write("**From / To Location:**", from_location, to_location)
        st.write("**PO Number:**", po_number)
        st.write("**Warehouse:**", warehouse)
        st.write("**Note:**", note)
        if transaction_type != "Manual Adjustment":
            st.write("**Scans:**")
            st.code("\n".join(st.session_state.scan_inputs))

        if st.button("Confirm and Submit"):
            signed_qty = total_qty
            bypassed_warning = False

            if transaction_type == "Internal Movement":
                signed_qty = -abs(total_qty)
                cursor.execute("SELECT quantity FROM current_inventory WHERE item_code = %s AND location = %s", (item_code, from_location))
                result = cursor.fetchone()
                available = result[0] if result else 0
                if available < total_qty:
                    st.warning(f"Only {available} units available in {from_location}. Admin override required.")
                    admin_pass = st.text_input("Enter admin password to override:", type="password")
                    if admin_pass != st.secrets["general"]["admin_password"]:
                        st.error("Incorrect admin password. Transaction blocked.")
                        st.stop()
                    bypassed_warning = True

            target_loc = get_target_location(transaction_type, from_location, to_location)

            cursor.execute("""
                SELECT multi_item_allowed FROM locations
                WHERE location_code = %s AND warehouse = %s
            """, (target_loc, warehouse))
            result = cursor.fetchone()
            is_multi_item = result and result[0]

            if transaction_type != "Manual Adjustment" and not is_multi_item:
                cursor.execute("""
                    SELECT item_code FROM current_inventory
                    WHERE location = %s AND quantity > 0
                """, (target_loc,))
                items_present = [row[0] for row in cursor.fetchall()]
                if items_present and any(existing != item_code for existing in items_present):
                    st.error(f"Location '{target_loc}' already has a different item with nonzero quantity. Only multi-item locations can hold multiple item types.")
                    st.stop()

            insert_transaction({
                "transaction_type": transaction_type,
                "item_code": item_code,
                "quantity": total_qty,
                "job_number": job_number,
                "lot_number": lot_number,
                "po_number": po_number,
                "from_location": from_location,
                "to_location": to_location,
                "from_warehouse": None,
                "to_warehouse": None,
                "user_id": st.session_state.user,
                "bypassed_warning": bypassed_warning,
                "note": note,
                "warehouse": warehouse
            })

            if transaction_type == "Internal Movement":
                cursor.execute("""
                    UPDATE current_inventory SET quantity = quantity - %s
                    WHERE item_code = %s AND location = %s
                """, (total_qty, item_code, from_location))
                cursor.execute("""
                    INSERT INTO current_inventory (item_code, location, quantity)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (item_code, location) DO UPDATE
                    SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, (item_code, to_location, total_qty))

            elif transaction_type == "Job Issue":
                cursor.execute("""
                    UPDATE current_inventory
                    SET quantity = quantity - %s
                    WHERE item_code = %s AND location = %s
                """, (total_qty, item_code, from_location))
                cursor.execute("SELECT quantity FROM current_inventory WHERE item_code = %s AND location = %s", (item_code, from_location))
                result = cursor.fetchone()
                remaining = result[0] if result else 0
                if remaining < 0:
                    st.warning(f"Warning: Inventory at {from_location} is now negative ({remaining}). Please investigate.")

            else:
                cursor.execute("""
                    INSERT INTO current_inventory (item_code, location, quantity)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (item_code, location) DO UPDATE
                    SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, (item_code, target_loc, signed_qty))

            for s in st.session_state.scan_inputs:
                insert_scan_verification({
                    "item_code": item_code,
                    "job_number": job_number,
                    "lot_number": lot_number,
                    "scan_id": s,
                    "location": target_loc,
                    "transaction_type": transaction_type,
                    "warehouse": warehouse
                })

            conn.commit()
            st.success("Transaction submitted and recorded. 🍻 Cheers, ya did your job well!")

            # --- Reset all relevant session state ---
            st.session_state.review_mode = False
            st.session_state.scan_inputs = []
            st.rerun()

        if st.button("Cancel Review"):
            st.session_state.review_mode = False
            st.rerun()
