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
    st.header("📦 Submit Inventory Transaction")

    conn = get_db_connection()
    cursor = conn.cursor()

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

    scans = []
    if transaction_type != "Manual Adjustment":
        expected_scans = total_qty // max(pallet_qty, 1)
        for i in range(expected_scans):
            scans.append(st.text_input(f"Scan {i+1}"))

    if st.button("Submit Transaction"):
        if transaction_type != "Manual Adjustment" and len(scans) != expected_scans:
            st.error("Scan count must match expected scan count based on total quantity and pallet quantity.")
            st.stop()

        signed_qty = total_qty
        if transaction_type == "Internal Movement":
            signed_qty = -abs(total_qty)

        bypassed_warning = False
        if transaction_type == "Internal Movement":
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

        if transaction_type != "Manual Adjustment" and target_loc not in STAGING_LOCATIONS:
            cursor.execute(""" SELECT DISTINCT item_code FROM current_inventory WHERE location = %s AND quantity > 0""", (target_loc,))
            items_present = cursor.fetchall()
            if any(existing[0] != item_code for existing in items_present):
                st.error(f"Location '{target_loc}' already has a different item. Only staging locations can hold multiple item types.")
                st.stop()

        if transaction_type != "Manual Adjustment":
            cursor.execute("SELECT 1 FROM locations WHERE location_code = %s", (target_loc,))
            if not cursor.fetchone():
                st.warning(f"Location '{target_loc}' is not in the locations table. Admin override required.")
                admin_pass = st.text_input("Enter admin password to override:", type="password")
                if admin_pass != st.secrets["general"]["admin_password"]:
                    st.error("Incorrect admin password. Transaction blocked.")
                    st.stop()
                bypassed_warning = True

        # Insert transaction using db helper
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

        # Insert each scan using db helper
        for s in scans:
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
        st.success("Transaction submitted and recorded.")
