import streamlit as st
from datetime import datetime
from db import (
    get_db_cursor,
    insert_transaction,
    insert_scan_verification,
    update_scan_location,
    update_current_inventory,
    get_item_metadata,
    insert_pulltag_line,
    validate_scan_for_transaction
)
from config import WAREHOUSES
from auth import require_login

# ──────────────────────────────────────────────────────────────────────────────
# Require login to access the page
require_login()

# ──────────────────────────────────────────────────────────────────────────────
st.title("➕ Add-On Pulltag (Job Issue/Return)")

# --- Select transaction type ---
transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"])

# --- Metadata inputs ---
job_number = st.text_input("Job Number").strip()
lot_number = st.text_input("Lot Number").strip()
location = st.text_input("Location").strip()
warehouse = st.selectbox("Warehouse", WAREHOUSES)
note = st.text_input("Optional Note")

# --- Add-on line entry ---
item_code = st.text_input("Item Code").strip()
quantity = st.number_input("Quantity", min_value=1, step=1)

# --- Scan inputs ---
scans = []
for i in range(quantity):
    scan = st.text_input(f"Scan [{i+1}]", key=f"scan_{i}").strip()
    scans.append(scan)

# ──────────────────────────────────────────────────────────────────────────────
if st.button("Submit Add-On Line"):
    if not all([job_number, lot_number, item_code, location, warehouse]):
        st.error("Please fill out all required fields.")
    elif any([s == "" for s in scans]):
        st.error("All scan fields must be filled.")
    else:
        now = datetime.now()
        user = st.session_state.get("username", "unknown")

        # Fetch metadata from items_master
        item_meta = get_item_metadata(item_code)
        if not item_meta:
            st.error("Item not found in items_master.")
            st.stop()

        cost_code = item_meta.get("cost_code")
        description = item_meta.get("description")
        uom = item_meta.get("uom")

        # Skip if not a 1:1 scanable item
        if cost_code != item_code:
            st.warning("Item does not require scan tracking. Skipped.")
            st.stop()

        # Insert new pulltag row
        insert_pulltag_line({
            "job_number": job_number,
            "lot_number": lot_number,
            "item_code": item_code,
            "cost_code": cost_code,
            "description": description,
            "quantity": quantity,
            "status": "complete",
            "uploaded_at": now,
            "last_updated": now,
            "warehouse": warehouse,
            "uom": uom,
            "transaction_type": transaction_type
        })

        # Insert transaction record
        insert_transaction({
            "transaction_type": transaction_type,
            "item_code": item_code,
            "quantity": quantity,
            "date": now,
            "job_number": job_number,
            "lot_number": lot_number,
            "po_number": None,
            "from_location": location if transaction_type == "ADD" else None,
            "to_location": location if transaction_type == "RETURNB" else None,
            "user_id": user,
            "bypassed_warning": False,
            "note": note,
            "warehouse": warehouse
        })

        # Insert scans into scan_verifications
        for scan in scans:
            if validate_scan_for_transaction(scan, item_code):
                insert_scan_verification({
                    "item_code": item_code,
                    "job_number": job_number,
                    "lot_number": lot_number,
                    "scan_time": now,
                    "location": location,
                    "transaction_type": transaction_type,
                    "warehouse": warehouse,
                    "pulltag_id": None,
                    "scanned_by": user
                })

                update_scan_location(scan_id=scan, item_code=item_code, location=location, updated_at=now)
            else:
                st.warning(f"⚠️ Duplicate or invalid scan: {scan}")

        # Update inventory
        update_current_inventory(item_code, location, quantity if transaction_type == "ADD" else -quantity, warehouse)

        st.success(f"✅ Add-on line submitted successfully for item {item_code}.")
