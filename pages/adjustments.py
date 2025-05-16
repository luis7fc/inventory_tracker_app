import streamlit as st
from datetime import datetime
from db import (
    get_db_cursor,
    insert_transaction,
    insert_scan_verification,
    update_scan_location,
    insert_pulltag_line,
    validate_scan_for_transaction,
    update_current_inventory,
    get_item_metadata
)
from config import WAREHOUSES

st.title("🛠️ Post-Kitting Adjustments")

st.markdown("""
Use this tool to add or remove material **after original kitting is complete**. 
Supports multiple job/lot entries at once.
""")

transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"], help="Choose 'ADD' to allocate new material or 'RETURNB' to remove extras.")
warehouse = st.selectbox("Warehouse", WAREHOUSES)
location = st.text_input("Location", placeholder="e.g., STAGE-A")
note = st.text_input("Transaction Note (Optional)")

st.markdown("### 📋 Add Job/Lot Adjustments")
rows = st.data_editor(
    [{"job_number": "", "lot_number": "", "item_code": "", "quantity": 1}],
    num_rows="dynamic",
    use_container_width=True,
    key="adjustment_editor"
)

if st.button("Submit Adjustments"):
    now = datetime.now()
    user = st.session_state.get("username", "unknown")

    for row in rows:
        job = row["job_number"].strip()
        lot = row["lot_number"].strip()
        code = row["item_code"].strip()
        qty = int(row["quantity"])

        if not all([job, lot, code]) or qty <= 0:
            st.warning(f"❌ Invalid entry skipped: {row}")
            continue

        meta = get_item_metadata(code)
        if not meta:
            st.warning(f"⚠️ Item not found in items_master: {code}")
            continue

        cost_code = meta.get("cost_code")
        if cost_code != code:
            st.info(f"ℹ️ Item {code} is not scan-tracked. Skipped.")
            continue

        desc = meta.get("description")
        uom = meta.get("uom")

        insert_pulltag_line({
            "job_number": job,
            "lot_number": lot,
            "item_code": code,
            "cost_code": cost_code,
            "description": desc,
            "quantity": qty,
            "status": "complete",
            "uploaded_at": now,
            "last_updated": now,
            "warehouse": warehouse,
            "uom": uom,
            "transaction_type": transaction_type
        })

        insert_transaction({
            "transaction_type": transaction_type,
            "item_code": code,
            "quantity": qty,
            "date": now,
            "job_number": job,
            "lot_number": lot,
            "po_number": None,
            "from_location": location if transaction_type == "ADD" else None,
            "to_location": location if transaction_type == "RETURNB" else None,
            "user_id": user,
            "bypassed_warning": False,
            "note": note,
            "warehouse": warehouse
        })

        for i in range(qty):
            scan = st.text_input(f"Scan {code} [{i+1}]", key=f"scan_{code}_{i}").strip()
            if not scan:
                st.warning(f"Missing scan {i+1} for {code}. Skipping.")
                continue

            if validate_scan_for_transaction(scan, code):
                insert_scan_verification({
                    "item_code": code,
                    "job_number": job,
                    "lot_number": lot,
                    "scan_time": now,
                    "location": location,
                    "transaction_type": transaction_type,
                    "warehouse": warehouse,
                    "pulltag_id": None,
                    "scanned_by": user
                })
                update_scan_location(scan_id=scan, item_code=code, location=location, updated_at=now)
            else:
                st.warning(f"⚠️ Duplicate or invalid scan: {scan}")

        update_current_inventory(code, location, qty if transaction_type == "ADD" else -qty, warehouse)

    st.success("✅ All valid adjustment rows submitted.")
