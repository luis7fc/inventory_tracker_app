import streamlit as st
from datetime import datetime
from db import (
    get_db_cursor,
    get_item_metadata,
    insert_pulltag_line,
    finalize_scans
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
adjustment_rows = st.data_editor(
    [{"job_number": "", "lot_number": "", "item_code": "", "quantity": 1}],
    num_rows="dynamic",
    use_container_width=True,
    key="adjustment_editor"
)

if st.button("Submit Adjustments"):
    now = datetime.now()
    user = st.session_state.get("username", "unknown")
    scans_needed = {}
    job_lot_queue = []

    for row in adjustment_rows:
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

        # Add to pulltags
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

        # Track scans
        job_lot_queue.append((job, lot))
        scans_needed.setdefault(code, {}).setdefault((job, lot), 0)
        scans_needed[code][(job, lot)] += qty

    if not scans_needed:
        st.warning("No valid adjustments to process.")
        st.stop()

    st.markdown("---")
    st.subheader("🔍 Scan Collection")
    scan_inputs = {}
    for item_code, lots in scans_needed.items():
        total = sum(lots.values())
        st.write(f"**{item_code}** — Total Scans: {total}")
        for i in range(1, total + 1):
            key = f"scan_{item_code}_{i}"
            scan_inputs[key] = st.text_input(f"Scan {i}", key=key)

    if st.button("Finalize Adjustments"):
        if not location:
            st.error("Please enter a Location before finalizing.")
        else:
            sb = st.session_state.get("username", "unknown")
            progress_bar = st.progress(0)

            with st.spinner("Processing adjustments..."):
                def update_progress(pct: int):
                    progress_bar.progress(pct)

                finalize_scans(
                    scans_needed,
                    scan_inputs,
                    job_lot_queue,
                    from_location=location if transaction_type == "ADD" else None,
                    to_location=location if transaction_type == "RETURNB" else None,
                    scanned_by=sb,
                    progress_callback=update_progress
                )

            st.success("✅ Adjustments finalized and inventory updated.")
