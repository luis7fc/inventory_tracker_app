import streamlit as st
from datetime import datetime
from db import get_db_cursor, insert_pulltag_line, finalize_scans
from config import WAREHOUSES

def run():
    st.markdown("""
        <style>
        [data-testid="stSidebarNav"] { display: none; }
        </style>
    """, unsafe_allow_html=True)

    st.title("🛠️ Post-Kitting Adjustments")

    st.markdown("""
    Use this tool to **add or remove material after original kitting is complete**. 
    Designed for mobile and desktop — enter one row at a time.
    """)

    transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"], help="Choose 'ADD' to allocate new material or 'RETURNB' to remove extras.")
    warehouse = st.selectbox("Warehouse", WAREHOUSES)
    location = st.text_input("Location", placeholder="e.g., STAGE-A")
    note = st.text_input("Transaction Note (Optional)")

    if "adjustments" not in st.session_state:
        st.session_state.adjustments = []

    with st.expander("➕ Add Adjustment Row"):
        job = st.text_input("Job Number")
        lot = st.text_input("Lot Number")
        code = st.text_input("Item Code")
        qty = st.number_input("Quantity", min_value=1, value=1, step=1)

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                st.session_state.adjustments.append({
                    "job_number": job.strip(),
                    "lot_number": lot.strip(),
                    "item_code": code.strip(),
                    "quantity": int(qty)
                })
            else:
                st.warning("Please complete all fields before adding.")

    if st.session_state.adjustments:
        st.markdown("### 📋 Adjustments Preview")
        st.dataframe(st.session_state.adjustments, use_container_width=True)

        if st.button("Submit Adjustments"):
            now = datetime.now()
            user = st.session_state.get("username", "unknown")
            scans_needed = {}
            job_lot_queue = []
            confirmed_rows = []

            for row in st.session_state.adjustments:
                job = row["job_number"]
                lot = row["lot_number"]
                code = row["item_code"]
                qty = row["quantity"]

                with get_db_cursor() as cur:
                    cur.execute("""
                        SELECT item_code, item_description FROM items_master 
                        WHERE item_code = %s AND cost_code = item_code
                    """, (code,))
                    result = cur.fetchone()

                    if not result:
                        st.info(f"ℹ️ Item {code} is not scan-tracked or not found. Skipped.")
                        continue

                    insert_pulltag_line(cur, job, lot, code, qty, transaction_type=transaction_type)

                job_lot_queue.append((job, lot))
                scans_needed.setdefault(code, {}).setdefault((job, lot), 0)
                scans_needed[code][(job, lot)] += qty
                confirmed_rows.append({"Job": job, "Lot": lot, "Item": code, "Qty": qty, "Type": transaction_type})

            if not scans_needed:
                st.warning("No valid adjustments submitted.")
                st.stop()

            st.markdown("### ✅ Valid Rows Submitted")
            st.dataframe(confirmed_rows, use_container_width=True)

            st.markdown("### 🔍 Scan Required Items")
            scan_inputs = {}
            for item_code, lots in scans_needed.items():
                total = sum(lots.values())
                st.write(f"**{item_code}** — Total Scans: {total}")
                for i in range(1, total + 1):
                    key = f"scan_{item_code}_{i}"
                    scan_inputs[key] = st.text_input(f"Scan {i} for {item_code}", key=key)

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
                    st.session_state.adjustments.clear()
