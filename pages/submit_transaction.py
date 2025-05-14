# pages/submit_transaction.py

import streamlit as st
from db import get_db_cursor

# you’ll still import your STAGING_LOCATIONS or WAREHOUSE_OPTIONS from config.py
from config import WAREHOUSE_OPTIONS

def run():
    st.header("📑 Submit Transaction — Receiving")

    # 1) Gather inputs
    item_code    = st.text_input("Item Code",        key="recv_item_code")
    quantity     = st.number_input("Quantity", min_value=1, step=1, key="recv_quantity")
    job_number   = st.text_input("Job Number",       key="recv_job_number")
    lot_number   = st.text_input("Lot Number",       key="recv_lot_number")
    to_location  = st.text_input("Receiving Location", key="recv_to_location")
    po_number    = st.text_input("PO Number",          key="recv_po_number")
    warehouse    = st.selectbox("Warehouse", WAREHOUSE_OPTIONS, key="recv_warehouse")

    # 2) Scan inputs (one field per unit)
    scan_inputs = []
    for i in range(quantity):
        scan_inputs.append(
            st.text_input(f"Scan {i+1} of {quantity}", key=f"recv_scan_{i}")
        )

    # 3) Submit button
    if st.button("Confirm & Submit Receiving"):
        # — 3a) Basic field validation
        missing = []
        for name, val in [
            ("Item Code", item_code),
            ("Job Number", job_number),
            ("Lot Number", lot_number),
            ("Receiving Location", to_location),
            ("PO Number", po_number),
            ("Warehouse", warehouse),
        ]:
            if not val:
                missing.append(name)

        if missing:
            st.error(f"Missing required fields: {', '.join(missing)}")
            return

        if any(not s.strip() for s in scan_inputs):
            st.error("All scan entries must be filled out.")
            return

        # — 3b) Write to the DB
        try:
            with get_db_cursor() as cur:
                # 1) Insert into transactions, grab the new ID
                cur.execute("""
                    INSERT INTO transactions (
                        transaction_type,
                        item_code, quantity, date,
                        job_number, lot_number, po_number,
                        from_location, to_location,
                        user_id, bypassed_warning, note, warehouse
                    ) VALUES (
                        %s, %s, %s, NOW(),
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s
                    )
                    RETURNING id
                """, (
                    "Receiving",
                    item_code, quantity,
                    job_number, lot_number, po_number,
                    None, to_location,
                    st.session_state.user_id,  # or however you get it
                    False,                     # bypassed_warning
                    "",                        # note
                    warehouse
                ))
                txn_id = cur.fetchone()[0]

                # 2) Insert each scan into scan_verifications
                for scan in scan_inputs:
                    cur.execute("""
                        INSERT INTO scan_verifications (
                            transaction_id,
                            scan_value,
                            scan_time,
                            warehouse
                        ) VALUES (
                            %s, %s, NOW(), %s
                        )
                    """, (txn_id, scan.strip(), warehouse))

            # 3c) Success feedback & reset
            st.success("✅ Receiving transaction recorded!")
            # clear your session state if needed…
            st.session_state.pop("recv_item_code", None)
            # (and similarly for other keys)
            st.experimental_rerun()

        except Exception as e:
            st.error(f"Failed to submit transaction: {e}")
