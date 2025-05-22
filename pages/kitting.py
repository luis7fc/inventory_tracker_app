import streamlit as st
from fpdf import FPDF
import tempfile
from db import (
    get_pulltag_rows,
    finalize_scans,
    submit_kitting,
    get_db_cursor,
    insert_pulltag_line,
    update_pulltag_line,
    delete_pulltag_line,
    generate_finalize_summary_pdf
)
import os

# -----------------------------------------------------------------------------
# Main Streamlit App Entry
# -----------------------------------------------------------------------------

def run():
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("📦 Job Kitting")

    tx_type = st.selectbox(
        "Transaction Type",
        ["Issue", "Return"],
        help="Select 'Issue' to pull stock from this location, or 'Return' to credit it here."
    )
    location = st.text_input(
        "Location",
        help="If Issue: this is your from_location; if Return: this is your to_location."
    ).strip()

    if 'job_lot_queue' not in st.session_state:
        st.session_state.job_lot_queue = []
    if 'kitting_inputs' not in st.session_state:
        st.session_state.kitting_inputs = {}

    with st.form("add_joblot", clear_on_submit=True):
        job = st.text_input("Job Number")
        lot = st.text_input("Lot Number")
        if st.form_submit_button("Add Job/Lot"):
            if job and lot:
                pair = (job.strip(), lot.strip())
                if pair not in st.session_state.job_lot_queue:
                    st.session_state.job_lot_queue.append(pair)
                else:
                    st.warning("This Job/Lot is already queued.")
            else:
                st.error("Both Job Number and Lot Number are required.")

    for job, lot in st.session_state.job_lot_queue:
        st.markdown(f"---\n**Job:** {job} | **Lot:** {lot}")

        if st.button(f"Remove {lot}", key=f"remove_{job}_{lot}"):
            st.session_state.job_lot_queue = [p for p in st.session_state.job_lot_queue if p != (job, lot)]
            st.rerun()

        st.markdown("### ➕ Add New Kitted Item")
        with st.form(f"add_new_line_{job}_{lot}", clear_on_submit=True):
            new_code = st.text_input(
                "Item Code", 
                placeholder="Scan or type item_code",
                key=f"new_code_{job}_{lot}"
            )
            new_qty = st.number_input(
                "Quantity Kitted", 
                min_value=1, 
                step=1,
                key=f"new_qty_{job}_{lot}"
            )
            add_clicked = st.form_submit_button("Add Item")

        if add_clicked:
            inserted = False
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT item_description FROM items_master WHERE item_code = %s",
                    (new_code,)
                )
                row = cur.fetchone()
                if row:
                    insert_pulltag_line(cur, job, lot, new_code, new_qty, transaction_type="Job Issue" if tx_type == "Issue" else "Return")
                    inserted = True
            if not inserted:
                st.error(f"`{new_code}` not found in items_master!")
            else:
                st.success(f"Added {new_qty} × `{new_code}` to {job}-{lot}.")
                st.rerun()

        rows = [r for r in get_pulltag_rows(job, lot) if r['transaction_type'] in ('Job Issue', 'Return')]

        if not rows:
            st.info("No pull-tags found for this combination.")
            continue

        if any(r['status'] in ('kitted', 'processed') for r in rows):
            st.warning(f"Job {job}, Lot {lot} is locked from edits (status: kitted/processed).")
            continue

        headers = ["Code", "Desc", "Req", "UOM", "Kit", "Cost Code", "Status"]
        with st.container():
            cols = st.columns([1, 3, 1, 1, 1, 1, 1])
            for col, hdr in zip(cols, headers):
                col.markdown(f"**{hdr}**")

            for row in rows:
                cols = st.columns([1, 3, 1, 1, 1, 1, 1])
                cols[0].write(row['item_code'])
                cols[1].write(row['description'])
                cols[2].write(row['qty_req'])
                cols[3].write(row['uom'])
                key = f"kit_{job}_{lot}_{row['item_code']}_{row['id']}"
                default = row['qty_req']
                kq = cols[4].number_input(
                    label="Kit Qty",
                    min_value=-999 if tx_type == "Return" else 0,
                    max_value=999,
                    value=st.session_state.kitting_inputs.get(key, default),
                    key=key,
                    label_visibility="collapsed"
                )
                cols[5].write(row['cost_code'])
                cols[6].write(row['status'])
                st.session_state.kitting_inputs[(job, lot, row['item_code'], row['id'])] = kq

        if st.button(f"Submit Kitting for {job}-{lot}", key=f"submit_{job}_{lot}"):
            kits = {
                code: qty
                for (j, l, code), qty in st.session_state.kitting_inputs.items()
                if j == job and l == lot
            }
            with get_db_cursor() as cur:
                existing = [r['item_code'] for r in rows]

                for code, qty in kits.items():
                    if code not in existing and qty != 0:
                        adjusted_qty = -abs(qty) if tx_type == "Return" else qty
                        insert_pulltag_line(cur, job, lot, code, adjusted_qty, transaction_type="Job Issue" if tx_type == "Issue" else "Return")

                for r in rows:
                    new_qty = kits.get(r['item_code'], 0)
                    adjusted_qty = -abs(new_qty) if tx_type == "Return" else new_qty
                    if adjusted_qty == 0:
                        delete_pulltag_line(cur, r['id'])
                    elif adjusted_qty != r['qty_req']:
                        update_pulltag_line(cur, r['id'], adjusted_qty)

            st.success(f"Kitting updated for {job}-{lot}.")
            
    scans_needed = {}
    for job, lot in st.session_state.job_lot_queue:
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT p.item_code, p.quantity
                FROM pulltags p
                JOIN items_master im ON p.item_code = im.item_code
                WHERE p.job_number = %s
                  AND p.lot_number = %s
                  AND im.scan_required = TRUE
                """,
                (job, lot)
            )
            for item_code, qty in cur.fetchall():
                scans_needed.setdefault(item_code, {}).setdefault((job, lot), 0)
                scans_needed[item_code][(job, lot)] += qty

    if scans_needed:
        st.markdown("---")
        st.subheader("🔍 Scan Collection")
        scan_inputs = {}
        for item_code, lots in scans_needed.items():
            for (job, lot), qty in lots.items():
                st.write(f"**{item_code} — {job}/{lot}** — Total Scans: {qty}")
                for i in range(1, qty + 1):
                    key = f"scan_{job}_{lot}_{item_code}_{i}"
                    scan_inputs[key] = st.text_input(f"Scan {i}", key=key)

        if st.button("Finalize Scans"):
            if not location:
                st.error("Please enter a Location before finalizing scans.")
            else:
                sb = st.session_state.user
                progress_bar = st.progress(0)
                with st.spinner("Processing scans…"):
                    def update_progress(pct: int):
                        progress_bar.progress(pct)

                    if tx_type == "Issue":
                        finalize_scans(
                            scans_needed,
                            scan_inputs,
                            st.session_state.job_lot_queue,
                            from_location=location,
                            to_location=None,
                            scanned_by=sb,
                            progress_callback=update_progress
                        )
                    else:
                        finalize_scans(
                            scans_needed,
                            scan_inputs,
                            st.session_state.job_lot_queue,
                            from_location=None,
                            to_location=location,
                            scanned_by=sb,
                            progress_callback=update_progress
                        )

                st.success("Scans processed and inventory updated.")
                summary_path = "/mnt/data/final_scan_summary.pdf"
                if os.path.exists(summary_path):
                    with open(summary_path, "rb") as f:
                        st.download_button(
                            label="📄 Download Final Scan Summary",
                            data=f,
                            file_name="final_scan_summary.pdf",
                            mime="application/pdf"
                        )
                    st.info("You may now download the summary and click below to refresh the page.")
                    if st.button("Reset Page"):
                        st.rerun()

# End of job_kitting.py
