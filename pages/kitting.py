import streamlit as st
from db import (
    get_pulltag_rows,
    finalize_scans,
    submit_kitting,
    get_db_cursor,
    insert_pulltag_line,
    update_pulltag_line,
    delete_pulltag_line,
)

# -----------------------------------------------------------------------------
# Main Streamlit App Entry
# -----------------------------------------------------------------------------

def run():
    st.title("📦 Job Kitting")

    # Transaction type + single location input
    tx_type = st.selectbox(
        "Transaction Type",
        ["Issue", "Return"],
        help="Select 'Issue' to pull stock from this location, or 'Return' to credit it here."
    )
    location = st.text_input(
        "Location",
        help="If Issue: this is your from_location; if Return: this is your to_location."
    ).strip()

    # Initialize session state
    if 'job_lot_queue' not in st.session_state:
        st.session_state.job_lot_queue = []
    if 'kitting_inputs' not in st.session_state:
        st.session_state.kitting_inputs = {}

    # 1) Add Job/Lot form
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

    # 2) Kitting UI for each queued Job/Lot
    for job, lot in st.session_state.job_lot_queue:
        st.markdown(f"---\n**Job:** {job} | **Lot:** {lot}")

        # 2.1) Add New Kitted Item scoped to this Job/Lot
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
            if st.form_submit_button("Add Item"):
                # Validate existence in items_master and insert
                with get_db_cursor() as conn:
                    cur = conn
                    cur.execute(
                        "SELECT item_description FROM items_master WHERE item_code = %s",
                        (new_code,)
                    )
                    found = cur.fetchone()
                    if not found:
                        st.error(f"`{new_code}` not found in items_master!")
                    else:
                        insert_pulltag_line(conn, job, lot, new_code, new_qty)
                        st.success(f"Added {new_qty} × `{new_code}` to {job}-{lot}.")
                        st.experimental_rerun()

        # 2.2) Load existing pull-tag rows
        rows = get_pulltag_rows(job, lot)
        if not rows:
            st.info("No pull-tags found for this combination.")
            continue

        # Table header
        headers = ["Code", "Desc", "Req", "UOM", "Kit", "Cost Code", "Status"]
        cols = st.columns([1, 3, 1, 1, 1, 1, 1])
        for col, hdr in zip(cols, headers):
            col.markdown(f"**{hdr}**")

        # Table rows with editable Kit Qty
        for row in rows:
            cols = st.columns([1, 3, 1, 1, 1, 1, 1])
            cols[0].write(row['item_code'])
            cols[1].write(row['description'])
            cols[2].write(row['qty_req'])
            cols[3].write(row['uom'])
            key = f"kit_{job}_{lot}_{row['item_code']}"
            default = row['qty_req']
            kq = cols[4].number_input(
                label="Kit Qty",
                min_value=-row['qty_req'],
                max_value=row['qty_req'],
                value=st.session_state.kitting_inputs.get(key, default),
                key=key,
                label_visibility="collapsed"
            )
            cols[5].write(row['cost_code'])
            cols[6].write(row['status'])
            st.session_state.kitting_inputs[(job, lot, row['item_code'])] = kq

        # 2.3) Submit kitting for this Job/Lot using CRUD helpers
        if st.button(f"Submit Kitting for {job}-{lot}", key=f"submit_{job}_{lot}"):
            kits = {
                code: qty
                for (j, l, code), qty in st.session_state.kitting_inputs.items()
                if j == job and l == lot
            }
            with get_db_cursor() as conn:
                existing = [r['item_code'] for r in rows]
                for code, qty in kits.items():
                    if code not in existing and qty > 0:
                        insert_pulltag_line(conn, job, lot, code, qty)
                for r in rows:
                    qty = kits.get(r['item_code'], 0)
                    if qty == 0:
                        delete_pulltag_line(conn, r['id'])
                    elif qty != r['qty_req']:
                        update_pulltag_line(conn, r['id'], qty)
            st.success(f"Kitting updated for {job}-{lot}.")

    # 3) Scan Collection
    scans_needed = {}
    for job, lot in st.session_state.job_lot_queue:
        with get_db_cursor() as cur:
            cur.execute(
                "SELECT item_code, quantity FROM pulltags "
                "WHERE job_number = %s AND lot_number = %s AND cost_code = item_code",
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
            total = sum(lots.values())
            st.write(f"**{item_code}** — Total Scans: {total}")
            for i in range(1, total + 1):
                key = f"scan_{item_code}_{i}"
                scan_inputs[key] = st.text_input(f"Scan {i}", key=key)
                
        if st.button("Finalize Scans"):
            if not location:
                st.error("Please enter a Location before finalizing scans.")
            else:
                sb = st.session_state.user

                # 1) Calculate total scans for the progress bar
                total_scans = sum(
                    qty
                    for lots in scans_needed.values()
                    for qty in lots.values()
                )

                # 2) Create and initialize the progress bar
                progress_bar = st.progress(0)

                # 3) Show a spinner while scans are processed
                with st.spinner("Processing scans…"):
                    def update_progress(pct: int):
                        # pct is an integer from 0 to 100
                        progress_bar.progress(pct)

                    # 4) Call finalize_scans with our progress callback
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
                    else:  # Return
                        finalize_scans(
                            scans_needed,
                            scan_inputs,
                            st.session_state.job_lot_queue,
                            from_location=None,
                            to_location=location,
                            scanned_by=sb,
                            progress_callback=update_progress
                        )

        # 5) Final success message
        st.success("Scans processed and inventory updated.")

# End of job_kitting.py
