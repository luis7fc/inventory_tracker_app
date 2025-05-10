import streamlit as st
from db import get_pulltag_rows, submit_kitting, finalize_scans
from db import get_db_cursor  # for scan collection

# -----------------------------------------------------------------------------
# Main Streamlit App Entry
# -----------------------------------------------------------------------------

def run():
    st.title("📦 Job Kitting")

    # Source location input
    source_location = st.text_input(
        "Source Location",
        help="Location for issue/return (e.g., staging area)."
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
        rows = get_pulltag_rows(job, lot)
        if not rows:
            st.info("No pull-tags found for this combination.")
            continue

        # Table header
        headers = ["Code", "Desc", "Req", "UOM", "Kit", "Cost", "Status"]
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
                label="",
                min_value=-row['qty_req'],
                max_value=row['qty_req'],
                value=st.session_state.kitting_inputs.get(key, default),
                key=key
            )
            cols[5].write(row['cost_code'])
            cols[6].write(row['status'])
            st.session_state.kitting_inputs[(job, lot, row['item_code'])] = kq

        # Submit kitting for this Job/Lot
        if st.button(f"Submit Kitting for {job}-{lot}", key=f"submit_{job}_{lot}"):
            # Filter inputs for this combo
            kits = {k: v for k, v in st.session_state.kitting_inputs.items() if k[0] == job and k[1] == lot}
            submit_kitting(kits)
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
            if not source_location:
                st.error("Please enter a Source Location before finalizing scans.")
            else:
                finalize_scans(scans_needed, scan_inputs, st.session_state.job_lot_queue, source_location)
                st.success("Scans processed and inventory updated.")

# End of job_kitting.py
