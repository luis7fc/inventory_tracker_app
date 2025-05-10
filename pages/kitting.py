import streamlit as st
from db import get_db_cursor

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

def get_pulltag_rows(job, lot):
    """
    Fetch pulltag rows for a given job/lot combination.
    Returns a list of dicts: warehouse, item_code, description, qty_req, uom, cost_code
    """
    query = (
        "SELECT warehouse, item_code, description, quantity AS qty_req, uom, cost_code "
        "FROM pulltags WHERE job_number = %s AND lot_number = %s"
    )
    with get_db_cursor() as cur:
        cur.execute(query, (job, lot))
        rows = cur.fetchall()
    return [
        {
            'warehouse': w,
            'item_code': ic,
            'description': desc,
            'qty_req': qty,
            'uom': u,
            'cost_code': cc
        }
        for (w, ic, desc, qty, u, cc) in rows
    ]


def submit_kitting(kits):
    """
    Update or delete pulltags based on kitted quantities.
    kits: dict keyed by (job,lot,item_code) -> kitted_qty
    """
    with get_db_cursor() as cur:
        for (job, lot, item_code), kq in kits.items():
            if kq > 0:
                cur.execute(
                    "UPDATE pulltags SET quantity = %s "
                    "WHERE job_number = %s AND lot_number = %s AND item_code = %s",
                    (kq, job, lot, item_code)
                )
            else:
                cur.execute(
                    "DELETE FROM pulltags "
                    "WHERE job_number = %s AND lot_number = %s AND item_code = %s",
                    (job, lot, item_code)
                )


def finalize_scans(scans_needed, scan_inputs, job_lot_queue, source_location):
    """
    Process scans: insert transactions, scan_verifications, update inventory, remove scan entries.
    scans_needed: dict mapping item_code -> {(job,lot): qty_needed}
    scan_inputs: dict mapping scan_{item_code}_{i} -> scan_id
    job_lot_queue: list of (job,lot) queued
    source_location: user-input location for kitting
    """
    with get_db_cursor() as cur:
        for item_code, lots in scans_needed.items():
            remaining = sum(lots.values())
            # FIFO per lot
            for (job, lot), need in lots.items():
                assign = min(need, remaining)
                if assign <= 0:
                    continue
                # fetch warehouse for record-keeping
                cur.execute(
                    "SELECT warehouse FROM pulltags "
                    "WHERE job_number = %s AND lot_number = %s AND item_code = %s LIMIT 1",
                    (job, lot, item_code)
                )
                res = cur.fetchone()
                warehouse = res[0] if res else None
                # 1) Insert transaction
                cur.execute(
                    "INSERT INTO transactions "
                    "(transaction_type,warehouse,source_location,job_number,lot_number,item_code,quantity) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    ("Job Issue", warehouse, source_location, job, lot, item_code, assign)
                )
                # 2) Insert scan verifications and enforce uniqueness
                for idx in range(1, assign + 1):
                    scan_id = scan_inputs.get(f"scan_{item_code}_{idx}")
                    cur.execute(
                        "SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s", (scan_id,)
                    )
                    if cur.fetchone()[0] > 0:
                        raise Exception(f"Scan {scan_id} already used; return required before reuse.")
                    cur.execute(
                        "INSERT INTO scan_verifications "
                        "(item_code,scan_id,job_number,lot_number,warehouse) "
                        "VALUES (%s,%s,%s,%s,%s)",
                        (item_code, scan_id, job, lot, warehouse)
                    )
                    # 3) Remove from current_scan_location if exists
                    cur.execute(
                        "DELETE FROM current_scan_location WHERE scan_id = %s", (scan_id,)
                    )
                # 4) Update current_inventory at source_location
                cur.execute(
                    "UPDATE current_inventory SET quantity = quantity - %s "
                    "WHERE item_code = %s AND location = %s",
                    (assign, item_code, source_location)
                )
                remaining -= assign
                if remaining <= 0:
                    break

# -----------------------------------------------------------------------------
# Main Streamlit App Entry
# -----------------------------------------------------------------------------

def run():
    st.title("📦 Job Kitting")

    # Source location input
    source_location = st.text_input(
        "Source Location",
        help="Enter the location from which items are kitted (e.g., staging area or bin)."
    ).strip()

    # Initialize session state
    if 'job_lot_queue' not in st.session_state:
        st.session_state.job_lot_queue = []
    if 'kitting_inputs' not in st.session_state:
        st.session_state.kitting_inputs = {}

    # 1) Add Job/Lot form
    with st.form("add_joblot_form", clear_on_submit=True):
        job = st.text_input("Job Number")
        lot = st.text_input("Lot Number")
        if st.form_submit_button("Add Job/Lot"):
            if job and lot:
                pair = (job.strip(), lot.strip())
                if pair not in st.session_state.job_lot_queue:
                    st.session_state.job_lot_queue.append(pair)
                else:
                    st.warning("Already queued.")
            else:
                st.error("Both fields required.")

    # 2) Kitting UI
    for job, lot in st.session_state.job_lot_queue:
        st.markdown(f"---\n**Job:** {job} | **Lot:** {lot}")
        rows = get_pulltag_rows(job, lot)
        if not rows:
            st.info("No pulltags found.")
            continue
        cols = st.columns([1,3,1,1,1,1])
        for c, h in zip(cols, ["Code","Desc","Req","UOM","Kit","Cost"]):
            c.markdown(f"**{h}**")
        for r in rows:
            cols = st.columns([1,3,1,1,1,1])
            cols[0].write(r['item_code']); cols[1].write(r['description'])
            cols[2].write(r['qty_req']); cols[3].write(r['uom'])
            key = f"kit_{job}_{lot}_{r['item_code']}"
            val = st.session_state.kitting_inputs.get(key, r['qty_req'])
            kq = cols[4].number_input("", min_value=0, max_value=r['qty_req'], value=val, key=key)
            cols[5].write(r['cost_code'])
            st.session_state.kitting_inputs[(job, lot, r['item_code'])] = kq
        if st.button(f"Submit Kitting {job}-{lot}", key=f"submit_{job}_{lot}"):
            submit_kitting({k: v for k, v in st.session_state.kitting_inputs.items() if k[0] == job and k[1] == lot})
            st.success(f"Kitting saved for {job}-{lot}.")

    # 3) Scan Collection
    scans_needed = {}
    for job, lot in st.session_state.job_lot_queue:
        with get_db_cursor() as cur:
            cur.execute(
                "SELECT item_code, quantity FROM pulltags "
                "WHERE job_number = %s AND lot_number = %s AND cost_code = item_code",
                (job, lot)
            )
            for ic, qty in cur.fetchall():
                scans_needed.setdefault(ic, {}).setdefault((job, lot), 0)
                scans_needed[ic][(job, lot)] += qty

    if scans_needed:
        st.markdown("---")
        st.subheader("🔍 Scan Collection")
        scan_inputs = {}
        for ic, lots in scans_needed.items():
            total = sum(lots.values())
            st.write(f"**{ic}** - Total: {total}")
            for i in range(1, total + 1):
                key = f"scan_{ic}_{i}"
                scan_inputs[key] = st.text_input(f"Scan {i}", key=key)
        if st.button("Finalize Scans"):
            if not source_location:
                st.error("Please enter a source location before finalizing scans.")
            else:
                finalize_scans(scans_needed, scan_inputs, st.session_state.job_lot_queue, source_location)
                st.success("Scans and inventory updated.")

# End of job_kitting.py
