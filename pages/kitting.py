import streamlit as st
import pandas as pd

from db import (
    get_pulltag_rows,
    finalize_scans,
    get_db_cursor,
    insert_pulltag_line,
    update_pulltag_line,
    delete_pulltag_line,
)

# -----------------------------------------------------------------------------
# Job Kitting Page (QR‑free version)  
# Removes all QR‑snapshot logic so we can focus on core kitting + scan flow.
# -----------------------------------------------------------------------------

def run():
    """Job‑kitting workflow without QR code generation."""

    # ── Basic styling (hide Streamlit multipage sidebar) ──────────────────
    st.markdown(
        """<style>[data-testid='stSidebarNav']{display:none;}</style>""",
        unsafe_allow_html=True,
    )
    st.title("📦 Job Kitting")

    # ── 0. Transaction context (Issue vs Return) ─────────────────────────
    tx_type = st.selectbox(
        "Transaction Type",
        ["Issue", "Return"],
        help="Select 'Issue' to remove stock from the location, or 'Return' to credit it back.",
    )
    location = st.text_input(
        "Location",
        help="If Issue: this is *from_location*; if Return: this is *to_location*.",
    ).strip()

    # ── 1. Session state bootstrapping ───────────────────────────────────
    st.session_state.setdefault("job_lot_queue", [])
    st.session_state.setdefault("kitting_inputs", {})

    # ── 2. Add Job/Lot to queue ──────────────────────────────────────────
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
                st.error("Both Job and Lot numbers are required.")

    # ── 3. Iterate over each queued Job/Lot ──────────────────────────────
    for job, lot in st.session_state.job_lot_queue:
        st.markdown(f"---\n**Job:** {job} | **Lot:** {lot}")

        # 3.1 Add new kitted line
        with st.form(f"add_line_{job}_{lot}", clear_on_submit=True):
            code = st.text_input("Item Code", key=f"code_{job}_{lot}")
            qty = st.number_input("Quantity", min_value=1, step=1, key=f"qty_{job}_{lot}")
            if st.form_submit_button("Add Item"):
                with get_db_cursor() as cur:
                    cur.execute("SELECT 1 FROM items_master WHERE item_code=%s", (code,))
                    if cur.fetchone():
                        insert_pulltag_line(
                            cur,
                            job,
                            lot,
                            code,
                            qty,
                            transaction_type="Job Issue" if tx_type == "Issue" else "Return",
                        )
                        st.success(f"Added {qty} × {code} to {job}-{lot}.")
                        st.rerun()
                    else:
                        st.error(f"Item code {code} not found in items_master.")

        # 3.2 Existing pull‑tag rows
        rows = get_pulltag_rows(job, lot)
        if not rows:
            st.info("No pull‑tags for this Job/Lot yet.")
            continue

        # Header
        hdr = ["Code", "Desc", "Req", "UOM", "Kit", "Cost", "Status"]
        hcols = st.columns([1, 3, 1, 1, 1, 1, 1])
        for col, title in zip(hcols, hdr):
            col.markdown(f"**{title}**")

        # Rows (editable Kit Qty)
        for r in rows:
            cols = st.columns([1, 3, 1, 1, 1, 1, 1])
            cols[0].write(r["item_code"])
            cols[1].write(r["description"])
            cols[2].write(r["qty_req"])
            cols[3].write(r["uom"])
            key = f"kit_{job}_{lot}_{r['item_code']}"
            input_qty = cols[4].number_input(
                label="Kit Qty",
                min_value=-r["qty_req"],
                max_value=r["qty_req"],
                value=st.session_state.kitting_inputs.get(key, r["qty_req"]),
                key=key,
                label_visibility="collapsed",
            )
            cols[5].write(r["cost_code"])
            cols[6].write(r["status"])
            st.session_state.kitting_inputs[(job, lot, r["item_code"])] = input_qty

        # 3.3 Commit edits to DB
        if st.button(f"Submit Kitting for {job}-{lot}", key=f"submit_{job}_{lot}"):
            pending = {
                code: qty
                for (j, l, code), qty in st.session_state.kitting_inputs.items()
                if j == job and l == lot
            }
            with get_db_cursor() as cur:
                existing_codes = {r["item_code"] for r in rows}
                # Inserts
                for code, qty in pending.items():
                    if code not in existing_codes and qty > 0:
                        insert_pulltag_line(cur, job, lot, code, qty, transaction_type="Job Issue" if tx_type == "Issue" else "Return")
                # Updates / Deletes
                for r in rows:
                    new_qty = pending.get(r["item_code"], 0)
                    if new_qty == 0:
                        delete_pulltag_line(cur, r["id"])
                    elif new_qty != r["qty_req"]:
                        update_pulltag_line(cur, r["id"], new_qty)
            st.success(f"Kitting saved for {job}-{lot}.")

    # ── 4. Scan collection & finalize (original logic retained) ───────────
    scans_needed: dict[str, dict[tuple[str, str], int]] = {}
    for job, lot in st.session_state.job_lot_queue:
        with get_db_cursor() as cur:
            cur.execute(
                "SELECT item_code, quantity FROM pulltags WHERE job_number=%s AND lot_number=%s AND cost_code=item_code",
                (job, lot),
            )
            for item_code, qty in cur.fetchall():
                scans_needed.setdefault(item_code, {}).setdefault((job, lot), 0)
                scans_needed[item_code][(job, lot)] += qty

    if scans_needed:
        st.markdown("---")
        st.subheader("🔍 Scan Collection")
        scan_inputs: dict[str, str] = {}
        for item_code, lots in scans_needed.items():
            total = sum(lots.values())
            st.write(f"**{item_code}** — Total Scans: {total}")
            for i in range(1, total + 1):
                key = f"scan_{item_code}_{i}"
                scan_inputs[key] = st.text_input(f"Scan {i}", key=key, label_visibility="collapsed")

        if st.button("Finalize Scans"):
            if not location:
                st.error("Please enter a Location before finalizing scans.")
            else:
                user = st.session_state.user
                total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
                progress = st.progress(0)

                def upd(pct: int):
                    progress.progress(pct)

                with st.spinner("Processing scans…"):
                    if tx_type == "Issue":
                        finalize_scans(scans_needed, scan_inputs, st.session_state.job_lot_queue, from_location=location, to_location=None, scanned_by=user, progress_callback=upd)
                    else:
                        finalize_scans(scans_needed, scan_inputs, st.session_state.job_lot_queue, from_location=None, to_location=location, scanned_by=user, progress_callback=upd)
                st.success("Scans processed and inventory updated.")
