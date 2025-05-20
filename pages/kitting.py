import streamlit as st
import pandas as pd

from qr_snapshot import generate_qr_snapshot_from_df
from db import (
    get_pulltag_rows,
    finalize_scans,
    get_db_cursor,
    insert_pulltag_line,
    update_pulltag_line,
    delete_pulltag_line,
)

# -----------------------------------------------------------------------------
# Main Streamlit App Entry
# -----------------------------------------------------------------------------

def run():
    """Job‑kitting workflow page."""

    # ── Global style tweaks ──────────────────────────────────────────────
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("📦 Job Kitting")

    # ── Transaction context ─────────────────────────────────────────────
    tx_type = st.selectbox(
        "Transaction Type",
        ["Issue", "Return"],
        help=(
            "Select 'Issue' to pull stock from this location, "
            "or 'Return' to credit it here."
        ),
    )
    location = (
        st.text_input(
            "Location",
            help=(
                "If Issue: this is **from_location**; "
                "if Return: this is **to_location**."
            ),
        )
        .strip()
    )

    # ── Init session state ──────────────────────────────────────────────
    st.session_state.setdefault("job_lot_queue", [])
    st.session_state.setdefault("kitting_inputs", {})

    # ── 1)  Add Job/Lot to queue ────────────────────────────────────────
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

    visible_rows: list[dict] = []

    # ── 2)  Kitting UI per queued Job/Lot ───────────────────────────────
    for job, lot in st.session_state.job_lot_queue:
        st.markdown(f"---\n**Job:** {job} | **Lot:** {lot}")

        # 2.1) Add new kitted item
        with st.form(f"add_new_line_{job}_{lot}", clear_on_submit=True):
            new_code = st.text_input(
                "Item Code",
                placeholder="Scan or type item_code",
                key=f"new_code_{job}_{lot}",
            )
            new_qty = st.number_input(
                "Quantity Kitted",
                min_value=1,
                step=1,
                key=f"new_qty_{job}_{lot}",
            )
            add_clicked = st.form_submit_button("Add Item")

        if add_clicked:
            inserted = False
            with get_db_cursor() as cur:
                # validate code exists
                cur.execute(
                    "SELECT 1 FROM items_master WHERE item_code = %s", (new_code,)
                )
                if cur.fetchone():
                    insert_pulltag_line(
                        cur,
                        job,
                        lot,
                        new_code,
                        new_qty,
                        transaction_type="Job Issue" if tx_type == "Issue" else "Return",
                    )
                    inserted = True
            if not inserted:
                st.error(f"`{new_code}` not found in items_master!")
            else:
                st.success(f"Added {new_qty} × `{new_code}` to {job}-{lot}.")
                st.rerun()

        # 2.2) Load existing rows for this Job/Lot
        rows = get_pulltag_rows(job, lot)
        if not rows:
            st.info("No pull‑tags found for this combination.")
            continue

        visible_rows.extend(rows)

        # Table header
        hdr_cols = ["Code", "Desc", "Req", "UOM", "Kit", "Cost Code", "Status"]
        cols = st.columns([1, 3, 1, 1, 1, 1, 1])
        for col, hdr in zip(cols, hdr_cols):
            col.markdown(f"**{hdr}**")

        # Editable rows
        for r in rows:
            cols = st.columns([1, 3, 1, 1, 1, 1, 1])
            cols[0].write(r["item_code"])
            cols[1].write(r["description"])
            cols[2].write(r["qty_req"])
            cols[3].write(r["uom"])
            key = f"kit_{job}_{lot}_{r['item_code']}"
            default = r["qty_req"]
            kq = cols[4].number_input(
                label="Kit Qty",
                min_value=-r["qty_req"],
                max_value=r["qty_req"],
                value=st.session_state.kitting_inputs.get(key, default),
                key=key,
                label_visibility="collapsed",
            )
            cols[5].write(r["cost_code"])
            cols[6].write(r["status"])
            st.session_state.kitting_inputs[(job, lot, r["item_code"])] = kq

        # 2.3) Submit CRUD mutations
        if st.button(f"Submit Kitting for {job}-{lot}", key=f"submit_{job}_{lot}"):
            kits = {
                code: qty
                for (j, l, code), qty in st.session_state.kitting_inputs.items()
                if j == job and l == lot
            }
            with get_db_cursor() as cur:
                existing_codes = {r["item_code"] for r in rows}

                # INSERT brand‑new codes
                for code, qty in kits.items():
                    if code not in existing_codes and qty > 0:
                        insert_pulltag_line(
                            cur,
                            job,
                            lot,
                            code,
                            qty,
                            transaction_type="Job Issue" if tx_type == "Issue" else "Return",
                        )

                # UPDATE / DELETE
                for r in rows:
                    new_qty = kits.get(r["item_code"], 0)
                    if new_qty == 0:
                        delete_pulltag_line(cur, r["id"])
                    elif new_qty != r["qty_req"]:
                        update_pulltag_line(cur, r["id"], new_qty)
            st.success(f"Kitting updated for {job}-{lot}.")

    # ── 3)  QR snapshot for all visible rows ─────────────────────────────
    if visible_rows:
        visible_df = pd.DataFrame(visible_rows)
        keep_cols = [
            "id",
            "job_number",
            "lot_number",
            "item_code",
            "description",
            "qty_req",
            "uom",
        ]
        visible_df = visible_df.reindex(columns=[c for c in keep_cols if c in visible_df])

        st.markdown("---")
        st.subheader("QR Snapshot")

        if st.button("Generate QR for ALL rows in view"):
            with st.spinner("Building kit QR…"):
                url, png_bytes = generate_qr_snapshot_from_df(
                    visible_df, st.session_state.user
                )
                st.image(png_bytes, width=220)
                st.download_button(
                    "Download bundle QR",
                    png_bytes,
                    file_name="bundle_qr.png",
                    mime="image/png",
                )
                st.success("QR ready! Attach the label with NIIMBOT.")

    # ── 4)  Scan collection & finalization ───────────────────────────────
    scans_needed: dict[str, dict[tuple[str, str], int]] = {}
    for job, lot in st.session_state.job_lot_queue:
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT item_code, quantity
                FROM   pulltags
                WHERE  job_number = %s
                  AND  lot_number = %s
                  AND  cost_code = item_code
                """,
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
                scan_inputs[key] = st.text_input(
                    f"Scan {i}", key=key, label_visibility="collapsed"
                )

        if st.button("Finalize Scans"):
            if not location:
                st.error("Please enter a Location before finalizing scans.")
            else:
                sb = st.session_state.user

                # progress bar
                total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
                progress_bar = st.progress(0)

                def update_progress(pct: int):
                    progress_bar.progress(pct)

                with st.spinner("Processing scans…"):
                    if tx_type == "Issue":
                        finalize_scans(
                            scans_needed,
                            scan_inputs,
                            st.session_state.job_lot_queue,
                            from_location=location,
                            to_location=None,
                            scanned_by=sb,
                            progress_callback=update_progress,
                        )
                    else:  # Return
                        finalize_scans(
                            scans_needed,
                            scan_inputs,
                            st.session_state.job_lot_queue,
                            from_location=None,
                            to_location=location,
                            scanned_by=sb,
                            progress_callback=update_progress,
                        )
                st.success("Scans processed and inventory updated.")

# End of kitting.py
