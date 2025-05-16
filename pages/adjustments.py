import streamlit as st
from datetime import datetime
from db import get_db_cursor
from config import WAREHOUSES


# ──────────────────────────────────────────────────────────────────────────────
# Helper: insert a pull-tag row (adds warehouse + kitted status)
# ──────────────────────────────────────────────────────────────────────────────
def insert_pulltag_line(
    cur,
    job_number,
    lot_number,
    item_code,
    quantity,
    transaction_type="Job Issue",
    warehouse=None,
    status="kitted",
):
    sql = """
        INSERT INTO pulltags
              (job_number, lot_number, item_code, quantity,
               description, cost_code, uom, status, transaction_type, warehouse)
        SELECT %s, %s, item_code, %s, item_description,
               cost_code, uom, %s, %s, %s
        FROM   items_master
        WHERE  item_code = %s
        RETURNING id
    """
    cur.execute(
        sql,
        (
            job_number,
            lot_number,
            quantity,
            status,
            transaction_type,
            warehouse,
            item_code,
        ),
    )
    return cur.fetchone()[0]


# ──────────────────────────────────────────────────────────────────────────────
# Transaction finaliser (ADD / RETURNB)
# ──────────────────────────────────────────────────────────────────────────────
def finalize_add(
    scans_needed,
    scan_inputs,
    job_lot_queue,
    *,
    from_location,
    to_location=None,
    scanned_by=None,
    progress_callback=None,
    warehouse=None,
):
    if not warehouse:
        raise ValueError("Warehouse was not provided to finalize_add.")

    total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
    done = 0

    with get_db_cursor() as cur:
        for item_code, lots in scans_needed.items():
            total_needed = sum(lots.values())

            for (job, lot), need in lots.items():
                assign = min(need, total_needed)
                if assign == 0:
                    continue

                trans_type = "Return" if to_location and not from_location else "Job Issue"
                loc_value  = to_location or from_location
                sb         = scanned_by

                # dynamic from/to column
                loc_col = "to_location" if trans_type == "Return" else "from_location"
                cur.execute(
                    f"""
                    INSERT INTO transactions
                        (transaction_type, date, warehouse, {loc_col},
                         job_number, lot_number, item_code, quantity, user_id)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (trans_type, warehouse, loc_value, job, lot, item_code, assign, sb),
                )

                # ── pallet-level override ───────────────────────────────────
                pallet_id  = scan_inputs.get(f"pallet_{item_code}_{job}_{lot}", "").strip()
                pallet_qty = int(scan_inputs.get(f"pallet_qty_{item_code}_{job}_{lot}", "1"))

                if pallet_id and pallet_qty > 1 and trans_type == "Job Issue":
                    cur.execute(
                        "SELECT location FROM current_scan_location WHERE scan_id = %s",
                        (pallet_id,),
                    )
                    found = cur.fetchone()
                    if not found:
                        raise Exception(
                            f"Pallet ID **{pallet_id}** not found in current_scan_location."
                        )
                    if found[0] != from_location:
                        raise Exception(
                            f"Pallet ID **{pallet_id}** is in {found[0]}, "
                            f"not {from_location}."
                        )

                    # remove pallet from its current location
                    cur.execute(
                        "DELETE FROM current_scan_location WHERE scan_id = %s",
                        (pallet_id,),
                    )
                    # log verification
                    cur.execute(
                        """
                        INSERT INTO scan_verifications
                               (item_code, scan_id, job_number, lot_number,
                                scan_time, location, transaction_type,
                                warehouse, scanned_by)
                        VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                        """,
                        (
                            item_code,
                            pallet_id,
                            job,
                            lot,
                            from_location,
                            trans_type,
                            warehouse,
                            sb,
                        ),
                    )
                    done += 1
                else:
                    # ── individual scans ──────────────────────────────────
                    for idx in range(1, assign + 1):
                        sid = scan_inputs.get(
                            f"scan_{item_code}_{job}_{lot}_{idx}", ""
                        ).strip()
                        if not sid:
                            continue

                        cur.execute(
                            "SELECT location FROM current_scan_location WHERE scan_id = %s",
                            (sid,),
                        )
                        existing = cur.fetchone()

                        if trans_type == "Return":
                            if existing:
                                raise Exception(
                                    f"Scan **{sid}** already exists in {existing[0]}."
                                )
                            cur.execute(
                                """
                                INSERT INTO current_scan_location
                                        (scan_id, item_code, location)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (scan_id)
                                    DO UPDATE SET location = EXCLUDED.location
                                """,
                                (sid, item_code, to_location),
                            )
                        else:  # Job Issue
                            if existing and existing[0] != from_location:
                                raise Exception(
                                    f"Scan **{sid}** is in {existing[0]} "
                                    f"(expected {from_location})."
                                )
                            cur.execute(
                                "DELETE FROM current_scan_location WHERE scan_id = %s",
                                (sid,),
                            )

                        cur.execute(
                            """
                            INSERT INTO scan_verifications
                                   (item_code, scan_id, job_number, lot_number,
                                    scan_time, location, transaction_type,
                                    warehouse, scanned_by)
                            VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                            """,
                            (
                                item_code,
                                sid,
                                job,
                                lot,
                                loc_value,
                                trans_type,
                                warehouse,
                                sb,
                            ),
                        )

                        done += 1
                        if progress_callback:
                            progress_callback(int(done / total_scans * 100))

                # update inventory
                delta = assign if trans_type == "Return" else -assign
                cur.execute(
                    """
                    INSERT INTO current_inventory
                           (item_code, location, quantity, warehouse)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                        SET quantity = current_inventory.quantity + EXCLUDED.quantity
                    """,
                    (item_code, loc_value, delta, warehouse),
                )

                total_needed -= assign
                if total_needed <= 0:
                    break


# ──────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ──────────────────────────────────────────────────────────────────────────────
def run():
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("🛠️ Post-Kitting Adjustments")

    transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"])
    warehouse        = st.selectbox("Warehouse", WAREHOUSES)
    location         = st.text_input("Location", placeholder="e.g., STAGE-A")
    note             = st.text_input("Transaction Note (optional)")

    # keep adjustments list in session
    adjustments = st.session_state.setdefault("adjustments", [])

    # ── add an adjustment row ───────────────────────────────────────────────
    with st.expander("➕ Add Adjustment Row"):
        job  = st.text_input("Job Number")
        lot  = st.text_input("Lot Number")
        code = st.text_input("Item Code")
        qty  = st.number_input("Quantity", min_value=1, value=1, step=1)

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                # pull description
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT item_description FROM items_master WHERE item_code = %s",
                        (code.strip(),),
                    )
                    res  = cur.fetchone()
                    desc = res[0] if res else "(Unknown Item)"

                adjustments.append(
                    {
                        "job_number": job.strip(),
                        "lot_number": lot.strip(),
                        "item_code": code.strip(),
                        "quantity": int(qty),
                        "description": desc,
                    }
                )
            else:
                st.warning("Please complete all fields before adding.")

    # ── preview list ────────────────────────────────────────────────────────
    if adjustments:
        st.markdown("### 📋 Adjustments Preview")
        for i, row in enumerate(adjustments):
            cols = st.columns([3, 3, 3, 2, 3, 1])
            cols[0].markdown(f"**Job:** {row['job_number']}")
            cols[1].markdown(f"**Lot:** {row['lot_number']}")
            cols[2].markdown(f"**Item:** {row['item_code']}")
            cols[3].markdown(f"**Qty:** {row['quantity']}")
            cols[4].markdown(f"**Desc:** {row['description']}")
            if cols[5].button("❌", key=f"remove_{i}"):
                adjustments.pop(i)
                st.rerun()

    # ── submit adjustments ─────────────────────────────────────────────────
    if adjustments and st.button("Submit Adjustments"):
        scans_needed   = {}
        job_lot_queue  = []
        confirmed_rows = []

        for row in adjustments:
            job, lot, code, qty = (
                row["job_number"],
                row["lot_number"],
                row["item_code"],
                row["quantity"],
            )

            # ── DB look-ups & pull-tag insert ────────────────────────────
            with get_db_cursor() as cur:          # ←--- INDENT **8 spaces** (same as the comment)
                cur.execute(
                    "SELECT item_code FROM items_master "
                    "WHERE item_code = %s AND cost_code = item_code",
                    (code,),
                )
                scan_tracked = bool(cur.fetchone())

                qty_store = -abs(qty) if transaction_type == "RETURNB" else qty
                insert_pulltag_line(
                    cur, job, lot, code, qty_store, transaction_type, warehouse
                )

            # ── build scan checklist only if tracking required ───────────
            if scan_tracked:
                scans_needed.setdefault(code, {}).setdefault((job, lot), 0)
                scans_needed[code][(job, lot)] += qty

            job_lot_queue.append((job, lot))
            confirmed_rows.append(
                {
                    "Job":  job,
                    "Lot":  lot,
                    "Item": code,
                    "Qty":  qty,
                    "Type": transaction_type,
                }
            )

        # ── nothing to scan? skip straight to “done” ──────────────────────
        if not scans_needed:
            st.success("✅ Adjustments posted—no scans required.")
            st.session_state.adjustments.clear()
            return

        # persist for second stage
        st.session_state.update(
            {
                "scans_needed": scans_needed,
                "job_lot_queue": job_lot_queue,
                "confirmed_rows": confirmed_rows,
                "finalize_ready": True,
            }
        )
        st.rerun()

    # ───────────────────────────────────────────────────────────────────
    # 2-step: SCAN ENTRY + FINALISE
    # ───────────────────────────────────────────────────────────────────
    if st.session_state.get("finalize_ready"):
        scans_needed   = st.session_state["scans_needed"]
        job_lot_queue  = st.session_state["job_lot_queue"]

        st.markdown("### 🔍 Scan Required Items")
        with st.form("scan_form"):
            scan_inputs = {}
            for item_code, lots in scans_needed.items():
                for (job, lot), qty in lots.items():
                    st.write(
                        f"**{item_code} — Job {job} / Lot {lot} — Total Scans: {qty}**"
                    )
                    key_prefix = f"{item_code}_{job}_{lot}"
                    scan_inputs[f"pallet_{key_prefix}"]     = st.text_input(
                        "Optional Pallet ID", key=f"pallet_{key_prefix}"
                    )
                    scan_inputs[f"pallet_qty_{key_prefix}"] = st.number_input(
                        "Pallet Quantity",
                        min_value=1,
                        value=1,
                        step=1,
                        key=f"pallet_qty_{key_prefix}",
                    )
                    for i in range(1, qty + 1):
                        scan_inputs[f"scan_{key_prefix}_{i}"] = st.text_input(
                            f"Scan {i} for {item_code}",
                            key=f"scan_{key_prefix}_{i}",
                        )

            submitted = st.form_submit_button("Finalize Adjustments")

        if submitted:
            if not location.strip():
                st.error("Please enter a **Location** before finalising.")
                st.stop()

            sb = st.session_state.get("username", "unknown")
            progress_bar = st.progress(0)

            with st.spinner("Processing adjustments…"):

                def update_progress(pct: int):
                    progress_bar.progress(pct)

                try:
                    finalize_add(
                        scans_needed,
                        scan_inputs,
                        job_lot_queue,
                        from_location=location if transaction_type == "ADD" else None,
                        to_location=location if transaction_type == "RETURNB" else None,
                        scanned_by=sb,
                        progress_callback=update_progress,
                        warehouse=warehouse,
                    )
                except Exception as err:
                    st.error(f"⚠️ {err}")
                    st.stop()  # leave widgets intact for correction

            st.success("✅ Adjustments finalised and inventory updated.")
            # reset session
            for k in ("adjustments", "scans_needed", "job_lot_queue", "confirmed_rows"):
                st.session_state.pop(k, None)
            st.session_state.finalize_ready = False
