import streamlit as st
import math
import random
from collections import Counter, defaultdict
from datetime import datetime

from db import get_db_cursor
from config import WAREHOUSES

# ──────────────────────────────────────────────────────────────────────────────
# Shared constants (no location-based exceptions now)
# ──────────────────────────────────────────────────────────────────────────────
IRISH_TOASTS = [
    "☘️ Sláinte! Transaction submitted successfully!",
    "🍀 Luck o’ the Irish – you did it!",
    "🦃 Cheers, let’s grab a beer – transaction success!",
    "🌈 Pot of gold secured – job well done!",
    "🪙 May your inventory always balance – success!",
]

# No kitting exceptions – all locations are treated equally for scan‑ID validation
SKIP_SCAN_CHECK_LOCATIONS: tuple[str, ...] = ()


# ──────────────────────────────────────────────────────────────────────────────
# Helper: insert a pull‑tag row
# ──────────────────────────────────────────────────────────────────────────────

def insert_pulltag_line(
    cur,
    job_number,
    lot_number,
    item_code,
    quantity,
    location,
    transaction_type="Job Issue",
    note="Qty Verified by WH",
):
    """Insert a new pulltag row using items_master metadata."""

    cur.execute(
        "SELECT warehouse FROM locations WHERE location_code = %s",
        (location,),
    )
    wh_row = cur.fetchone()
    if not wh_row:
        raise Exception(f"Unknown location '{location}' – cannot resolve warehouse.")
    warehouse = wh_row[0]

    sql = """
        INSERT INTO pulltags
              (job_number, lot_number, item_code, quantity,
               description, cost_code, uom, status,
               transaction_type, note, warehouse)
        SELECT %s, %s, item_code, %s,
               item_description, cost_code, uom,
               'pending', %s, %s, %s
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
            transaction_type,
            note,
            warehouse,
            item_code,
        ),
    )
    return cur.fetchone()[0]


# ──────────────────────────────────────────────────────────────────────────────
# Finaliser for ADD / RETURNB (no location exceptions)
# ──────────────────────────────────────────────────────────────────────────────

def finalize_add(
    scans_needed: dict,
    scan_inputs: dict,
    job_lot_queue: list,
    *,
    from_location: str | None,
    to_location: str | None = None,
    scanned_by: str | None = None,
    progress_callback=None,
    warehouse: str,
):
    if progress_callback is None:
        progress_callback = lambda *_: None

    # ── structure scans ────────────────────────────────────────────────────
    scan_map: defaultdict[tuple[str, str, str], list[str]] = defaultdict(list)
    errors: list[str] = []

    for item_code, lots in scans_needed.items():
        for (job, lot), qty in lots.items():
            for i in range(1, qty + 1):
                key = f"scan_{item_code}_{job}_{lot}_{i}"
                sid = scan_inputs.get(key, "").strip()
                if not sid:
                    errors.append(
                        f"Missing scan {i} for {item_code} — Job {job} / Lot {lot}."
                    )
                else:
                    scan_map[(item_code, job, lot)].append(sid)

    # duplicates
    flat = [s for bundle in scan_map.values() for s in bundle]
    dups = [s for s, c in Counter(flat).items() if c > 1]
    if dups:
        errors.append(f"Duplicate scan IDs entered: {', '.join(dups)}")

    if errors:
        raise Exception("\n".join(errors))

    # ── DB processing ──────────────────────────────────────────────────────
    total_scans = len(flat)
    processed   = 0

    with get_db_cursor() as cur:
        for (item_code, job, lot), sid_list in scan_map.items():
            trans_type = "Return" if to_location and not from_location else "Job Issue"
            loc_field  = to_location if trans_type == "Return" else from_location

            # single‑item location guard (multi_item_allowed == false)
            cur.execute(
                "SELECT multi_item_allowed FROM locations WHERE location_code = %s",
                (loc_field,),
            )
            row = cur.fetchone()
            if row and not row[0]:
                cur.execute(
                    "SELECT DISTINCT item_code FROM current_inventory WHERE location = %s AND quantity > 0",
                    (loc_field,),
                )
                existing = [r[0] for r in cur.fetchall()]
                if existing and any(ei != item_code for ei in existing):
                    raise Exception(
                        f"Location '{loc_field}' currently holds other item codes ({', '.join(existing)})."
                    )

            for sid in sid_list:
                # look‑up current scan location (if any)
                cur.execute(
                    "SELECT item_code, location FROM current_scan_location WHERE scan_id = %s",
                    (sid,),
                )
                prev = cur.fetchone()

                if trans_type == "Return":
                    if prev and prev[0] != item_code:
                        raise Exception(
                            f"Scan '{sid}' already registered to {prev[0]} in location {prev[1]}."
                        )
                    # upsert → new home is to_location
                    cur.execute(
                        """
                        INSERT INTO current_scan_location (scan_id, item_code, location, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (scan_id) DO UPDATE
                               SET item_code = EXCLUDED.item_code,
                                   location  = EXCLUDED.location,
                                   updated_at= EXCLUDED.updated_at
                        """,
                        (sid, item_code, loc_field),
                    )
                else:  # Job Issue
                    if not prev:
                        raise Exception(f"Scan '{sid}' not found in inventory.")
                    if prev[0] != item_code:
                        raise Exception(
                            f"Scan '{sid}' belongs to item {prev[0]}, not {item_code}."
                        )
                    if prev[1] != from_location:
                        raise Exception(
                            f"Scan '{sid}' currently located in '{prev[1]}', expected '{from_location}'."
                        )
                    cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                # transaction row
                loc_col = "to_location" if trans_type == "Return" else "from_location"
                cur.execute(
                    f"""
                    INSERT INTO transactions
                          (transaction_type, date, warehouse, {loc_col},
                           job_number, lot_number, item_code, quantity, user_id)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        trans_type,
                        warehouse,
                        loc_field,
                        job,
                        lot,
                        item_code,
                        1,
                        scanned_by or "unknown",
                    ),
                )

                # inventory delta
                delta = 1 if trans_type == "Return" else -1
                cur.execute(
                    """
                    INSERT INTO current_inventory (item_code, location, warehouse, quantity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse)
                        DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity
                    """,
                    (item_code, loc_field, warehouse, delta),
                )

                processed += 1
                progress_callback(int(processed / total_scans * 100))


# ──────────────────────────────────────────────────────────────────────────────
# Streamlit UI – Post‑Kitting Adjustments
# ──────────────────────────────────────────────────────────────────────────────

def run():
    st.title("🛠️ Post‑Kitting Adjustments")

    transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"])
    warehouse        = st.selectbox("Warehouse", WAREHOUSES)
    location         = st.text_input("Location", placeholder="e.g., STAGE-A")
    note             = st.text_input("Transaction Note (optional)")

    adjustments = st.session_state.setdefault("adjustments", [])

    # ── Row entry ─────────────────────────────────────────────────────────
    with st.expander("➕ Add Adjustment Row"):
        job  = st.text_input("Job Number")
        lot  = st.text_input("Lot Number")
        code = st.text_input("Item Code")
        qty  = st.number_input("Quantity", min_value=1, value=1)

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT item_description FROM items_master WHERE item_code = %s",
                        (code.strip(),),
                    )
                    desc_row = cur.fetchone()
                adjustments.append(
                    {
                        "job_number": job.strip(),
                        "lot_number": lot.strip(),
                        "item_code": code.strip(),
                        "quantity": int(qty),
                        "description": desc_row[0] if desc_row else "(Unknown Item)",
                    }
                )
            else:
                st.warning("Please complete all fields before adding.")

    # ── Preview / remove ─────────────────────────────────────────────────
    if adjustments:
        st.markdown("### 📋 Adjustments Preview")
        for i, row in enumerate(adjustments):
            cols = st.columns([3, 3, 3, 2, 3, 1])
            cols[0].markdown(f"**Job:** {row['job_number']}")
            cols[1].markdown(f"**Lot:** {row['lot_number']}")
            cols[2].markdown(f"**Item:** {row['item_code']}")
            cols[3].markdown(f"**Qty:** {row['quantity']}")
            cols[4].markdown(f"**Desc:** {row['description']}")
            if cols[5].button("❌", key=f"rm_{i}"):
                adjustments.pop(i)
                st.rerun()

    # ── Stage 1 submit ───────────────────────────────────────────────────
    if adjustments and st.button("Submit Adjustments"):
        scans_needed  = {}
        job_lot_queue = []

        for row in adjustments:
            job, lot, code, qty = (
                row["job_number"],
                row["lot_number"],
                row["item_code"],
                row["quantity"],
            )

            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT scan_required FROM items_master WHERE item_code = %s",
                    (code,),
                )
                scan_tracked = bool(cur.fetchone() and cur.fetchone())

                qty_store = -abs(qty) if transaction_type == "RETURNB" else qty
                insert_pulltag_line(
                    cur,
                    job,
                    lot,
                    code,
                    qty_store,
                    location,
                    transaction_type,
                    note,
                )

            if scan_tracked:
                scans_needed.setdefault(code, {}).setdefault((job, lot), 0)
                scans_needed[code][(job, lot)] += qty

            job_lot_queue.append((job, lot))

        if not scans_needed:
            st.success(random.choice(IRISH_TOASTS))
            st.balloons()
            st.session_state.adjustments.clear()
            return

        st.session_state.update(
            {
                "scans_needed": scans_needed,
                "job_lot_queue": job_lot_queue,
                "finalize_ready": True,
            }
        )
        st.rerun()

    # ── Stage 2: scan & finalise ─────────────────────────────────────────
    if st.session_state.get("finalize_ready"):
        scans_needed   = st.session_state["scans_needed"]
        job_lot_queue  = st.session_state["job_lot_queue"]

        st.markdown("### 🔍 Scan Required Items")
        with st.form("scan_form"):
            scan_inputs = {}
            for item_code, lots in scans_needed.items():
                for (job, lot), qty in lots.items():
                    st.write(f"**{item_code} — Job {job} / Lot {lot} — Total Scans: {qty}**")
                    prefix = f"{item_code}_{job}_{lot}"
                    for n in range(1, qty + 1):
                        scan_inputs[f"scan_{prefix}_{n}"] = st.text_input(
                            f"Scan {n} for {item_code}",
                            key=f"scan_{prefix}_{n}",
                        )
            submitted = st.form_submit_button("Finalize Adjustments")

        if submitted:
            if not location.strip():
                st.error("Please enter a **Location** before finalising.")
                st.stop()

            bar = st.progress(0)
            with st.spinner("Processing adjustments…"):
                try:
                    finalize_add(
                        scans_needed,
                        scan_inputs,
                        job_lot_queue,
                        from_location=location if transaction_type == "ADD" else None,
                        to_location=location if transaction_type == "RETURNB" else None,
                        scanned_by=st.session_state.get("username", "unknown"),
                        progress_callback=lambda pct: bar.progress(pct),
                        warehouse=warehouse,
                    )
                except Exception as err:
                    st.error(f"⚠️ {err}")
                    st.stop()

            st.success(random.choice(IRISH_TOASTS))
            st.balloons()
            for key in ("adjustments", "scans_needed", "job_lot_queue"):
                st.session_state.pop(key, None)
            st.session_state.finalize_ready = False
