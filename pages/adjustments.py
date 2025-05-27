import streamlit as st
import random
from collections import Counter, defaultdict

from db import get_db_cursor
from config import WAREHOUSES

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────
IRISH_TOASTS = [
    "☘️ Sláinte! Transaction submitted successfully!",
    "🍀 Luck o’ the Irish – you did it!",
    "🦃 Cheers, let’s grab a beer – transaction success!",
    "🌈 Pot of gold secured – job well done!",
    "🪙 May your inventory always balance – success!",
]

SKIP_SCAN_CHECK_LOCATIONS: tuple[str, ...] = ()  # no exceptions

# ──────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def insert_pulltag_line(cur, job, lot, code, qty, loc, tx_type, note):
    """Insert pulltag row and derive warehouse from location."""
    cur.execute("SELECT warehouse FROM locations WHERE location_code = %s", (loc,))
    wh = cur.fetchone()
    if not wh:
        raise Exception(f"Unknown location '{loc}' – cannot map to warehouse.")
    warehouse = wh[0]

    cur.execute(
        """
        INSERT INTO pulltags
              (job_number, lot_number, item_code, quantity,
               description, cost_code, uom, status,
               transaction_type, note, warehouse)
        SELECT %s, %s, item_code, %s,
               item_description, cost_code, uom,
               'pending', %s, %s, %s
        FROM items_master
        WHERE item_code = %s
        RETURNING id
        """,
        (job, lot, qty, tx_type, note, warehouse, code),
    )
    return cur.fetchone()[0]


def finalize_scan_items(scans_needed, scan_inputs, *, from_loc, to_loc, user, wh, progress_cb):
    """Validate and apply all scan‑tracked movements."""
    if progress_cb is None:
        progress_cb = lambda *_: None

    errors = []
    lookup: defaultdict[tuple[str, str, str], list[str]] = defaultdict(list)
    for code, lots in scans_needed.items():
        for (job, lot), qty in lots.items():
            for i in range(1, qty + 1):
                sid = scan_inputs.get(f"scan_{code}_{job}_{lot}_{i}", "").strip()
                if not sid:
                    errors.append(f"Missing scan {i} for {code} — Job {job} / Lot {lot}.")
                else:
                    lookup[(code, job, lot)].append(sid)

    dups = [s for s, c in Counter([s for lst in lookup.values() for s in lst]).items() if c > 1]
    if dups:
        errors.append(f"Duplicate scan IDs: {', '.join(dups)}")

    if errors:
        raise Exception("\n".join(errors))

    total = sum(len(v) for v in lookup.values())
    done  = 0

    with get_db_cursor() as cur:
        for (code, job, lot), sids in lookup.items():
            tx_type = "Return" if to_loc and not from_loc else "Job Issue"
            loc_val = to_loc if tx_type == "Return" else from_loc

            # single‑item guard
            cur.execute("SELECT multi_item_allowed FROM locations WHERE location_code = %s", (loc_val,))
            flag = cur.fetchone()
            multi_ok = bool(flag and flag[0])
            if not multi_ok:
                cur.execute("SELECT DISTINCT item_code FROM current_inventory WHERE location = %s AND quantity > 0", (loc_val,))
                present = [r[0] for r in cur.fetchall()]
                if present and any(p != code for p in present):
                    raise Exception(f"Location '{loc_val}' holds {', '.join(present)} already.")

            for sid in sids:
                # validate / mutate current_scan_location
                cur.execute("SELECT item_code, location FROM current_scan_location WHERE scan_id = %s", (sid,))
                prev = cur.fetchone()

                if tx_type == "Return":
                    if prev and prev[0] != code:
                        raise Exception(f"Scan '{sid}' linked to {prev[0]} in {prev[1]}.")
                    cur.execute(
                        """
                        INSERT INTO current_scan_location (scan_id, item_code, location, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (scan_id) DO UPDATE
                               SET item_code = EXCLUDED.item_code,
                                   location  = EXCLUDED.location,
                                   updated_at= EXCLUDED.updated_at
                        """,
                        (sid, code, loc_val),
                    )
                else:
                    if not prev:
                        raise Exception(f"Scan '{sid}' not found in inventory.")
                    if prev[0] != code or prev[1] != from_loc:
                        raise Exception(f"Scan '{sid}' mismatch (expected {code} in {from_loc}).")
                    cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                # transaction
                loc_col = "to_location" if tx_type == "Return" else "from_location"
                cur.execute(
                    f"INSERT INTO transactions (transaction_type, date, warehouse, {loc_col}, job_number, lot_number, item_code, quantity, user_id) VALUES (%s, NOW(), %s, %s, %s, %s, %s, 1, %s)",
                    (tx_type, wh, loc_val, job, lot, code, user),
                )

                # inventory delta
                delta = 1 if tx_type == "Return" else -1
                cur.execute(
                    """
                    INSERT INTO current_inventory (item_code, location, warehouse, quantity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse)
                        DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity
                    """,
                    (code, loc_val, wh, delta),
                )

                done += 1
                progress_cb(int(done / total * 100))

# ──────────────────────────────────────────────────────────────────────────────
# STREAMLIT PAGE
# ──────────────────────────────────────────────────────────────────────────────

def run():
    st.title("🛠️ Post‑Kitting Adjustments")

    tx_type   = st.selectbox("Transaction Type", ["ADD", "RETURNB"], key="tx")
    warehouse = st.selectbox("Warehouse", WAREHOUSES, key="wh")
    location  = st.text_input("Location", key="loc")
    note      = st.text_input("Note (optional)", key="note")

    adjustments = st.session_state.setdefault("adj_list", [])

    # ── Entry form ───────────────────────────────────────────────────────
    with st.expander("➕ Add Row"):
        col_j, col_l, col_c, col_q = st.columns([2, 2, 3, 1])
        job  = col_j.text_input("Job #", key="job")
        lot  = col_l.text_input("Lot #", key="lot")
        code = col_c.text_input("Item Code", key="code")
        qty  = col_q.number_input("Qty", min_value=1, value=1, key="qty")

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                with get_db_cursor() as cur:
                    cur.execute("SELECT item_description, scan_required FROM items_master WHERE item_code=%s", (code.strip(),))
                    data = cur.fetchone()
                desc = data[0] if data else "(Unknown)"
                scan_req = bool(data and data[1])

                adjustments.append({
                    "job": job.strip(),
                    "lot": lot.strip(),
                    "code": code.strip(),
                    "qty": int(qty),
                    "desc": desc,
                    "scan_required": scan_req,
                })
                st.experimental_rerun()
            else:
                st.warning("Fill all fields first.")

    # ── Preview list ────────────────────────────────────────────────────
    if adjustments:
        st.markdown("### 📋 Pending Rows")
        for i, row in enumerate(adjustments):
            c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 3, 1, 2, 1])
            c1.write(row["job"])
            c2.write(row["lot"])
            c3.write(row["code"])
            c4.write(row["qty"])
            c5.write("🔒" if row["scan_required"] else "—")
            if c6.button("❌", key=f"rm{i}"):
                adjustments.pop(i)
                st.experimental_rerun()

    # ── Stage 1 submit ─────────────────────────────────────────────────
    if adjustments and st.button("Submit Adjustments"):
        scans_needed = {}
        for row in adjustments:
            qty_store = -abs(row["
