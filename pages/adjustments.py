import streamlit as st
import random
from collections import Counter, defaultdict

from db import get_db_cursor
from config import WAREHOUSES

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
IRISH_TOASTS = [
    "☘️ Sláinte! Transaction submitted successfully!",
    "🍀 Luck o’ the Irish – you did it!",
    "🦃 Cheers, let’s grab a beer – transaction success!",
    "🌈 Pot of gold secured – job well done!",
    "🪙 May your inventory always balance – success!",
]

# ─────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────

def insert_pulltag_line(cur, job, lot, code, qty, loc, tx_type, note):
    """Insert a pull-tag row and resolve warehouse via location."""
    # For RETURNB transactions, record negative quantity
    insert_qty = -qty if tx_type == "RETURNB" else qty

    cur.execute("SELECT warehouse FROM locations WHERE location_code = %s", (loc,))
    row = cur.fetchone()
    if not row:
        raise Exception(f"Unknown location '{loc}'.")
    warehouse = row[0]

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
        (job, lot, insert_qty, tx_type, note, warehouse, code),
    )
    return cur.fetchone()[0]


def finalize_scan_items(scans_needed, scan_inputs, *, from_loc, to_loc, user, note, input_tx, progress_cb=None):
    """Validate scans and update inventory/scan_verifications/transactions for scan-tracked items."""
    if progress_cb is None:
        progress_cb = lambda *_: None

    # Build scan map → detect blanks/dupes
    scan_map = defaultdict(list)
    errors = []
    for code, lots in scans_needed.items():
        for (job, lot), qty in lots.items():
            for i in range(1, qty + 1):
                key = f"scan_{code}_{job}_{lot}_{i}"
                sid = scan_inputs.get(key, "").strip()
                if not sid:
                    errors.append(f"Missing scan {i} for {code} — Job {job} / Lot {lot}.")
                else:
                    scan_map[(code, job, lot)].append(sid)

    duplicates = [s for s, c in Counter([s for v in scan_map.values() for s in v]).items() if c > 1]
    if duplicates:
        errors.append("Duplicate scan IDs: " + ", ".join(duplicates))
    if errors:
        raise Exception("\n".join(errors))

    total = sum(len(v) for v in scan_map.values())
    completed = 0

    with get_db_cursor() as cur:
        for (code, job, lot), sid_list in scan_map.items():
            # internal transaction type: ADD→Job Issue, RETURNB→Return
            internal_tx = "Return" if input_tx == "RETURNB" else "Job Issue"
            loc_val = to_loc if input_tx == "RETURNB" else from_loc

            # determine warehouse
            cur.execute("SELECT warehouse FROM locations WHERE location_code = %s", (loc_val,))
            wh_row = cur.fetchone()
            warehouse = wh_row[0] if wh_row else None

            # enforce single-item location rule
            cur.execute(
                "SELECT multi_item_allowed FROM locations WHERE location_code = %s", (loc_val,)
            )
            flag = cur.fetchone()
            if not (flag and flag[0]):
                cur.execute(
                    "SELECT DISTINCT item_code FROM current_inventory WHERE location = %s AND quantity > 0", (loc_val,)
                )
                present = [r[0] for r in cur.fetchall()]
                if present and any(p != code for p in present):
                    raise Exception(f"Location '{loc_val}' holds other items: {', '.join(present)}.")

            for sid in sid_list:
                # insert into scan_verifications with raw transaction type
                cur.execute(
                    """
                    INSERT INTO scan_verifications
                      (scan_id, item_code, job_number, lot_number,
                       location, scanned_by, transaction_type, warehouse, scan_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (sid, code, job, lot, loc_val, user, input_tx, warehouse),
                )

                # update current_scan_location
                cur.execute(
                    "SELECT item_code, location FROM current_scan_location WHERE scan_id = %s", (sid,)
                )
                prev = cur.fetchone()
                if input_tx == "RETURNB":
                    if prev and prev[0] != code:
                        raise Exception(f"Scan '{sid}' registered to {prev[0]} in {prev[1]}.")
                    cur.execute(
                        """
                        INSERT INTO current_scan_location
                          (scan_id, item_code, location, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (scan_id) DO UPDATE
                          SET item_code = EXCLUDED.item_code,
                              location  = EXCLUDED.location,
                              updated_at= EXCLUDED.updated_at
                        """,
                        (sid, code, loc_val),
                    )
                else:
                    if not prev or prev[0] != code or prev[1] != from_loc:
                        raise Exception(f"Scan '{sid}' not in expected {from_loc}.")
                    cur.execute(
                        "DELETE FROM current_scan_location WHERE scan_id = %s", (sid,)
                    )

                # record transaction
                loc_col = "to_location" if input_tx == "RETURNB" else "from_location"
                cur.execute(
                    f"""
                    INSERT INTO transactions
                      (transaction_type, date, warehouse, {loc_col},
                       job_number, lot_number, item_code, quantity, note, user_id)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (internal_tx, warehouse, loc_val, job, lot, code, 1, note, user),
                )

                # update inventory
                delta = 1 if input_tx == "RETURNB" else -1
                cur.execute(
                    """
                    INSERT INTO current_inventory
                      (item_code, location, warehouse, quantity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                      SET quantity = current_inventory.quantity + EXCLUDED.quantity
                    """,
                    (code, loc_val, warehouse, delta),
                )

                completed += 1
                progress_cb(int(completed / total * 100))


# ─────────────────────────────────────────────
# STREAMLIT PAGE
# ─────────────────────────────────────────────

def run():
    st.title("🛠️ Post-Kitting Adjustments")

    # Ensure user is known
    if "user" not in st.session_state or not st.session_state.user:
        st.error("Please log in to use this page.")
        st.stop()
    user = st.session_state.user

    tx_input = st.selectbox("Transaction Type", ["ADD", "RETURNB"])
    warehouse_sel = st.selectbox("Warehouse", WAREHOUSES)
    location = st.text_input("Location")
    note = st.text_input("Note (optional)")

    adjustments = st.session_state.setdefault("adj_rows", [])

    with st.expander("➕ Add Row"):
        c1, c2, c3, c4 = st.columns([2, 2, 3, 1])
        job = c1.text_input("Job #")
        lot = c2.text_input("Lot #")
        code = c3.text_input("Item Code")
        qty = c4.number_input("Qty", min_value=1, value=1)

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT item_description, scan_required FROM items_master WHERE item_code = %s",
                        (code.strip(),),
                    )
                    data = cur.fetchone()
                adjustments.append({
                    "job": job.strip(),
                    "lot": lot.strip(),
                    "code": code.strip(),
                    "qty": int(qty),
                    "desc": data[0] if data else "(Unknown)",
                    "scan_required": bool(data and data[1]),
                })
                st.rerun()
            else:
                st.warning("Fill all fields before adding.")

    if adjustments:
        st.markdown("### 📋 Pending Adjustments")
        for idx, row in enumerate(adjustments):
            cols = st.columns([2, 2, 3, 1, 1, 1])
            cols[0].write(row["job"])
            cols[1].write(row["lot"])
            cols[2].write(row["code"])
            cols[3].write(str(row["qty"]))
            cols[4].write("🔒" if row["scan_required"] else "—")
            if cols[5].button("❌", key=f"del{idx}"):
                adjustments.pop(idx)
                st.rerun()

    if adjustments and any(r["scan_required"] for r in adjustments):
        st.markdown("### 🔍 Enter Scan IDs")
        for row in adjustments:
            if row["scan_required"]:
                for i in range(1, row["qty"] + 1):
                    st.text_input(
                        f"Scan ID for {row['code']} — Job {row['job']} / Lot {row['lot']} #{i}",
                        key=f"scan_{row['code']}_{row['job']}_{row['lot']}_{i}",
                    )

    if adjustments and st.button("Submit Adjustments"):
        if not location.strip():
            st.error("Location required first.")
            st.stop()

        # Build scan requirements map
        scans_needed = {row["code"]: {(row["job"], row["lot"]): row["qty"]}
                       for row in adjustments if row["scan_required"]}
        scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}

        try:
            if scans_needed:
                finalize_scan_items(
                    scans_needed,
                    scan_inputs,
                    from_loc=location if tx_input == "ADD" else "",
                    to_loc=location if tx_input == "RETURNB" else "",
                    user=user,
                    note=note,
                    input_tx=tx_input,
                )
            # Always create pull-tag rows with proper sign
            for row in adjustments:
                with get_db_cursor() as cur:
                    insert_pulltag_line(
                        cur,
                        row["job"],
                        row["lot"],
                        row["code"],
                        row["qty"],
                        location,
                        tx_input,
                        note,
                    )
            st.success(random.choice(IRISH_TOASTS))
            st.session_state["adj_rows"] = []
        except Exception as e:
            st.error(str(e))
