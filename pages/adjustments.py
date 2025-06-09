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
# Modular Scan Finalization Functions
# ─────────────────────────────────────────────

def validate_scan(scan_id, code, from_loc, to_loc, input_tx, cur):
    if input_tx == "ADD":
        cur.execute("SELECT item_code, location FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        prev = cur.fetchone()
        if not prev:
            cur.execute("SELECT location FROM scan_verifications WHERE scan_id = %s ORDER BY scan_time DESC LIMIT 1", (scan_id,))
            last = cur.fetchone()
            if last:
                raise Exception(f"Scan '{scan_id}' is not in any location but was last seen at '{last[0]}'.")
            else:
                raise Exception(f"Scan '{scan_id}' not found in current inventory or scan history.")
        if prev[0] != code or prev[1] != from_loc:
            raise Exception(f"Scan '{scan_id}' is registered to {prev[0]} in {prev[1]}, not in {from_loc}.")

    elif input_tx == "RETURNB":
        cur.execute("SELECT scan_id FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        if cur.fetchone():
            raise Exception(f"Scan '{scan_id}' already placed in inventory. Cannot RETURNB again.")


def insert_scan_verification(scan_id, code, job, lot, loc_val, user, input_tx, warehouse, cur):
    cur.execute(
        """
        INSERT INTO scan_verifications
          (scan_id, item_code, job_number, lot_number,
           location, scanned_by, transaction_type, warehouse, scan_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        (scan_id, code, job, lot, loc_val, user, input_tx, warehouse),
    )


def update_scan_location(scan_id, code, loc_val, input_tx, cur):
    if input_tx == "RETURNB":
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
            (scan_id, code, loc_val),
        )
    else:
        cur.execute("SELECT location FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        loc = cur.fetchone()
        if not loc:
            st.warning(f"Scan ID '{scan_id}' was not removed because it doesn't exist in current inventory.")
        elif loc[0] != loc_val:
            st.warning(f"Scan ID '{scan_id}' is in location '{loc[0]}', not expected '{loc_val}'. Skipping.")
        else:
            cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (scan_id,))


def insert_transaction(tx_type, warehouse, loc_val, job, lot, code, note, user, cur):
    loc_col = "to_location" if tx_type == "RETURNB" else "from_location"
    cur.execute(
        f"""
        INSERT INTO transactions
          (transaction_type, date, warehouse, {loc_col},
           job_number, lot_number, item_code, quantity, note, user_id)
        VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        ("Return" if tx_type == "RETURNB" else "Job Issue", warehouse, loc_val, job, lot, code, 1, note, user),
    )


def adjust_inventory(code, loc_val, warehouse, delta, cur):
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


def insert_pulltag_line(cur, job, lot, code, qty, loc, tx_type, note, warehouse_sel=None):
    insert_qty = -qty if tx_type == "RETURNB" else qty

    cur.execute("SELECT warehouse FROM locations WHERE location_code = %s", (loc,))
    row = cur.fetchone()
    if not row:
        raise Exception(f"Invalid location '{loc}': not found in system.")
    warehouse = row[0]

    if warehouse_sel and warehouse != warehouse_sel:
        raise Exception(f"Mismatch: Location '{loc}' is tied to warehouse '{warehouse}', not '{warehouse_sel}'.")

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
    result = cur.fetchone()
    if not result:
        raise Exception(f"Item '{code}' not found in items_master.")
    return result[0]


# ─────────────────────────────────────────────
# FINALIZE SCANS – DELEGATES TO MODULAR HELPERS
# ─────────────────────────────────────────────

def finalize_scan_items(adjustments, scans_needed, scan_inputs, *, from_loc, to_loc, user, note, input_tx, warehouse_sel, progress_cb=None):
    if progress_cb is None:
        progress_cb = lambda *_: None

    from collections import defaultdict, Counter

    scan_map = defaultdict(list)
    errors = []
    for row_idx, row in enumerate(adjustments):
        code, job, lot, qty = row["code"], row["job"], row["lot"], row["qty"]
        for i in range(1, qty + 1):
            key = f"scan_{code}_{job}_{lot}_{i}_row{row_idx}"
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
        try:
            location_configs = {}

            for (code, job, lot), sid_list in scan_map.items():
                loc_val = to_loc if input_tx == "RETURNB" else from_loc

                if loc_val not in location_configs:
                    cur.execute("SELECT warehouse, multi_item_allowed FROM locations WHERE location_code = %s", (loc_val,))
                    row = cur.fetchone()
                    if not row:
                        raise Exception(f"Location '{loc_val}' not found.")
                    warehouse, multi_item_allowed = row

                    if warehouse != warehouse_sel:
                        raise Exception(f"Mismatch: Location '{loc_val}' is tied to warehouse '{warehouse}', not '{warehouse_sel}'.")

                    location_configs[loc_val] = (warehouse, multi_item_allowed)

                warehouse, multi_item_allowed = location_configs[loc_val]

                if not multi_item_allowed:
                    cur.execute("SELECT DISTINCT item_code FROM current_inventory WHERE location = %s AND quantity > 0", (loc_val,))
                    present = [r[0] for r in cur.fetchall()]
                    if present and any(p != code for p in present):
                        raise Exception(f"Location '{loc_val}' holds other items: {', '.join(present)}.")

                for sid in sid_list:
                    cur.execute("SELECT transaction_type, location, scan_time FROM scan_verifications WHERE scan_id = %s ORDER BY scan_time ASC", (sid,))
                    history = cur.fetchall()
                    if history:
                        tx_count = Counter([h[0] for h in history])
                        if any(tx_count[typ] > 1 for typ in ["ADD", "RETURN", "RETURNB", "Job Issue"]):
                            st.warning(f"⚠️ Scan ID '{sid}' has a complex history:\n" + "\n".join([f"{r[0]} at {r[1]} on {r[2]}" for r in history]))

                    validate_scan(sid, code, from_loc, to_loc, input_tx, cur)
                    insert_scan_verification(sid, code, job, lot, loc_val, user, input_tx, warehouse, cur)
                    update_scan_location(sid, code, loc_val, input_tx, cur)
                    insert_transaction(input_tx, warehouse, loc_val, job, lot, code, note, user, cur)
                    adjust_inventory(code, loc_val, warehouse, 1 if input_tx == "RETURNB" else -1, cur)

                    completed += 1
                    progress_cb(int(completed / total * 100))

        except Exception as e:
            st.error(f"Transaction failed: {e}")
            with st.expander("Debug Info", expanded=True):
                st.code(str(scan_map), language="python")
            raise
# ─────────────────────────────────────────────
# NEW: Modular Scan Finalization Functions
# ─────────────────────────────────────────────

preview = []  # used to hold preview results across runs

def preview_scan_validity(adjustments, scans_needed, scan_inputs, from_loc, to_loc, input_tx):
    from collections import defaultdict
    results = []

    with get_db_cursor() as cur:
            for row_idx, row in enumerate(adjustments):
                code, job, lot, qty = row["code"], row["job"], row["lot"], row["qty"]
                for i in range(1, qty + 1):
                    key = f"scan_{code}_{job}_{lot}_{i}_row{row_idx}"
                    sid = scan_inputs.get(key, "").strip()
                    if not sid:
                        results.append({
                            "scan_id": key,
                            "item_code": code,
                            "job": job,
                            "lot": lot,
                            "status": "❌ Missing",
                            "reason": f"Scan #{i} is missing"
                        })
                        continue
                    try:
                        validate_scan(sid, code, from_loc, to_loc, input_tx, cur)
                        results.append({
                            "scan_id": sid,
                            "item_code": code,
                            "job": job,
                            "lot": lot,
                            "status": "✅ Valid",
                            "reason": ""
                        })
                    except Exception as e:
                        results.append({
                            "scan_id": sid,
                            "item_code": code,
                            "job": job,
                            "lot": lot,
                            "status": "❌ Invalid",
                            "reason": str(e)
                        })
    return results

def show_scan_preview(adjustments, scan_inputs, location, tx_input):
    scans_needed = {row["code"]: {(row["job"], row["lot"]): row["qty"]}
                    for row in adjustments if row["scan_required"]}
    preview = preview_scan_validity(
        adjustments,
        scans_needed,
        scan_inputs,
        from_loc=location if tx_input == "ADD" else "",
        to_loc=location if tx_input == "RETURNB" else "",
        input_tx=tx_input,
    )
    st.markdown("### 🧾 Scan Validation Preview")
    for entry in preview:
        st.write(f"{entry['scan_id']} — {entry['status']} ({entry['reason']})")
    return preview


def all_scans_valid(preview):
    return all(entry["status"] == "✅ Valid" for entry in preview)

#
# ─────────────────────────────────────────────
# STREAMLIT PAGE
# ─────────────────────────────────────────────

def run():
    st.title("🛠️ Post-Kitting Adjustments")

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

        with st.expander("⚠️ Reset Options", expanded=False):
            confirm = st.checkbox("Yes, I want to reset all adjustments.")
            if st.button("🔄 Reset All Adjustments"):
                if confirm:
                    st.session_state["adj_rows"] = []
                    st.session_state["scan_preview"] = []
                    st.rerun()
                else:
                    st.warning("Please check the box to confirm reset.")

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
        for idx, row in enumerate(adjustments):
            if row["scan_required"]:
                for i in range(1, row["qty"] + 1):
                    st.text_input(
                        f"Scan ID for {row['code']} — Job {row['job']} / Lot {row['lot']} #{i}",
                        key=f"scan_{row['code']}_{row['job']}_{row['lot']}_{i}_row{idx}",
                    )
                    
        if st.button("🔍 Preview Scan Validity"):
            if not location.strip():
                st.error("Location required first.")
            else:
                scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}
                st.session_state["scan_preview"] = show_scan_preview(adjustments, scan_inputs, location, tx_input)

    valid_scans_only = all_scans_valid(st.session_state.get("scan_preview", []))

    if adjustments and st.button("Submit Adjustments"):
        if not location.strip():
            st.error("Location required first.")
            st.stop()

        if not valid_scans_only:
            st.warning("Some scans are invalid or missing. Please preview and resolve issues before submitting.")
            st.stop()

        scans_needed = {row["code"]: {(row["job"], row["lot"]): row["qty"]}
                        for row in adjustments if row["scan_required"]}
        scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}

        try:
            if scans_needed:
                finalize_scan_items(
                    adjustments,
                    scans_needed,
                    scan_inputs,
                    from_loc=location if tx_input == "ADD" else "",
                    to_loc=location if tx_input == "RETURNB" else "",
                    user=user,
                    note=note,
                    input_tx=tx_input,
                    warehouse_sel=warehouse_sel,
                )
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
                        warehouse_sel=warehouse_sel,
                    )
            st.success(random.choice(IRISH_TOASTS))
            st.session_state["adj_rows"] = []
            st.session_state["scan_preview"] = []
        except Exception as e:
            st.error(str(e))
