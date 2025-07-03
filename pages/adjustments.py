import streamlit as st
import random
from collections import Counter, defaultdict
from enum import Enum

from db import get_db_cursor
from config import WAREHOUSES

# ─────────────────────────────────────────────
# ENUMS
# ─────────────────────────────────────────────
class TxType(str, Enum):
    ADD = "ADD"
    RETURNB = "RETURNB"

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
IRISH_TOASTS: list[str] = [
    "☘️ Sláinte! Transaction submitted successfully!",
    "🍀 Luck o’ the Irish – you did it!",
    "🦃 Cheers, let’s grab a beer – transaction success!",
    "🌈 Pot of gold secured – job well done!",
    "🪙 May your inventory always balance – success!",
]

# ─────────────────────────────────────────────
# Core Scan Utilities
# ─────────────────────────────────────────────

def validate_scan(
    scan_id: str,
    code: str,
    from_loc: str,
    to_loc: str,
    input_tx: TxType,
    cur,
) -> tuple[list[str], list[str]]:
    """Return (warnings, errors) for scan validation; errors block, warnings allow bypass for ADD."""
    warnings: list[str] = []
    errors: list[str] = []
    if input_tx is TxType.ADD:
        cur.execute(
            "SELECT item_code, location FROM current_scan_location WHERE scan_id = %s",
            (scan_id,)
        )
        prev = cur.fetchone()
        if not prev:
            cur.execute(
                "SELECT location FROM scan_verifications WHERE scan_id = %s ORDER BY scan_time DESC LIMIT 1",
                (scan_id,),
            )
            last = cur.fetchone()
            if last:
                warnings.append(f"Scan '{scan_id}' is not in any location but was last seen at '{last[0]}'.")
            else:
                warnings.append(f"Scan '{scan_id}' not found in current inventory or scan history.")
        else:
            if prev[0] != code:
                warnings.append(f"Scan '{scan_id}' is registered to {prev[0]}, not {code}.")
            if prev[1] != from_loc:
                warnings.append(f"Scan '{scan_id}' is in {prev[1]}, not in {from_loc}.")
    elif input_tx is TxType.RETURNB:
        cur.execute(
            "SELECT scan_id FROM current_scan_location WHERE scan_id = %s",
            (scan_id,)
        )
        if cur.fetchone():
            errors.append(f"Scan '{scan_id}' already placed in inventory. Cannot RETURNB again.")
    return warnings, errors


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
    if input_tx is TxType.RETURNB:
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
        cur.execute(
            "SELECT location FROM current_scan_location WHERE scan_id = %s",
            (scan_id,)
        )
        loc = cur.fetchone()
        if not loc:
            st.warning(f"Scan ID '{scan_id}' was not removed because it doesn't exist in current inventory.")
        elif loc[0] != loc_val:
            st.warning(f"Scan ID '{scan_id}' is in location '{loc[0]}', not expected '{loc_val}'. Skipping.")
        else:
            cur.execute(
                "DELETE FROM current_scan_location WHERE scan_id = %s",
                (scan_id,)
            )


def insert_transaction(
    tx_type: TxType,
    warehouse: str,
    loc_val: str,
    job: str,
    lot: str,
    code: str,
    note: str,
    user: str,
    cur,
) -> None:
    loc_col = "to_location" if tx_type is TxType.RETURNB else "from_location"
    cur.execute(
        f"""
        INSERT INTO transactions
          (transaction_type, date, warehouse, {loc_col},
           job_number, lot_number, item_code, quantity, note, user_id)
        VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            "Return" if tx_type is TxType.RETURNB else "Job Issue",
            warehouse, loc_val, job, lot, code, 1, note, user
        ),
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
    insert_qty = -qty if tx_type is TxType.RETURNB else qty
    cur.execute(
        "SELECT warehouse FROM locations WHERE location_code = %s",
        (loc,)
    )
    row = cur.fetchone()
    if not row:
        raise Exception(f"Invalid location '{loc}': not found in system.")
    warehouse = row[0]
    if warehouse_sel and warehouse != warehouse_sel:
        raise Exception(
            f"Mismatch: Location '{loc}' is tied to warehouse '{warehouse}', not '{warehouse_sel}'."
        )
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
        (job, lot, insert_qty, tx_order, note, warehouse, code),
    )
    result = cur.fetchone()
    if not result:
        raise Exception(f"Item '{code}' not found in items_master.")
    return result[0]


def finalize_scan_items(adjustments, scans_needed, scan_inputs, *, from_loc, to_loc, user, note, input_tx, warehouse_sel, progress_cb=None):
    if progress_cb is None:
        progress_cb = lambda *_: None

    scan_map: dict = defaultdict(list)
    errors: list[str] = []
    for row_idx, row in enumerate(adjustments):
        code, job, lot, qty = row["code"], row["job"], row["lot"], row["qty"]
        for i in range(1, qty + 1):
            key = f"scan_{code}_{job}_{lot}_{i}_row{row_idx}"
            sid = scan_inputs.get(key, "").strip()
            if not sid:
                errors.append(
                    f"Missing scan {i} for {code} — Job {job} / Lot {lot}."
                )
            else:
                scan_map[(code, job, lot)].append(sid)

    duplicates = [s for s, c in Counter([s for v in scan_map.values() for s in v]).items() if c > 1]
    if duplicates:
        errors.append("Duplicate scan IDs: " + ", ".join(duplicates))
    if errors:
        raise Exception("\n".join(errors))

    total: int = sum(len(v) for v in scan_map.values())
    completed = 0

    with get_db_cursor() as cur:
        # Validate location exists and matches warehouse
        loc_val = from_loc or to_loc
        cur.execute("SELECT warehouse FROM locations WHERE location_code = %s", (loc_val,))
        loc_row = cur.fetchone()
        if not loc_row:
            # All scans invalid if location is unknown
            return [{
                "scan_id": "",
                "item_code": "",
                "job": "",
                "lot": "",
                "status": "❌ Invalid",
                "reason": f"Location '{loc_val}' not found.",
                "bypassable": False
            }]
        if loc_row[0] != warehouse_sel:
            return [{
                "scan_id": "",
                "item_code": "",
                "job": "",
                "lot": "",
                "status": "❌ Invalid",
                "reason": f"Location '{loc_val}' is tied to warehouse '{loc_row[0]}', not '{warehouse_sel}'.",
                "bypassable": False
            }]
        # proceed with individual scan validity checks
        try:
            location_configs: dict = {}

            for (code, job, lot), sid_list in scan_map.items():
                loc_val = to_loc if input_tx is TxType.RETURNB else from_loc
                if loc_val not in location_configs:
                    cur.execute(
                        "SELECT warehouse, multi_item_allowed FROM locations WHERE location_code = %s",
                        (loc_val,),
                    )
                    row = cur.fetchone()
                    if not row:
                        raise Exception(f"Location '{loc_val}' not found.")
                    warehouse, multi_item_allowed = row
                    if warehouse != warehouse_sel:
                        raise Exception(
                            f"Mismatch: Location '{loc_val}' is tied to warehouse '{warehouse}', not '{warehouse_sel}'."
                        )
                    location_configs[loc_val] = (warehouse, multi_item_allowed)

                warehouse, multi_item_allowed = location_configs[loc_val]
                if not multi_item_allowed:
                    cur.execute(
                        "SELECT DISTINCT item_code FROM current_inventory WHERE location = %s AND quantity > 0",
                        (loc_val,),
                    )
                    present = [r[0] for r in cur.fetchall()]
                    if present and any(p != code for p in present):
                        raise Exception(
                            f"Location '{loc_val}' holds other items: {', '.join(present)}."
                        )

                for sid in sid_list:
                    cur.execute(
                        "SELECT transaction_type, location, scan_time FROM scan_verifications WHERE scan_id = %s ORDER BY scan_time ASC",
                        (sid,),
                    )
                    history = cur.fetchall()
                    if history:
                        tx_count = Counter([h[0] for h in history])
                        if any(tx_count[typ] > 1 for typ in ["ADD", "RETURN", "RETURNB", "Job Issue"]):
                            st.warning(
                                f"⚠️ Scan ID '{sid}' has a complex history:\n" +
                                "\n".join([f"{r[0]} at {r[1]} on {r[2]}" for r in history])
                            )

                    if input_tx is TxType.RETURNB:
                        _, err = validate_scan(sid, code, from_loc, to_loc, input_tx, cur)
                        if err:
                            raise Exception(err[0])

                    insert_scan_verification(sid, code, job, lot, loc_val, user, input_tx, warehouse, cur)
                    update_scan_location(sid, code, loc_val, input_tx, cur)
                    insert_transaction(input_tx, warehouse, loc_val, job, lot, code, note, user, cur)
                    adjust_inventory(code, loc_val, warehouse, 1 if input_tx is TxType.RETURNB else -1, cur)

                    completed += 1
                    progress_cb(int(completed / total * 100))

        except Exception as exc:
            st.error(f"Transaction failed: {exc}")
            with st.expander("Debug Info", expanded=True):
                st.code(repr(scan_map), language="python")
            raise


def preview_scan_validity(adjustments, scans_needed, scan_inputs, from_loc, to_loc, input_tx, warehouse_sel):
    results: list[dict] = []
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
                        "reason": f"Scan #{i} is missing",
                        "bypassable": False
                    })
                    continue

                # Early warning for ADD when no current inventory and no history
                if input_tx is TxType.ADD:
                    cur.execute(
                        "SELECT item_code, location FROM current_scan_location WHERE scan_id = %s",
                        (sid,)
                    )
                    prev = cur.fetchone()
                    if not prev:
                        # check last known location
                        cur.execute(
                            "SELECT location FROM scan_verifications WHERE scan_id = %s ORDER BY scan_time DESC LIMIT 1",
                            (sid,)
                        )
                        last = cur.fetchone()
                        status = "⚠️ Warning"
                        if last:
                            reason = f"Scan '{sid}' is new; last seen at '{last[0]}'"
                        else:
                            reason = f"Scan '{sid}' not found in inventory or history."
                        results.append({
                            "scan_id": sid,
                            "item_code": code,
                            "job": job,
                            "lot": lot,
                            "status": status,
                            "reason": reason,
                            "bypassable": True
                        })
                        continue

                # fetch transaction history
                cur.execute(
                    "SELECT transaction_type FROM scan_verifications WHERE scan_id = %s ORDER BY scan_time ASC",
                    (sid,)
                )
                history = cur.fetchall()
                # detect complex history
                warning_needed = (
                    history and any(
                        Counter(h[0] for h in history)[typ] > 1
                        for typ in ("ADD", "RETURNB", "Job Issue")
                    )
                )
                if warning_needed:
                    status = "⚠️ Warning"
                    reason = f"Complex history ({len(history)} events)"
                    bypassable = True
                else:
                    warnings, errors = validate_scan(sid, code, from_loc, to_loc, input_tx, cur)
                    if errors:
                        status = "❌ Invalid"
                        reason = "; ".join(errors)
                        bypassable = False
                    elif warnings:
                        status = "⚠️ Warning"
                        reason = "; ".join(warnings)
                        bypassable = True
                    else:
                        status = "✅ Valid"
                        reason = ""
                        bypassable = False

                results.append({
                    "scan_id": sid,
                    "item_code": code,
                    "job": job,
                    "lot": lot,
                    "status": status,
                    "reason": reason,
                    "bypassable": bypassable
                })

    # Detect duplicate scan IDs across inputs
    all_sids = [r["scan_id"] for r in results if r["status"] != "❌ Missing"]
    dup_counts = Counter(all_sids)
    duplicates = {sid for sid, cnt in dup_counts.items() if cnt > 1}
    if duplicates:
        for r in results:
            if r["scan_id"] in duplicates:
                r["status"] = "❌ Invalid"
                r["reason"] = "Duplicate scan ID in input."
                r["bypassable"] = False

    return results


def show_scan_preview(adjustments, scan_inputs, location, tx_input, warehouse_sel):
    preview = preview_scan_validity(
        adjustments,
        scans_needed,
        scan_inputs,
        from_loc=location if tx_input == "ADD" else "",
        to_loc=location if tx_input == "RETURNB" else "",
        input_tx=tx_input,
        warehouse_sel=warehouse_sel
    )

    st.markdown("### 🧾 Scan Validation Preview")
    for entry in preview:
        st.write(f"{entry['scan_id']} — {entry['status']} ({entry['reason']})")
    return preview


def run():
    st.title("🛠️ Post-Kitting Adjustments")

    if not st.session_state.get("user"):
        st.error("Please log in to use this page.")
        st.stop()
    user = st.session_state.user

    tx_input = st.selectbox("Transaction Type", ["ADD", "RETURNB"])  
    warehouse_sel = st.selectbox("Warehouse", WAREHOUSES)
    location = st.text_input("Location")
    note = st.text_input("Note (optional)")

    adjustments = st.session_state.setdefault("adj_rows", [])

    with st.expander("➕ Add Row"):
        c1, c2, c3, c4 = st.columns([2,2,3,1])
        job = c1.text_input("Job #")
        lot = c2.text_input("Lot #")
        code = c3.text_input("Item Code")
        qty = c4.number_input("Qty", min_value=1, value=1)
        if st.button("Add to List"):
            if job and lot and code and qty>0:
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT item_description, scan_required FROM items_master WHERE item_code=%s",
                        (code.strip(),)
                    )
                    data=cur.fetchone()
                adjustments.append({
                    "job":job.strip(),
                    "lot":lot.strip(),
                    "code":code.strip(),
                    "qty":qty,
                    "scan_required":bool(data and data[1])
                })
                st.rerun()
            else:
                st.warning("Fill all fields before adding.")

    if adjustments:
        st.markdown("### 📋 Pending Adjustments")
        for idx,row in enumerate(adjustments):
            cols=st.columns([2,2,3,1,1,1])
            cols[0].write(row['job'])
            cols[1].write(row['lot'])
            cols[2].write(row['code'])
            cols[3].write(str(row['qty']))
            cols[4].write("🔒" if row['scan_required'] else "—")
            if cols[5].button("❌",key=f"del{idx}"):
                adjustments.pop(idx)
                st.rerun()

    if any(r['scan_required'] for r in adjustments):
        st.markdown("### 🔍 Enter Scan IDs")
        for idx,row in enumerate(adjustments):
            if row['scan_required']:
                for i in range(1,row['qty']+1):
                    st.text_input(
                        f"Scan ID for {row['code']} — Job {row['job']} / Lot {row['lot']} #{i}",
                        key=f"scan_{row['code']}_{row['job']}_{row['lot']}_{i}_row{idx}"
                    )

    if st.button("🔍 Preview Scan Validity"):
        if not location:
            st.error("Location required first.")
        else:
            scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith('scan_')}
            scans_needed = {
                row["code"]: {(row["job"], row["lot"]): row["qty"]}
                for row in adjustments
                if row["scan_required"]
            }
            st.session_state["scan_preview"] = show_scan_preview(
                adjustments,
                scans_needed,
                scan_inputs,
                location if tx_input == TxType.ADD.value else "",
                location if tx_input == TxType.RETURNB.value else "",
                tx_input,
                warehouse_sel,
            )

    preview = st.session_state.get('scan_preview', [])

    if st.button("Submit Adjustments"):
        if not location:
            st.error("Location required first.")
            st.stop()
        errors = [e for e in preview if e['status'].startswith('❌')]
        warnings = [e for e in preview if e['status'].startswith('⚠️')]
        if errors:
            st.error("Cannot submit due to errors:")
            for e in errors:
                st.write(f"- {e['scan_id']}: {e['reason']}")
            st.stop()
        if warnings and tx_input == 'ADD' and not st.session_state.get('bypass_confirmed'):
            st.warning("There are warnings:")
            for e in warnings:
                st.write(f"- {e['scan_id']}: {e['reason']}")
            if st.button("Proceed with warnings"):
                st.session_state['bypass_confirmed'] = True
                st.experimental_rerun()
            st.stop()
        try:
            scans_needed = {row['code']:{(row['job'],row['lot']):row['qty']} for row in adjustments if row['scan_required']}
            scan_inputs = {k:v for k,v in st.session_state.items() if k.startswith('scan_')}
            if scans_needed:
                finalize_scan_items(
                    adjustments, scans_needed, scan_inputs,
                    from_loc=location if tx_input == 'ADD' else '',
                    to_loc=location if tx_input == 'RETURNB' else '',
                    user=user, note=note,
                    input_tx=tx_input, warehouse_sel=warehouse_sel
                )
            with get_db_cursor() as cur:
                for row in adjustments:
                    insert_pulltag_line(
                        cur, row['job'], row['lot'], row['code'], row['qty'],
                        location, tx_input, note, warehouse_sel=warehouse_sel
                    )
            st.success(random.choice(IRISH_TOASTS))
            st.session_state['adj_rows'] = []
            st.session_state.pop('scan_preview', None)
            st.session_state.pop('bypass_confirmed', None)
        except Exception as exc:
            st.error(f"Submission failed: {exc}")
            st.stop()
