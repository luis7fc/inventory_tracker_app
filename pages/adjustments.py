# adjustments.py  –  single‑file Streamlit inventory app
# ------------------------------------------------------
#   • Request Pulltags  (writes status='pending')
#   • Kitting – Add‑On / Return / Transfer  (load, scan, validate, commit)
#   • Dashboard – Pending & Fulfilled
# ------------------------------------------------------

import math
import psycopg2
import streamlit as st
import pandas as pd
from contextlib import contextmanager
from collections import defaultdict, Counter
from enum import Enum
from config import WAREHOUSES
# ───────────────────────────────────────────────────────────────
#  0.  ENUMS
# ───────────────────────────────────────────────────────────────
class TxType(str, Enum):
    ADD = "ADD"
    RETURNB = "RETURNB"
    TRANSFER = "TRANSFER"

# ───────────────────────────────────────────────────────────────
#  1.  DB helper
# ───────────────────────────────────────────────────────────────
@contextmanager
def get_db_cursor():
    """Yields a fresh cursor and commits+closes when done."""
    conn = psycopg2.connect(
        host=st.secrets["DB_HOST"],
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        port=st.secrets.get("DB_PORT", 5432)
    )
    try:
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ───────────────────────────────────────────────────────────────
#  2.  Helper: collect_scan_map  (works for all TxTypes)
# ───────────────────────────────────────────────────────────────
def collect_scan_map(adjustments, scan_inputs, input_tx: TxType) -> dict:
    """
    Builds a scan_map:
    - For ADD or RETURNB: dict[(code, job, lot)] = [scan_id1, scan_id2, ...]
    - For TRANSFER: dict[(code, job, lot)] = [(scan_id1, pallet_qty), ...]
    Raises on missing or duplicate scans.
    """
    scan_map = defaultdict(list)
    errors = []

    for row_idx, row in enumerate(adjustments):
        if not row.get("scan_required"):
            continue

        code = row["code"]
        job = row["job"]
        lot = row["lot"]
        qty = row["qty"]
        pallet_qty = max(row.get("pallet_qty") or 1, 1)  # Always ≥ 1

        scan_count_needed = qty if input_tx != TxType.TRANSFER else math.ceil(qty / pallet_qty)

        for i in range(1, scan_count_needed + 1):
            key = f"scan_{code}_{job}_{lot}_{i}_row{row_idx}"
            sid = scan_inputs.get(key, "").strip()
            if not sid:
                errors.append(f"Missing scan #{i} for {code} — Job {job} / Lot {lot}")
            else:
                if input_tx == TxType.TRANSFER:
                    scan_map[(code, job, lot)].append((sid, pallet_qty))
                else:
                    scan_map[(code, job, lot)].append(sid)

    # Duplicate scan detection (flattened across all entries)
    all_sids = []
    for v in scan_map.values():
        if input_tx == TxType.TRANSFER:
            all_sids.extend([sid for sid, _ in v])
        else:
            all_sids.extend(v)

    dupes = [sid for sid, count in Counter(all_sids).items() if count > 1]
    if dupes:
        errors.append("Duplicate scan IDs: " + ", ".join(dupes))

    if errors:
        raise Exception("Scan Input Errors:\n" + "\n".join(errors))

    return scan_map
# ───────────────────────────────────────────────────────────────
#  3.  Helper: validate_scan_items (row‑level location aware)
# ───────────────────────────────────────────────────────────────
#Added transfer tx_type validation logic
def validate_scan_items(scan_map, input_tx: TxType, warehouse_sel: str):
    log = []
    adj_rows = st.session_state.get("adj_rows", [])

    with get_db_cursor() as cur:
        for (code, job, lot), scan_entries in scan_map.items():
            matching_row = next((r for r in adj_rows if r["code"] == code and r["job"] == job and r["lot"] == lot), None)
            if not matching_row:
                msg = f"❌ Internal error: adjustment row not found for {code} — Job {job} / Lot {lot}"
                log.append({"level": "error", "message": msg})
                st.session_state["scan_validation_log"] = log
                raise Exception(msg)

            row_loc = matching_row.get("location", "").strip()
            if not row_loc:
                msg = f"❌ Location missing for item {code} — Job {job} / Lot {lot}"
                log.append({"level": "error", "message": msg})
                st.session_state["scan_validation_log"] = log
                raise Exception(msg)

            cur.execute("SELECT warehouse FROM locations WHERE location_code = %s", (row_loc,))
            loc_row = cur.fetchone()
            if not loc_row:
                msg = f"❌ Location '{row_loc}' not found for item {code} (Job {job}, Lot {lot})"
                log.append({"level": "error", "message": msg})
                st.session_state["scan_validation_log"] = log
                raise Exception(msg)
            if loc_row[0] != warehouse_sel:
                msg = f"❌ Location '{row_loc}' belongs to warehouse '{loc_row[0]}', not '{warehouse_sel}' (Item {code})"
                log.append({"level": "error", "message": msg})
                st.session_state["scan_validation_log"] = log
                raise Exception(msg)

            for entry in scan_entries:
                if input_tx == TxType.TRANSFER:
                    if not isinstance(entry, tuple) or len(entry) != 2:
                        msg = f"❌ TRANSFER expects (scan_id, pallet_qty). Got: {entry}"
                        log.append({"level": "error", "message": msg})
                        st.session_state["scan_validation_log"] = log
                        raise Exception(msg)
                    sid, pallet_qty = entry
                    if not isinstance(pallet_qty, int) or pallet_qty <= 0:
                        msg = f"❌ Invalid pallet_qty ({pallet_qty}) for scan '{sid}'"
                        log.append({"level": "error", "message": msg})
                        st.session_state["scan_validation_log"] = log
                        raise Exception(msg)
                else:
                    sid = entry

                if input_tx == TxType.RETURNB:
                    cur.execute("SELECT scan_id FROM current_scan_location WHERE scan_id = %s", (sid,))
                    if cur.fetchone():
                        msg = f"❌ Scan '{sid}' already in inventory — cannot RETURNB again"
                        log.append({"level": "error", "message": msg})
                        st.session_state["scan_validation_log"] = log
                        raise Exception(msg)

                elif input_tx in [TxType.ADD, TxType.TRANSFER]:
                    cur.execute("SELECT item_code, location FROM current_scan_location WHERE scan_id = %s", (sid,))
                    prev = cur.fetchone()
                    if not prev:
                        cur.execute(
                            "SELECT location FROM scan_verifications WHERE scan_id = %s ORDER BY scan_time DESC LIMIT 1",
                            (sid,)
                        )
                        last = cur.fetchone()
                        if last:
                            msg = f"⚠️ Scan '{sid}' not in inventory but was last seen at '{last[0]}'"
                        else:
                            msg = f"⚠️ Scan '{sid}' not found in inventory or scan history"
                        log.append({"level": "warn", "message": msg})
                    else:
                        prev_code, prev_loc = prev
                        if prev_code != code:
                            msg = f"⚠️ Scan '{sid}' registered to item '{prev_code}', expected '{code}'"
                            log.append({"level": "warn", "message": msg})
                        if prev_loc != row_loc:
                            msg = f"⚠️ Scan '{sid}' is located at '{prev_loc}', not '{row_loc}'"
                            log.append({"level": "warn", "message": msg})

                else:
                    msg = f"❌ Unknown transaction type {input_tx}"
                    log.append({"level": "error", "message": msg})
                    st.session_state["scan_validation_log"] = log
                    raise Exception(msg)

    st.session_state["scan_validation_log"] = log
# ───────────────────────────────────────────────────────────────
#  4.  Helper: commit_scan_items (atomic, row‑level locks)
# ───────────────────────────────────────────────────────────────
def commit_scan_items(scan_map, input_tx: TxType, warehouse_sel: str, user: str, note: str):
    adj_rows = st.session_state.get("adj_rows", [])
    with get_db_cursor() as cur:
        for (code, job, lot), scans in scan_map.items():
            row = next(r for r in adj_rows if r["code"] == code and r["job"] == job and r["lot"] == lot)
            loc = row["location"]
            pallet_qty = max(row.get("pallet_qty") or 1, 1)

            for entry in scans:
                sid, qty_units = (entry if input_tx == TxType.TRANSFER else (entry, 1))
                qty_delta = qty_units if input_tx == TxType.RETURNB else -qty_units

                # 🔒 Lock scan ID
                cur.execute("SELECT scan_id FROM current_scan_location WHERE scan_id = %s FOR UPDATE", (sid,))

                # 🧾 Log scan verification
                cur.execute("""
                    INSERT INTO scan_verifications (
                        scan_id, item_code, job_number, lot_number,
                        location, scanned_by, transaction_type, warehouse, scan_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (sid, code, job, lot, loc, user, input_tx.value, warehouse_sel))

                # 📦 Update scan location
                if input_tx == TxType.RETURNB:
                    cur.execute("""
                        INSERT INTO current_scan_location (scan_id, item_code, location, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (scan_id) DO UPDATE
                        SET item_code = EXCLUDED.item_code,
                            location = EXCLUDED.location,
                            updated_at = EXCLUDED.updated_at
                    """, (sid, code, loc))
                else:
                    cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                # 📜 Insert transaction log
                tx_label = "Return" if input_tx == TxType.RETURNB else "Job Issue"
                loc_col = "to_location" if input_tx == TxType.RETURNB else "from_location"
                cur.execute(
                    f"""
                    INSERT INTO transactions (
                        transaction_type, date, warehouse, {loc_col},
                        job_number, lot_number, item_code, quantity, note, user_id
                    )
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (tx_label, warehouse_sel, loc, job, lot, code, abs(qty_units), note, user)
                )

                # 📊 Adjust inventory
                cur.execute("""
                    SELECT quantity FROM current_inventory
                    WHERE item_code = %s AND location = %s AND warehouse = %s
                    FOR UPDATE
                """, (code, loc, warehouse_sel))
                cur.execute("""
                    INSERT INTO current_inventory (item_code, location, warehouse, quantity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                    SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, (code, loc, warehouse_sel, qty_delta))

            # ✅ Update pulltag status if it exists
            cur.execute("""
                UPDATE pulltags
                SET status = 'kitted', last_updated = NOW()
                WHERE status = 'pending'
                  AND job_number = %s AND lot_number = %s AND item_code = %s AND transaction_type = %s
            """, (job, lot, code, input_tx.value))

            # ➕ Insert new pulltag row if missing
            # ➕ Insert new pulltag row if missing
            cur.execute("""
                SELECT 1 FROM pulltags
                WHERE job_number = %s AND lot_number = %s AND item_code = %s AND transaction_type = %s
            """, (job, lot, code, input_tx.value))
            
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO pulltags (
                        job_number, lot_number, item_code, quantity,
                        description, cost_code, uom, status,
                        transaction_type, note, warehouse
                    )
                    SELECT %s, %s, %s, %s,
                           im.item_description, im.cost_code, im.uom,
                           'kitted', %s, %s, %s
                    FROM items_master im
                    WHERE im.item_code = %s
                """, (
                    job, lot, code,
                    qty_delta if input_tx == TxType.RETURNB else abs(qty_delta),
                    input_tx.value, note, warehouse_sel,
                    code
                ))


# ───────────────────────────────────────────────────────────────
#  5.  Helper: load_pending_pulltags
# ───────────────────────────────────────────────────────────────
def load_pending_pulltags(tx_type: str, warehouse: str) -> list[dict]:
    """
    Returns a list of adjustment row dicts built from pulltags
    with status='pending', filtered by transaction type and warehouse.
    Ordered by uploaded_at (FIFO for fulfillment).
    """
    rows = []
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT pt.job_number, pt.lot_number, pt.item_code, pt.quantity,
                   pt.note, im.scan_required
            FROM pulltags pt
            JOIN items_master im ON pt.item_code = im.item_code
            WHERE pt.status = 'pending'
              AND pt.transaction_type = %s
              AND pt.warehouse = %s
            ORDER BY pt.uploaded_at
        """, (tx_type, warehouse))

        for job, lot, code, qty, note, scan_req in cur.fetchall():
            rows.append({
                "job": job,
                "lot": lot,
                "code": code,
                "qty": qty,
                "location": "",
                "scan_required": bool(scan_req),
                "pallet_qty": 1,
                "note": note or "loaded from request"
            })

    return rows

# ───────────────────────────────────────────────────────────────
#  6.  Helper: log view & export
# ───────────────────────────────────────────────────────────────
def show_validation_log():
    log = st.session_state.get("scan_validation_log", [])
    if not log:
        st.info("No scan validation messages.")
        return
    for e in log:
        (st.warning if e["level"] == "warn" else st.error if e["level"] == "error" else st.write)(e["message"])

def export_validation_log_csv():
    log = st.session_state.get("scan_validation_log", [])
    if log:
        csv = pd.DataFrame(log).to_csv(index=False).encode("utf-8")
        st.download_button("⬇ Export Validation Log CSV", data=csv, file_name="scan_validation_log.csv")

# ───────────────────────────────────────────────────────────────
#  7.  Request Tab
# ───────────────────────────────────────────────────────────────
#V3
# ───────────────────────────────────────────────────────────────
# canonical request() logic — validated July 2025
# - Handles pulltag requests for ADD, RETURNB, TRANSFER
# - Deduplicates rows
# - Pulls metadata from items_master
# - Writes pulltags with status = 'pending'
# - Supports CSV export
# ───────────────────────────────────────────────────────────────
def requests():
    st.title("📝 Request Pulltags")

    tx_type = st.selectbox("Transaction Type", [t.value for t in TxType])
    warehouse = st.selectbox("Warehouse", WAREHOUSES)
    note = st.text_input("Note (optional)", placeholder="e.g. urgent, staging restock")

    if "request_rows" not in st.session_state:
        st.session_state.request_rows = []

    # ⬆️ Submit button at the top
    if st.session_state.request_rows:
        if st.button("📨 Submit All Requests"):
            with get_db_cursor() as cur:
                for row in st.session_state.request_rows:
                    cur.execute("""
                        INSERT INTO pulltags (
                            job_number, lot_number, item_code, quantity,
                            description, cost_code, uom, status,
                            transaction_type, note, warehouse
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s, %s)
                    """, (
                        row["job"], row["lot"], row["code"],
                        -row["qty"] if tx_type == "RETURNB" else row["qty"],
                        row["description"], row["cost_code"], row["uom"],
                        tx_type, row["note"], warehouse
                    ))

            st.success("✅ Requests submitted.")
            st.session_state.request_rows = []

    # ➕ Add row form
    with st.form("add_request_row"):
        c1, c2, c3 = st.columns([2, 2, 2])
        job = c1.text_input("Job Number")
        lot = c2.text_input("Lot Number")
        code = c3.text_input("Item Code")
        qty = st.number_input("Quantity", min_value=1, value=1)

        submitted = st.form_submit_button("➕ Add to Request List")
        if submitted:
            with get_db_cursor() as cur:
                cur.execute("""
                    SELECT item_description, cost_code, uom, scan_required
                    FROM items_master
                    WHERE item_code = %s
                """, (code.strip(),))
                meta = cur.fetchone()

            if not meta:
                st.error(f"Item code '{code}' not found in items_master.")
            else:
                job_clean = job.strip()
                lot_clean = lot.strip()
                code_clean = code.strip()

                # ❌ Duplicate row check
                duplicate = any(
                    r["job"] == job_clean and r["lot"] == lot_clean and r["code"] == code_clean
                    for r in st.session_state.request_rows
                )

                if duplicate:
                    st.warning(f"⚠️ Row for {code_clean} (Job {job_clean}, Lot {lot_clean}) already exists.")
                else:
                    st.session_state.request_rows.append({
                        "job": job_clean,
                        "lot": lot_clean,
                        "code": code_clean,
                        "qty": qty,
                        "note": note.strip() or "requested",
                        "description": meta[0],
                        "cost_code": meta[1],
                        "uom": meta[2],
                        "scan_required": bool(meta[3])
                    })
                    st.rerun()

    # 📋 Show rows
    if st.session_state.request_rows:
        st.markdown("### 📋 Request List")
        for idx, row in enumerate(st.session_state.request_rows):
            cols = st.columns([1.5, 1.5, 2, 1.5, 2, 1])
            cols[0].write(row["job"])
            cols[1].write(row["lot"])
            cols[2].write(row["code"])
            cols[3].write(str(row["qty"]))
            cols[4].write(row["note"])
            if cols[5].button("❌", key=f"del_row_{idx}"):
                st.session_state.request_rows.pop(idx)
                st.rerun()

        # 📄 Download options
        df = pd.DataFrame(st.session_state.request_rows)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇ Export CSV", data=csv, file_name="pulltag_requests.csv", use_container_width=True)
# ───────────────────────────────────────────────────────────────
#  8.  Kitting Tabs (Add‑On / Return / Transfer)
#
# ───────────────────────────────────────────────────────────────

#v2
def adjustments_return():
    st.title("🔁 Return (Material Back In)")

    user = st.session_state.get("user", "unknown")
    warehouse = st.selectbox("Warehouse", WAREHOUSES, key="return_wh")
    note = st.text_input("Note (optional)", placeholder="e.g. excess material return")
    # Set global default location
    if "global_default_location" not in st.session_state:
        st.session_state["global_default_location"] = ""

    default_location = st.text_input(
        "📍 Default Location (applies to empty rows)",
        value=st.session_state["global_default_location"],
        key=f"default_location_RETURNB"
    )


    if "adj_rows" not in st.session_state:
        st.session_state["adj_rows"] = []

    if st.button("📥 Load Pending Requests"):
        pulled = load_pending_pulltags(tx_type=TxType.RETURNB.value, warehouse=warehouse)
        if pulled:
            st.session_state["adj_rows"].extend(pulled)
            st.success(f"✅ Added {len(pulled)} request row(s) to the batch.")
        else:
            st.info("No pending requests found.")

    # ➕ Manual Add Row
    with st.form("add_manual_return_row"):
        c1, c2, c3 = st.columns([2, 2, 2])
        job = c1.text_input("Job Number")
        lot = c2.text_input("Lot Number")
        code = c3.text_input("Item Code")
        qty = st.number_input("Quantity", min_value=1, value=1)
    
        submitted = st.form_submit_button("➕ Add Manual Row")
        if submitted:
            # Fetch scan_required from items_master
            with get_db_cursor() as cur:
                cur.execute("SELECT scan_required FROM items_master WHERE item_code = %s", (code.strip(),))
                result = cur.fetchone()
                scan_required = bool(result[0]) if result else True  # fallback to True if missing
    
            st.session_state["adj_rows"].append({
                "job": job.strip(),
                "lot": lot.strip(),
                "code": code.strip(),
                "qty": qty,
                "location": "",
                "scan_required": scan_required
            })
            st.rerun()


    adjustments = st.session_state.get("adj_rows", [])

    if adjustments and st.button("✅ Submit Return", key="return_submit"):
        try:
            scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}
            scan_map = collect_scan_map(adjustments, scan_inputs, input_tx=TxType.RETURNB)
            validate_scan_items(scan_map, input_tx=TxType.RETURNB, warehouse_sel=warehouse)
            commit_scan_items(scan_map, input_tx=TxType.RETURNB, warehouse_sel=warehouse, user=user, note=note)
            st.success("✅ Return committed.")
            st.session_state["adj_rows"] = []
            for k in list(st.session_state.keys()):
                if k.startswith("scan_"):
                    del st.session_state[k]
            st.session_state.pop("scan_validation_log", None)
        except Exception as e:
            st.error(f"❌ Submission failed: {e}")

    if adjustments:
        st.markdown("### ✏️ Edit Return Batch")
        rows_to_keep = []
        for idx, row in enumerate(adjustments):
            cols = st.columns([2, 2, 2, 1.5, 2, 1])
            cols[0].write(f"Job: {row['job']}")
            cols[1].write(f"Lot: {row['lot']}")
            cols[2].write(f"Item: {row['code']}")
            cols[3].write(f"Qty: {row['qty']}")
            row["location"] = cols[4].text_input(
                "Location",
                value=row.get("location") or default_location,
                key=f"loc_{idx}"
            )

            if not cols[5].button("❌", key=f"remove_{idx}"):
                rows_to_keep.append(row)
        st.session_state["adj_rows"] = rows_to_keep

        # 🔍 Scan Inputs
        st.markdown("### 🔍 Scan Inputs")
        for idx, row in enumerate(st.session_state["adj_rows"]):
            if row.get("scan_required"):
                for i in range(1, row["qty"] + 1):
                    st.text_input(
                        f"{row['code']} — Job {row['job']} / Lot {row['lot']} — Scan #{i}",
                        key=f"scan_{row['code']}_{row['job']}_{row['lot']}_{i}_row{idx}"
                    )

        df = pd.DataFrame(st.session_state["adj_rows"])
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇ Export Adjustment CSV", data=csv, file_name="return_batch.csv")

        scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}
        if st.button("🔍 Preview Scan Validity"):
            try:
                scan_map = collect_scan_map(st.session_state["adj_rows"], scan_inputs, input_tx=TxType.RETURNB)
                validate_scan_items(scan_map, input_tx=TxType.RETURNB, warehouse_sel=warehouse)
                st.success("✅ No blocking errors detected.")
            except Exception as e:
                st.error(f"❌ Validation failed: {e}")

        show_validation_log()
        export_validation_log_csv()
       

#v2
def adjustments_add():
    st.title("➕ Add-On (Job Issue)")

    user = st.session_state.get("user", "unknown")
    warehouse = st.selectbox("Warehouse", WAREHOUSES, key="addon_wh")
    note = st.text_input("Note (optional)", placeholder="e.g. final punch")
    # Set global default location
    if "global_default_location" not in st.session_state:
        st.session_state["global_default_location"] = ""

    default_location = st.text_input(
        "📍 Default Location (applies to empty rows)",
        value=st.session_state["global_default_location"],
        key=f"default_location_ADD"
    )

    if "adj_rows" not in st.session_state:
        st.session_state["adj_rows"] = []

    if st.button("📥 Load Pending Requests"):
        pulled = load_pending_pulltags(tx_type=TxType.ADD.value, warehouse=warehouse)
        if pulled:
            st.session_state["adj_rows"].extend(pulled)
            st.success(f"✅ Added {len(pulled)} request row(s) to the batch.")
        else:
            st.info("No pending requests found.")

    # ➕ Add Manual Row
    with st.form("add_manual_addon_row"):
        c1, c2, c3 = st.columns([2, 2, 2])
        job = c1.text_input("Job Number")
        lot = c2.text_input("Lot Number")
        code = c3.text_input("Item Code")
        qty = st.number_input("Quantity", min_value=1, value=1)
    
        submitted = st.form_submit_button("➕ Add Manual Row")
        if submitted:
            # Fetch scan_required from items_master
            with get_db_cursor() as cur:
                cur.execute("SELECT scan_required FROM items_master WHERE item_code = %s", (code.strip(),))
                result = cur.fetchone()
                scan_required = bool(result[0]) if result else True  # fallback to True if not found
    
            st.session_state["adj_rows"].append({
                "job": job.strip(),
                "lot": lot.strip(),
                "code": code.strip(),
                "qty": qty,
                "location": "",
                "scan_required": scan_required
            })
            st.rerun()


    adjustments = st.session_state.get("adj_rows", [])

    if adjustments and st.button("✅ Submit Add-On", key="addon_submit"):
        try:
            scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}
            scan_map = collect_scan_map(adjustments, scan_inputs, input_tx=TxType.ADD)
            validate_scan_items(scan_map, input_tx=TxType.ADD, warehouse_sel=warehouse)
            commit_scan_items(scan_map, input_tx=TxType.ADD, warehouse_sel=warehouse, user=user, note=note)
            st.success("✅ Add-On committed.")
            st.session_state["adj_rows"] = []
            for k in list(st.session_state.keys()):
                if k.startswith("scan_"):
                    del st.session_state[k]
            st.session_state.pop("scan_validation_log", None)
        except Exception as e:
            st.error(f"❌ Submission failed: {e}")

    if adjustments:
        st.markdown("### ✏️ Edit Add-On Batch")
        rows_to_keep = []
        for idx, row in enumerate(adjustments):
            cols = st.columns([2, 2, 2, 1.5, 2, 1])
            cols[0].write(f"Job: {row['job']}")
            cols[1].write(f"Lot: {row['lot']}")
            cols[2].write(f"Item: {row['code']}")
            cols[3].write(f"Qty: {row['qty']}")
            row["location"] = cols[4].text_input(
                "Location",
                value=row.get("location") or default_location,
                key=f"loc_{idx}"
            )

            if not cols[5].button("❌", key=f"remove_{idx}"):
                rows_to_keep.append(row)
        st.session_state["adj_rows"] = rows_to_keep

        st.markdown("### 🔍 Scan Inputs")
        for idx, row in enumerate(st.session_state["adj_rows"]):
            if row.get("scan_required"):
                for i in range(1, row["qty"] + 1):
                    st.text_input(
                        f"{row['code']} — Job {row['job']} / Lot {row['lot']} — Scan #{i}",
                        key=f"scan_{row['code']}_{row['job']}_{row['lot']}_{i}_row{idx}"
                    )

        df = pd.DataFrame(st.session_state["adj_rows"])
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇ Export Adjustment CSV", data=csv, file_name="addon_batch.csv")

        scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}
        if st.button("🔍 Preview Scan Validity"):
            try:
                scan_map = collect_scan_map(st.session_state["adj_rows"], scan_inputs, input_tx=TxType.ADD)
                validate_scan_items(scan_map, input_tx=TxType.ADD, warehouse_sel=warehouse)
                st.success("✅ No blocking errors detected.")
            except Exception as e:
                st.error(f"❌ Validation failed: {e}")

        show_validation_log()
        export_validation_log_csv()

#v2

def adjustments_transfer():
    st.title("📦 Transfer (Shipping Out)")

    user = st.session_state.get("user", "unknown")
    warehouse = st.selectbox("Warehouse", WAREHOUSES, key="transfer_wh")
    note = st.text_input("Note (optional)", placeholder="e.g. shipping pallets to Chowchilla")
    # Set global default location
    if "global_default_location" not in st.session_state:
        st.session_state["global_default_location"] = ""

    default_location = st.text_input(
        "📍 Default Location (applies to empty rows)",
        value=st.session_state["global_default_location"],
        key=f"default_location_TRANSFER"
    )



    if "adj_rows" not in st.session_state:
        st.session_state["adj_rows"] = []

    # 🚚 Load pending pulltags
    if st.button("📥 Load Pending Requests"):
        pulled = load_pending_pulltags(tx_type=TxType.TRANSFER.value, warehouse=warehouse)
        if pulled:
            st.session_state["adj_rows"].extend(pulled)
            st.success(f"✅ Added {len(pulled)} request row(s) to the batch.")
        else:
            st.info("No pending requests found.")

    # ➕ Manual Add Row Form
    with st.form("add_manual_transfer_row"):
        c1, c2, c3 = st.columns([2, 2, 2])
        job = c1.text_input("Job Number")
        lot = c2.text_input("Lot Number")
        code = c3.text_input("Item Code")
        qty = st.number_input("Quantity", min_value=1, value=1)
    
        submitted = st.form_submit_button("➕ Add Manual Row")
        if submitted:
            # Pull scan_required from items_master
            with get_db_cursor() as cur:
                cur.execute("SELECT scan_required FROM items_master WHERE item_code = %s", (code.strip(),))
                result = cur.fetchone()
                scan_required = bool(result[0]) if result else True
    
            st.session_state["adj_rows"].append({
                "job": job.strip(),
                "lot": lot.strip(),
                "code": code.strip(),
                "qty": qty,
                "location": "",
                "scan_required": scan_required,
                "pallet_qty": 1
            })
            st.rerun()


    adjustments = st.session_state.get("adj_rows", [])

    # ✅ Submit
    if adjustments and st.button("✅ Submit Transfer", key="transfer_submit"):
        try:
            scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}
            scan_map = collect_scan_map(adjustments, scan_inputs, input_tx=TxType.TRANSFER)
            validate_scan_items(scan_map, input_tx=TxType.TRANSFER, warehouse_sel=warehouse)
            commit_scan_items(scan_map, input_tx=TxType.TRANSFER, warehouse_sel=warehouse, user=user, note=note)

            st.success("✅ Transfer committed successfully.")
            st.session_state["adj_rows"] = []
            for k in list(st.session_state.keys()):
                if k.startswith("scan_"):
                    del st.session_state[k]
            st.session_state.pop("scan_validation_log", None)
        except Exception as e:
            st.error(f"❌ Submission failed: {e}")

    # ✏️ Row Editor
    if adjustments:
        st.markdown("### ✏️ Edit Transfer Batch")
        rows_to_keep = []
        for idx, row in enumerate(adjustments):
            cols = st.columns([2, 2, 2, 1.5, 2, 1, 1])
            cols[0].write(f"Job: {row['job']}")
            cols[1].write(f"Lot: {row['lot']}")
            cols[2].write(f"Item: {row['code']}")
            cols[3].write(f"Qty: {row['qty']}")
            row["location"] = cols[4].text_input(
                "Location",
                value=row.get("location") or default_location,
                key=f"loc_{idx}"
            )

            row["pallet_qty"] = cols[5].number_input("Pallet Qty", min_value=1, value=row.get("pallet_qty", 1), key=f"pq_{idx}")
            if not cols[6].button("❌", key=f"remove_{idx}"):
                rows_to_keep.append(row)
        st.session_state["adj_rows"] = rows_to_keep

        # 🔍 Scan Inputs
        st.markdown("### 🔍 Scan Pallet IDs")
        for idx, row in enumerate(adjustments):
            scan_count = -(-row["qty"] // max(row.get("pallet_qty") or 1, 1))
            for i in range(1, scan_count + 1):
                st.text_input(
                    f"{row['code']} — Job {row['job']} / Lot {row['lot']} — Pallet #{i}",
                    key=f"scan_{row['code']}_{row['job']}_{row['lot']}_{i}_row{idx}"
                )

        # 📄 CSV Export
        df = pd.DataFrame(adjustments)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇ Export Adjustment CSV", data=csv, file_name="transfer_batch.csv")

        # 🔍 Preview Validation
        scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith("scan_")}
        if st.button("🔍 Preview Scan Validity"):
            try:
                scan_map = collect_scan_map(adjustments, scan_inputs, input_tx=TxType.TRANSFER)
                validate_scan_items(scan_map, input_tx=TxType.TRANSFER, warehouse_sel=warehouse)
                st.success("✅ No blocking errors detected.")
            except Exception as e:
                st.error(f"❌ Validation failed: {e}")

        show_validation_log()
        export_validation_log_csv()


# ───────────────────────────────────────────────────────────────
#  9.  Dashboard (pending & fulfilled)
# ───────────────────────────────────────────────────────────────
def show_pending_pulltags():
    st.subheader("📥 Pending Pulltags")

    wh = st.selectbox("Warehouse", WAREHOUSES, key="dash_p_wh")
    tx_type = st.selectbox("Transaction Type", ["ADD", "RETURNB", "TRANSFER"], key="dash_p_tx")

    with get_db_cursor() as cur:
        cur.execute("""
            SELECT job_number, lot_number, item_code, quantity, transaction_type, note, last_updated
            FROM pulltags
            WHERE status = 'pending'
              AND warehouse = %s
              AND transaction_type = %s
              AND transaction_type IN ('ADD', 'RETURNB', 'TRANSFER')
            ORDER BY job_number, last_updated
        """, (wh, tx_type))

        df = pd.DataFrame(cur.fetchall(), columns=["Job", "Lot", "Item", "Qty", "Tx", "Note", "Updated"])

    st.dataframe(df, use_container_width=True)
    st.download_button("⬇ Export Pending CSV", df.to_csv(index=False).encode(), file_name="pending_pulltags.csv")


def show_fulfilled_pulltags():
    st.subheader("✅ Fulfilled Pulltags")

    wh = st.selectbox("Warehouse", WAREHOUSES, key="dash_f_wh")
    tx_type = st.selectbox("Transaction Type", ["ADD", "RETURNB", "TRANSFER"], key="dash_f_tx")

    col1, col2 = st.columns(2)
    start = col1.date_input("Start Date", pd.to_datetime("today") - pd.Timedelta(30))
    end = col2.date_input("End Date", pd.to_datetime("today"))

    job = st.text_input("Job Number (optional)")
    lot = st.text_input("Lot Number (optional)")

    # Base query
    q = """
        SELECT job_number, lot_number, item_code, quantity,
               transaction_type, note, last_updated
        FROM pulltags
        WHERE status = 'kitted'
          AND warehouse = %s
          AND transaction_type = %s
          AND transaction_type IN ('ADD', 'RETURNB', 'TRANSFER')
          AND last_updated BETWEEN %s AND %s
    """
    p = [wh, tx_type, start, end]

    if job:
        q += " AND job_number = %s"
        p.append(job.strip())
    if lot:
        q += " AND lot_number = %s"
        p.append(lot.strip())

    q += " ORDER BY last_updated DESC"

    with get_db_cursor() as cur:
        cur.execute(q, tuple(p))
        df = pd.DataFrame(cur.fetchall(), columns=["Job", "Lot", "Item", "Qty", "Tx", "Note", "Kitted"])

    st.dataframe(df, use_container_width=True)
    st.download_button("⬇ Export Fulfilled CSV", df.to_csv(index=False).encode(), file_name="fulfilled_pulltags.csv")


def dashboard():
    st.title("📊 Pulltag Dashboard")
    tab1, tab2 = st.tabs(["📥 Pending", "✅ Fulfilled"])
    with tab1:
        show_pending_pulltags()
    with tab2:
        show_fulfilled_pulltags()

# ───────────────────────────────────────────────────────────────
# 10.  Main navigation
# ───────────────────────────────────────────────────────────────

def run():
    st.sidebar.markdown("### 🛠️ Adjustments Navigation")
    choice = st.sidebar.radio("Select workflow:", [
        "📊 Pulltag Dashboard",
        "📝 Request Pulltags",
        "➕ Add-On (Job Issue)",
        "🔁 Return (Material Back)",
        "📦 Transfer (Shipping Out)"
    ])

    if choice == "📊 Pulltag Dashboard":
        dashboard()
    elif choice == "📝 Request Pulltags":
        requests()
    elif choice == "➕ Add-On (Job Issue)":
        adjustments_add()
    elif choice == "🔁 Return (Material Back)":
        adjustments_return()
    elif choice == "📦 Transfer (Shipping Out)":
        adjustments_transfer()

