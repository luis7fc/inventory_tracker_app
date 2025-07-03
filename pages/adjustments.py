import streamlit as st
import random
from collections import Counter, defaultdict
from enum import Enum
from contextlib import contextmanager
import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from config import WAREHOUSES

# Initialize connection pool (singleton, created once)
DB_POOL = None

def init_db_pool():
    global DB_POOL
    if DB_POOL is None:
        DB_POOL = ThreadedConnectionPool(
            minconn=2,  # Minimum connections
            maxconn=30,  # Maximum connections, adjust based on DB capacity
            host=st.secrets["DB_HOST"],
            dbname=st.secrets["DB_NAME"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"],
            port=st.secrets.get("DB_PORT", 5432)
        )

@contextmanager
def get_db_cursor():
    """Yields a cursor from the connection pool, commits/rolls back, and returns connection to pool."""
    init_db_pool()  # Ensure pool is initialized
    conn = DB_POOL.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("SET statement_timeout = 5000")  # 5-second query timeout
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        DB_POOL.putconn(conn)

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
        (scan_id, code, job, lot, loc_val, user, input_tx.value, warehouse),
    )

def update_scan_location(scan_id, code, loc_val, input_tx, cur):
    # Note: Assumes unique index on scan_id to prevent duplicates
    # Run once: CREATE UNIQUE INDEX unique_scan_id ON current_scan_location (scan_id);
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
    else:  # TxType.ADD
        cur.execute(
            "SELECT location FROM current_scan_location WHERE scan_id = %s FOR UPDATE",
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
    # Note: Assumes version column added to current_inventory
    # Run once: ALTER TABLE current_inventory ADD COLUMN version INTEGER DEFAULT 0;
    cur.execute(
        """
        SELECT quantity, version FROM current_inventory
        WHERE item_code = %s AND location = %s AND warehouse = %s FOR UPDATE
        """,
        (code, loc_val, warehouse)
    )
    row = cur.fetchone()
    current_quantity = row[0] if row else 0
    current_version = row[1] if row else 0

    new_quantity = current_quantity + delta
    if new_quantity < 0:
        raise Exception(f"Cannot reduce inventory below 0 for {code} at {loc_val}")

    if row:
        cur.execute(
            """
            UPDATE current_inventory
            SET quantity = %s, version = %s
            WHERE item_code = %s AND location = %s AND warehouse = %s AND version = %s
            """,
            (new_quantity, current_version + 1, code, loc_val, warehouse, current_version)
        )
        if cur.rowcount == 0:
            raise Exception("Concurrent update detected for inventory")
    else:
        cur.execute(
            """
            INSERT INTO current_inventory
              (item_code, location, warehouse, quantity, version)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (code, loc_val, warehouse, new_quantity, 1)
        )

def insert_pulltag_line(cur, job, lot, code, qty, loc, tx_type_str, note, warehouse_sel=None):
    insert_qty = -qty if TxType(tx_type_str) is TxType.RETURNB else qty
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
        (job, lot, insert_qty, tx_type_str, note, warehouse, code),
    )
    result = cur.fetchone()
    if not result:
        raise Exception(f"Item '{code}' not found in items_master.")
    return result[0]

def finalize_scan_items(adjustments, scans_needed, scan_inputs, *, from_loc, to_loc, user, note, input_tx_str, warehouse_sel, progress_cb=None):
    if progress_cb is None:
        progress_cb = lambda *_: None

    input_tx = TxType(input_tx_str)
    scan_map: dict = defaultdict(list)
    errors: list[str] = []
    for row_idx, row in enumerate(adjustments):
        if not row.get('scan_required'):
            continue
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
        cur.execute("BEGIN")  # Start transaction
        try:
            loc_val = from_loc or to_loc
            cur.execute("SELECT warehouse FROM locations WHERE location_code = %s FOR UPDATE", (loc_val,))
            loc_row = cur.fetchone()
            if not loc_row:
                raise Exception(f"Location '{loc_val}' not found.")
            if loc_row[0] != warehouse_sel:
                raise Exception(f"Location '{loc_val}' is in warehouse '{loc_row[0]}', not '{warehouse_sel}'.")

            for (code, job, lot), sid_list in scan_map.items():
                loc_val = to_loc if input_tx is TxType.RETURNB else from_loc
                for sid in sid_list:
                    warnings, errs = validate_scan(sid, code, from_loc, to_loc, input_tx, cur)
                    if errs:
                        raise Exception(errs[0])

                    insert_scan_verification(sid, code, job, lot, loc_val, user, input_tx, warehouse_sel, cur)
                    update_scan_location(sid, code, loc_val, input_tx, cur)
                    insert_transaction(input_tx, warehouse_sel, loc_val, job, lot, code, note, user, cur)
                    adjust_inventory(code, loc_val, warehouse_sel, 1 if input_tx is TxType.RETURNB else -1, cur)

                    completed += 1
                    if total > 0:
                        progress_cb(int(completed / total * 100))

            cur.execute("COMMIT")  # Commit transaction
        except Exception as exc:
            cur.execute("ROLLBACK")  # Rollback on error
            st.error(f"Transaction failed: {exc}")
            with st.expander("Debug Info", expanded=True):
                st.code(f"Scan Map: {dict(scan_map)}\nError: {exc}", language="text")
            raise

def preview_scan_validity(adjustments, scans_needed, scan_inputs, from_loc, to_loc, input_tx_str, warehouse_sel):
    results: list[dict] = []
    input_tx = TxType(input_tx_str)
    
    with get_db_cursor() as cur:
        for row_idx, row in enumerate(adjustments):
            if not row.get('scan_required'):
                continue
            code, job, lot, qty = row["code"], row["job"], row["lot"], row["qty"]
            for i in range(1, qty + 1):
                key = f"scan_{code}_{job}_{lot}_{i}_row{row_idx}"
                sid = scan_inputs.get(key, "").strip()
                if not sid:
                    results.append({
                        "scan_id": f"Missing Scan #{i}", "item_code": code, "job": job, "lot": lot,
                        "status": "❌ Missing", "reason": f"Scan #{i} is missing", "bypassable": False
                    })
                    continue

                warnings, errors = validate_scan(sid, code, from_loc, to_loc, input_tx, cur)
                if errors:
                    status, reason, bypassable = "❌ Invalid", "; ".join(errors), False
                elif warnings:
                    status, reason, bypassable = "⚠️ Warning", "; ".join(warnings), True
                else:
                    status, reason, bypassable = "✅ Valid", "", False

                results.append({
                    "scan_id": sid, "item_code": code, "job": job, "lot": lot,
                    "status": status, "reason": reason, "bypassable": bypassable
                })

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

def show_scan_preview(adjustments, scans_needed, scan_inputs, location, tx_input_str, warehouse_sel):
    from_loc = location if TxType(tx_input_str) is TxType.ADD else ""
    to_loc = location if TxType(tx_input_str) is TxType.RETURNB else ""

    preview = preview_scan_validity(
        adjustments,
        scans_needed,
        scan_inputs,
        from_loc=from_loc,
        to_loc=to_loc,
        input_tx_str=tx_input_str,
        warehouse_sel=warehouse_sel
    )

    st.markdown("### 🧾 Scan Validation Preview")
    if not preview:
        st.info("No scannable items to preview.")
        return []
        
    for entry in preview:
        st.write(f"{entry['status']} **{entry['item_code']}** | Scan: `{entry['scan_id']}` | Job: {entry['job']} | Lot: {entry['lot']}")
        if entry['reason']:
            st.caption(f"   ↳ {entry['reason']}")
    return preview

def run():
    st.title("🛠️ Post-Kitting Adjustments")

    if not st.session_state.get("user"):
        st.error("Please log in to use this page.")
        st.stop()
    user = st.session_state.user

    # Debounce submissions
    if st.session_state.get('submitting'):
        st.error("Submission in progress. Please wait.")
        st.stop()

    tx_input = st.selectbox("Transaction Type", [t.value for t in TxType])
    warehouse_sel = st.selectbox("Warehouse", WAREHOUSES)
    location = st.text_input("Location")
    note = st.text_input("Note (optional)")

    if "adj_rows" not in st.session_state:
        st.session_state.adj_rows = []
    adjustments = st.session_state.adj_rows

    with st.expander("➕ Add Row"):
        c1, c2, c3, c4 = st.columns([2, 2, 3, 1])
        job = c1.text_input("Job #")
        lot = c2.text_input("Lot #")
        code = c3.text_input("Item Code")
        qty = c4.number_input("Qty", min_value=1, value=1)
        if st.button("Add to List"):
            if job and lot and code and qty > 0:
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT item_description, scan_required FROM items_master WHERE item_code=%s",
                        (code.strip(),)
                    )
                    data = cur.fetchone()
                adjustments.append({
                    "job": job.strip(), "lot": lot.strip(), "code": code.strip(), "qty": qty,
                    "scan_required": bool(data and data[1])
                })
                st.rerun()
            else:
                st.warning("Fill all fields before adding.")

    if adjustments:
        st.markdown("### 📋 Pending Adjustments")
        for idx, row in enumerate(adjustments):
            cols = st.columns([2, 2, 3, 1, 1, 1])
            cols[0].write(row['job'])
            cols[1].write(row['lot'])
            cols[2].write(row['code'])
            cols[3].write(str(row['qty']))
            cols[4].write("🔒" if row['scan_required'] else "—")
            if cols[5].button("❌", key=f"del{idx}"):
                adjustments.pop(idx)
                st.rerun()

    scannable_items_exist = any(r.get('scan_required') for r in adjustments)
    if scannable_items_exist:
        st.markdown("### 🔍 Enter Scan IDs")
        for idx, row in enumerate(adjustments):
            if row.get('scan_required'):
                for i in range(1, row['qty'] + 1):
                    st.text_input(
                        f"Scan ID for {row['code']} — Job {row['job']} / Lot {row['lot']} #{i}",
                        key=f"scan_{row['code']}_{row['job']}_{row['lot']}_{i}_row{idx}"
                    )

    if st.button("🔍 Preview Scan Validity"):
        if not location:
            st.error("Location is required to validate scans.")
        else:
            scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith('scan_')}
            scans_needed = defaultdict(lambda: defaultdict(int))
            for row in adjustments:
                if row.get("scan_required"):
                    scans_needed[row["code"]][(row["job"], row["lot"])] += row["qty"]

            st.session_state['scan_preview'] = show_scan_preview(
                adjustments=adjustments,
                scans_needed=dict(scans_needed),
                scan_inputs=scan_inputs,
                location=location,
                tx_input_str=tx_input,
                warehouse_sel=warehouse_sel
            )

    if st.button("Submit Adjustments"):
        if not location:
            st.error("Location required first.")
            st.stop()

        st.session_state['submitting'] = True
        try:
            preview = st.session_state.get('scan_preview', [])
            errors = [e for e in preview if e['status'].startswith('❌')]
            warnings = [e for e in preview if e['status'].startswith('⚠️')]

            if errors:
                st.error("Cannot submit due to errors:")
                for e in errors:
                    st.write(f"- {e['item_code']} (`{e['scan_id']}`): {e['reason']}")
                st.stop()

            if warnings and TxType(tx_input) is TxType.ADD and not st.session_state.get('bypass_confirmed'):
                st.warning("There are warnings that require confirmation:")
                for e in warnings:
                    st.write(f"- {e['item_code']} (`{e['scan_id']}`): {e['reason']}")
                if st.button("Confirm and Proceed with Warnings"):
                    st.session_state['bypass_confirmed'] = True
                    st.rerun()
                st.stop()

            scan_inputs = {k: v for k, v in st.session_state.items() if k.startswith('scan_')}
            scans_needed = any(r.get('scan_required') for r in adjustments)

            if scans_needed:
                finalize_scan_items(
                    adjustments, scans_needed, scan_inputs,
                    from_loc=location if TxType(tx_input) is TxType.ADD else '',
                    to_loc=location if TxType(tx_input) is TxType.RETURNB else '',
                    user=user, note=note,
                    input_tx_str=tx_input, warehouse_sel=warehouse_sel
                )
            
            with get_db_cursor() as cur:
                for row in adjustments:
                    if not row.get('scan_required'):
                        insert_pulltag_line(
                            cur, row['job'], row['lot'], row['code'], row['qty'],
                            location, tx_input, note, warehouse_sel=warehouse_sel
                        )

            st.success(random.choice(IRISH_TOASTS))
            st.session_state.adj_rows = []
            st.session_state.pop('scan_preview', None)
            st.session_state.pop('bypass_confirmed', None)
            for key in [k for k in st.session_state if k.startswith('scan_')]:
                del st.session_state[key]
            st.rerun()

        except Exception as exc:
            st.error(f"Submission failed: {exc}")
            st.stop()
        finally:
            st.session_state['submitting'] = False
