# db.py - Database Utilities for Citadel WH Management
import streamlit as st
import psycopg2
import bcrypt
from contextlib import contextmanager

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

# --- Location Utilities ---
def get_all_locations():
    with get_db_cursor() as cursor:
        cursor.execute("SELECT location_code FROM locations")
        return [row[0] for row in cursor.fetchall()]

def validate_location_exists(location_code):
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM locations WHERE location_code = %s",
            (location_code,)
        )
        return cursor.fetchone() is not None

# --- Inventory Transactions ---
def insert_transaction(transaction_data):
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO transactions (
                transaction_type, item_code, quantity, date,
                job_number, lot_number, po_number,
                from_location, to_location,
                from_warehouse, to_warehouse,
                user_id, bypassed_warning, note, warehouse
            )
            VALUES (%s, %s, %s, NOW(),
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s)
            """,
            (
                transaction_data["transaction_type"],
                transaction_data["item_code"],
                transaction_data["quantity"],
                transaction_data.get("job_number"),
                transaction_data.get("lot_number"),
                transaction_data.get("po_number"),
                transaction_data.get("from_location"),
                transaction_data.get("to_location"),
                transaction_data.get("from_warehouse"),
                transaction_data.get("to_warehouse"),
                transaction_data["user_id"],
                transaction_data.get("bypassed_warning", False),
                transaction_data.get("note", ""),
                transaction_data["warehouse"]
            )
        )

# --- Scan Verifications ---
def insert_scan_verification(scan_data):
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO scan_verifications (
                item_code, job_number, lot_number,
                scan_time, scan_id, location,
                transaction_type, warehouse
            )
            VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s)
            """,
            (
                scan_data["item_code"],
                scan_data.get("job_number"),
                scan_data.get("lot_number"),
                scan_data["scan_id"],
                scan_data["location"],
                scan_data["transaction_type"],
                scan_data["warehouse"]
            )
        )

# --- Scan Location Logic ---
def validate_scan_for_transaction(cursor, scan_id, item_code, transaction_type, from_location=None, to_location=None, job_number=None):
    if transaction_type in ["Internal Movement", "Job Issue", "Kitting"] and job_number:
        cursor.execute("SELECT location FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        if cursor.fetchone():
            raise ValueError(f"Scan ID {scan_id} already exists and was already issued or in use.")
        return
    if transaction_type == "Receiving":
        cursor.execute("SELECT 1 FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        if cursor.fetchone():
            raise ValueError(f"Scan ID {scan_id} already exists in the system.")
        return
    # Other transactions: scan must exist and be in correct location
    cursor.execute("SELECT location, item_code FROM current_scan_location WHERE scan_id = %s", (scan_id,))
    result = cursor.fetchone()
    if not result:
        raise ValueError(f"Scan ID {scan_id} not found in system.")
    actual_location, actual_item = result
    if actual_item != item_code:
        raise ValueError(f"Scan ID {scan_id} belongs to item {actual_item}, not {item_code}.")
    if transaction_type in ["Internal Movement", "Job Issue"] and actual_location != from_location:
        raise ValueError(f"Scan ID {scan_id} is in {actual_location}, not in {from_location}.")
    if transaction_type == "Return" and actual_location != to_location:
        raise ValueError(f"Scan ID {scan_id} is in {actual_location}, not return location {to_location}.")


def update_scan_location(cursor, scan_id, item_code, location, transaction_type=None, job_number=None):
    if transaction_type in ["Job Issue", "Kitting"] and job_number:
        location = f"ISSUED-{job_number}"
    cursor.execute(
        """
        INSERT INTO current_scan_location (scan_id, item_code, location, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (scan_id) DO UPDATE SET location = EXCLUDED.location, updated_at = now()
        """,
        (scan_id, item_code, location)
    )

def delete_scan_location(cursor, scan_id):
    cursor.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (scan_id,))

# --- Pull-tags Helper Functions ---

def get_pulltag_rows(job_number, lot_number):
    """
    Fetch pulltag rows for a given job/lot.
    Returns list of dicts: warehouse, item_code, description, qty_req, uom, cost_code, status
    """
    query = (
        "SELECT warehouse, item_code, description, quantity AS qty_req, uom, cost_code, status "
        "FROM pulltags WHERE job_number = %s AND lot_number = %s"
    )
    with get_db_cursor() as cur:
        cur.execute(query, (job_number, lot_number))
        rows = cur.fetchall()
    return [
        {
            'warehouse': w,
            'item_code': ic,
            'description': desc,
            'qty_req': qty,
            'uom': u,
            'cost_code': cc,
            'status': stt
        }
        for (w, ic, desc, qty, u, cc, stt) in rows
    ]

def submit_kitting(kits):
    """
    Update pulltags based on kitted quantities (replace quantity).
    kq > 0 => issue, kq < 0 => return, kq == 0 => delete.
    kits: dict[(job,lot,item_code) -> kitted_qty]
    """
    with get_db_cursor() as cur:
        for (job, lot, item_code), kq in kits.items():
            if kq > 0:
                cur.execute(
                    "UPDATE pulltags SET quantity = %s, status = 'complete' "
                    "WHERE job_number = %s AND lot_number = %s AND item_code = %s",  
                    (kq, job, lot, item_code)
                )
            elif kq < 0:
                cur.execute(
                    "UPDATE pulltags SET quantity = %s, status = 'pending' "
                    "WHERE job_number = %s AND lot_number = %s AND item_code = %s",
                    (kq, job, lot, item_code)
                )
            else:
                cur.execute(
                    "DELETE FROM pulltags WHERE job_number = %s AND lot_number = %s AND item_code = %s",
                    (job, lot, item_code)
                )

def finalize_scans(scans_needed, scan_inputs, job_lot_queue, source_location):
    """
    Process scans: insert into transactions, scan_verifications, inventory +/-,
    update current_scan_location for returns.
    scans_needed: dict[item_code -> {(job,lot): qty}]
    scan_inputs: dict[scan_key -> scan_id]
    """
    with get_db_cursor() as cur:
        for item_code, lots in scans_needed.items():
            total_needed = sum(lots.values())
            for (job, lot), need in lots.items():
                assign = min(need, total_needed)
                if assign == 0:
                    continue
                # determine type and qty
                trans_type = 'Job Issue' if assign > 0 else 'Return'
                qty = assign if assign > 0 else abs(assign)
                # fetch warehouse for record
                cur.execute(
                    "SELECT warehouse FROM pulltags "
                    "WHERE job_number = %s AND lot_number = %s AND item_code = %s LIMIT 1",  
                    (job, lot, item_code)
                )
                wh = cur.fetchone()
                warehouse = wh[0] if wh else None
                # 1) transaction record
                cur.execute(
                    "INSERT INTO transactions "
                    "(transaction_type,warehouse,source_location,job_number,lot_number,item_code,quantity) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (trans_type, warehouse, source_location, job, lot, item_code, qty)
                )
                # 2) scans
                for idx in range(1, qty+1):
                    sid = scan_inputs.get(f"scan_{item_code}_{idx}")
                    cur.execute("SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s", (sid,))
                    if cur.fetchone()[0] > 0:
                        raise Exception(f"Scan {sid} already used; return required before reuse.")
                    cur.execute(
                        "INSERT INTO scan_verifications "
                        "(item_code,scan_id,job_number,lot_number,warehouse,transaction_type) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (item_code, sid, job, lot, warehouse, trans_type)
                    )
                    if trans_type == 'Return':
                        cur.execute(
                            "INSERT INTO current_scan_location (scan_id, item_code, location) "
                            "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                            (sid, item_code, source_location)
                        )
                # 3) inventory update
                if trans_type == 'Job Issue':
                    cur.execute(
                        "UPDATE current_inventory SET quantity = quantity - %s "
                        "WHERE item_code = %s AND location = %s",
                        (qty, item_code, source_location)
                    )
                else:
                    cur.execute(
                        "UPDATE current_inventory SET quantity = quantity + %s "
                        "WHERE item_code = %s AND location = %s",
                        (qty, item_code, source_location)
                    )
                total_needed -= need
                if total_needed <= 0:
                    break
