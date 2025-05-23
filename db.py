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
    """Return all location codes."""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT location_code FROM locations")
        return [r[0] for r in cursor.fetchall()]

def validate_location_exists(location_code):
    """Check if a location exists."""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT 1 FROM locations WHERE location_code = %s", (location_code,))
        return cursor.fetchone() is not None

# --- Inventory Initialization ---
def clear_current_inventory():
    """Delete all rows in current_inventory."""
    with get_db_cursor() as cursor:
        cursor.execute("DELETE FROM current_inventory")

def bulk_insert_inventory(rows):
    """Insert or update multiple inventory rows.
    rows: iterable of (item_code, location, quantity)
    """
    with get_db_cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO current_inventory (item_code, location, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (item_code, location) DO UPDATE
            SET quantity = current_inventory.quantity + EXCLUDED.quantity
            """,
            rows
        )

# --- Inventory Transactions ---
def insert_transaction(transaction_data):
    """Insert a new transaction record."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO transactions (
                transaction_type, item_code, quantity, date,
                job_number, lot_number, po_number,
                from_location, to_location,
                user_id, bypassed_warning, note, warehouse
            )
            VALUES (%s, %s, %s, NOW(),
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s)
            """,
            (
                transaction_data.get("transaction_type"),
                transaction_data.get("item_code"),
                transaction_data.get("quantity"),
                transaction_data.get("job_number"),
                transaction_data.get("lot_number"),
                transaction_data.get("po_number"),
                transaction_data.get("from_location"),
                transaction_data.get("to_location"),
                transaction_data.get("user_id"),
                transaction_data.get("bypassed_warning", False),
                transaction_data.get("note", ""),
                transaction_data.get("warehouse")
            )
        )

# --- Scan Verifications ---
def insert_scan_verification(scan_data):
    """Insert a scan verification record."""
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
                scan_data.get("item_code"),
                scan_data.get("job_number"),
                scan_data.get("lot_number"),
                scan_data.get("scan_id"),
                scan_data.get("location"),
                scan_data.get("transaction_type"),
                scan_data.get("warehouse")
            )
        )

# --- Scan Location Logic ---
def validate_scan_for_transaction(cursor, scan_id, item_code, transaction_type,
                                   from_location=None, to_location=None, job_number=None):
    """
    Ensure a scan_id is valid for a given transaction type.
    Raises ValueError on invalid.
    """
    if transaction_type in ["Internal Movement", "Job Issue", "Kitting"] and job_number:
        cursor.execute("SELECT location FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        if cursor.fetchone():
            raise ValueError(f"Scan ID {scan_id} already exists and is in use.")
        return
    if transaction_type == "Receiving":
        cursor.execute("SELECT 1 FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        if cursor.fetchone():
            raise ValueError(f"Scan ID {scan_id} already exists in the system.")
        return
    # Other transactions: must exist and be at expected location
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
    """
    Insert or update current_scan_location for a scan.
    """
    if transaction_type in ["Job Issue", "Kitting"] and job_number:
        location_marker = f"ISSUED-{job_number}"
    else:
        location_marker = location
    cursor.execute(
        """
        INSERT INTO current_scan_location (scan_id, item_code, location, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (scan_id) DO UPDATE
        SET location = EXCLUDED.location, updated_at = NOW()
        """,
        (scan_id, item_code, location_marker)
    )
def delete_scan_location(cursor, scan_id):
    """Remove a scan_id from current_scan_location."""
    cursor.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (scan_id,))

# --- User Management ---
def create_user(username, plain_password, role):
    """Insert a new user with hashed password."""
    hashed_pw = bcrypt.hashpw(plain_password.encode(), bcrypt.gensalt()).decode()
    with get_db_cursor() as cursor:
        cursor.execute(
            "INSERT INTO users (username, password, role) VALUES (%s, %s, %s)",
            (username, hashed_pw, role)
        )

def update_user_role(user_id, new_role):
    """Update an existing user's role."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "UPDATE users SET role = %s WHERE id = %s",
            (new_role, user_id)
        )

def update_user_password(user_id, hashed_pw):
    """Update an existing user's password (expects hashed)."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "UPDATE users SET password = %s WHERE id = %s",
            (hashed_pw, user_id)
        )

def delete_user(user_id):
    """Delete a user by ID."""
    with get_db_cursor() as cursor:
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))

def get_all_users():
    """Return list of (id, username, role)."""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT id, username, role FROM users")
        return cursor.fetchall()

# --- Pull-tags Helper Functions ---
def get_pulltag_rows(job_number, lot_number):
    """Fetch pulltags for a given job and lot, including transaction type."""
    query = """
    SELECT id, warehouse, item_code, description, quantity AS qty_req, uom,
           cost_code, status, transaction_type
    FROM pulltags
    WHERE job_number = %s AND lot_number = %s
    """

    with get_db_cursor() as cur:
        cur.execute(query, (job_number, lot_number))
        rows = cur.fetchall()
        
    return [{
        'id': i, 'warehouse': w, 'item_code': ic, 'description': desc,
        'qty_req': qty, 'uom': u, 'cost_code': cc, 'status': stt,
        'transaction_type': tt
    } for i, w, ic, desc, qty, u, cc, stt, tt in rows]


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


def insert_pulltag_line(cur, job_number, lot_number, item_code, quantity,
                        location, transaction_type="Job Issue"):
    """
    Inserts a new pulltag row using items_master metadata.
    Derives warehouse from the provided location.
    Forces status = 'pending', note = 'Added'.
    """
    # Lookup warehouse
    cur.execute("SELECT warehouse FROM locations WHERE location_code = %s", (location,))
    wh_result = cur.fetchone()
    if not wh_result:
        raise Exception(f"Unknown location '{location}': cannot resolve warehouse.")
    warehouse = wh_result[0]

    # Insert pulltag
    sql = """
    INSERT INTO pulltags
      (job_number, lot_number, item_code, quantity,
       description, cost_code, uom, status, transaction_type, note, warehouse)
    SELECT
      %s, %s, item_code, %s,
      item_description, cost_code, uom,
      'pending', %s, %s, %s
    FROM items_master
    WHERE item_code = %s
    RETURNING id
    """
    cur.execute(sql, (job_number, lot_number, quantity, transaction_type, "Added", warehouse, item_code))
    return cur.fetchone()[0]


def update_pulltag_line(cur, line_id, quantity, status="pending"):
    """
    Updates both quantity and status on an existing pulltag row.
    """
    sql = "UPDATE pulltags SET quantity = %s, status = %s WHERE id = %s"
    cur.execute(sql, (quantity, status, line_id))


def delete_pulltag_line(cur, line_id):
    """
    cur: psycopg2 cursor from get_db_cursor()
    Deletes a pulltag row by its id.
    """
    sql = "DELETE FROM pulltags WHERE id = %s"
    cur.execute(sql, (line_id,))


from fpdf import FPDF
import tempfile

def generate_finalize_summary_pdf(summary_data):
    import os
    import tempfile
    from fpdf import FPDF

    # Safe path in system temp directory
    output_path = os.path.join(tempfile.gettempdir(), "final_scan_summary.pdf")

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.cell(200, 10, txt="CRS Final Scan Summary Report", ln=True, align="C")
    pdf.ln(10)

    headers = ["Job", "Lot", "Item Code", "Qty", "Transaction", "Warehouse", "Location", "Scan ID"]
    col_widths = [20, 20, 30, 10, 30, 30, 30, 40]

    for i, header in enumerate(headers):
        pdf.cell(col_widths[i], 10, header, border=1)
    pdf.ln()

    for row in summary_data:
        for i, field in enumerate(headers):
            val = str(row.get(field.lower().replace(" ", "_"), ""))
            pdf.cell(col_widths[i], 10, val, border=1)
        pdf.ln()

    pdf.output(output_path)
    return output_path

def finalize_scans(scans_needed, scan_inputs, job_lot_queue, from_location, to_location=None,
                   scanned_by=None, progress_callback=None):
    """
    Process scans for Job Issues, Returns.
    - Inserts transactions, scan_verifications
    - Updates inventory, pulltags
    - Generates downloadable summary PDF
    """
    total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
    done = 0
    summary_rows = []

    with get_db_cursor() as cur:
        for item_code, lots in scans_needed.items():
            total_needed = sum(lots.values())

            for (job, lot), need in lots.items():
                assign = min(need, total_needed)
                if assign == 0:
                    continue

                if from_location and not to_location:
                    trans_type = "Job Issue"
                    loc_field, loc_value = "from_location", from_location
                    qty = assign
                elif to_location and not from_location:
                    trans_type = "Return"
                    loc_field, loc_value = "to_location", to_location
                    qty = abs(assign)
                else:
                    raise ValueError("finalize_scans requires exactly one of from/to_location")

                cur.execute("""
                    SELECT warehouse FROM pulltags
                    WHERE job_number = %s AND lot_number = %s AND item_code = %s
                    LIMIT 1
                """, (job, lot, item_code))
                warehouse = cur.fetchone()[0]
                sb = scanned_by

                cur.execute(f"""
                    INSERT INTO transactions (
                        transaction_type, date, warehouse, {loc_field},
                        job_number, lot_number, item_code, quantity, user_id
                    ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """, (trans_type, warehouse, loc_value, job, lot, item_code, qty, sb))

                for idx in range(1, qty + 1):
                    key = f"scan_{job}_{lot}_{item_code}_{idx}"
                    sid = scan_inputs.get(key, "").strip()
                    if not sid:
                        raise Exception(f"Missing scan ID for {item_code} #{idx} in {job}-{lot}")

                    cur.execute("""
                        SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s AND transaction_type = 'Job Issue'
                    """, (sid,))
                    issues = cur.fetchone()[0]
                    cur.execute("""
                        SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s AND transaction_type = 'Return'
                    """, (sid,))
                    returns = cur.fetchone()[0]

                    if trans_type == "Job Issue" and issues - returns > 0:
                        raise Exception(f"Scan {sid} already issued.")
                    elif trans_type == "Return" and issues > 0 and returns >= issues:
                        raise Exception(f"Scan {sid} already returned.")

                    cur.execute("""
                        INSERT INTO scan_verifications (
                            item_code, scan_id, job_number, lot_number,
                            scan_time, location, transaction_type, warehouse, scanned_by
                        ) VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                    """, (item_code, sid, job, lot, loc_value, trans_type, warehouse, sb))

                    if trans_type == "Return":
                        cur.execute("""
                            INSERT INTO current_scan_location (scan_id, item_code, location)
                            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                        """, (sid, item_code, loc_value))
                    else:
                        cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                    done += 1
                    if progress_callback:
                        pct = int(done / total_scans * 100)
                        progress_callback(pct)

                    summary_rows.append({
                        "job": job,
                        "lot": lot,
                        "item_code": item_code,
                        "qty": 1,
                        "transaction_type": trans_type,
                        "warehouse": warehouse,
                        "location": loc_value,
                        "scan_id": sid
                    })

                cur.execute("""
                    UPDATE pulltags
                    SET status = %s
                    WHERE job_number = %s AND lot_number = %s AND item_code = %s
                """, ('kitted' if trans_type == 'Job Issue' else 'returned', job, lot, item_code))

                delta = qty if trans_type == "Return" else -qty
                cur.execute("""
                    INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                    SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, (item_code, loc_value, delta, warehouse))

                total_needed -= qty
                if total_needed <= 0:
                    break
                
    # Finalize any untouched pulltags for this job/lot group
    with get_db_cursor() as cur:
        for job, lot in job_lot_queue:
            if from_location:
                cur.execute("""
                    UPDATE pulltags
                    SET status = 'kitted'
                    WHERE job_number = %s AND lot_number = %s AND transaction_type = 'Job Issue'
                """, (job, lot))
            elif to_location:
                cur.execute("""
                    UPDATE pulltags
                    SET status = 'returned'
                    WHERE job_number = %s AND lot_number = %s AND transaction_type = 'Return'
                """, (job, lot))

    generate_finalize_summary_pdf(summary_rows)
