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


def validate_scan_for_transaction(cursor, scan_id, item_code, transaction_type, from_location, to_location, job_number=None):
    if transaction_type in ["Job Issue", "Kitting"]:
        cursor.execute("SELECT location FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        result = cursor.fetchone()
        if result:
            raise ValueError(f"Scan ID {scan_id} already exists and was already issued or in use.")
        return

    if transaction_type == "Receiving":
        cursor.execute("SELECT 1 FROM current_scan_location WHERE scan_id = %s", (scan_id,))
        if cursor.fetchone():
            raise ValueError(f"Scan ID {scan_id} already exists in the system.")
        return

    cursor.execute("SELECT location, item_code FROM current_scan_location WHERE scan_id = %s", (scan_id,))
    result = cursor.fetchone()
    if not result:
        raise ValueError(f"Scan ID {scan_id} not found in system.")
    actual_location, actual_item = result
    if actual_item != item_code:
        raise ValueError(f"Scan ID {scan_id} belongs to item {actual_item}, not {item_code}.")
    if transaction_type in ["Internal Movement"] and actual_location != from_location:
        raise ValueError(f"Scan ID {scan_id} is in {actual_location}, not in {from_location}.")
    if transaction_type == "Return" and actual_location != to_location:
        raise ValueError(f"Scan ID {scan_id} is in {actual_location}, not return location {to_location}.")
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
        ON CONFLICT (scan_id) DO UPDATE
        SET location = EXCLUDED.location, updated_at = now()
        """,
        (scan_id, item_code, location)
    )

def delete_scan_location(cursor, scan_id):
    cursor.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (scan_id,))

# --- Pull-tags Operations --- (future integration)
# insert, update and query pulltags as needed

# --- Locations Management ---

def add_location(location_code, warehouse, multi_item_allowed=False, description=None):
    with get_db_cursor() as cursor:
        cursor.execute(
            "INSERT INTO locations (location_code, warehouse, multi_item_allowed, description)"
            " VALUES (%s, %s, %s, %s)",
            (location_code, warehouse, multi_item_allowed, description)
        )


def delete_location(location_code):
    with get_db_cursor() as cursor:
        cursor.execute(
            "DELETE FROM locations WHERE location_code = %s",
            (location_code,)
        )


def reset_location(location_code):
    with get_db_cursor() as cursor:
        cursor.execute(
            "DELETE FROM current_inventory WHERE location = %s",
            (location_code,)
        )


def get_location_details(location_code):
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT location_code, warehouse, multi_item_allowed, description"
            " FROM locations WHERE location_code = %s",
            (location_code,)
        )
        return cursor.fetchone()

# --- Inventory Initialization ---

def clear_current_inventory():
    with get_db_cursor() as cursor:
        cursor.execute("DELETE FROM current_inventory")


def bulk_insert_inventory(rows):
    with get_db_cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO current_inventory (item_code, location, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (item_code, location) DO UPDATE
            SET quantity = current_inventory.quantity + EXCLUDED.quantity
            """,
            [(r["item_code"], r["location"], r["quantity"]) for r in rows]
        )


def insert_inventory_init_log(rows):
    with get_db_cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO inventory_init (item_code, location, quantity, scan_id)
            VALUES (%s, %s, %s, %s)
            """,
            [(r["item_code"], r["location"], r["quantity"], r.get("scan_id")) for r in rows]
        )

# --- User Management ---

def create_user(username, plain_password, role):
    hashed_pw = bcrypt.hashpw(plain_password.encode(), bcrypt.gensalt()).decode()
    with get_db_cursor() as cursor:
        cursor.execute(
            "INSERT INTO users (username, password, role) VALUES (%s, %s, %s)",
            (username, hashed_pw, role)
        )


def update_user_role(user_id, new_role):
    with get_db_cursor() as cursor:
        cursor.execute(
            "UPDATE users SET role = %s WHERE id = %s",
            (new_role, user_id)
        )


def update_user_password(user_id, hashed_pw):
    with get_db_cursor() as cursor:
        cursor.execute(
            "UPDATE users SET password = %s WHERE id = %s",
            (hashed_pw, user_id)
        )


def delete_user(user_id):
    with get_db_cursor() as cursor:
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))


def get_all_users():
    with get_db_cursor() as cursor:
        cursor.execute("SELECT id, username, role FROM users")
        return cursor.fetchall()
