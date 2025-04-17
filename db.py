#DB Utilities script
# db.py - Database Utilities
import streamlit as st
import psycopg2
import bcrypt


@st.cache_resource
def get_db_connection():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"]
    )

# --- Get All Location Codes ---
def get_all_locations():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT location_code FROM locations")
            return [row[0] for row in cursor.fetchall()]

# --- Check If Location Exists ---
def validate_location_exists(location_code):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM locations WHERE location_code = %s", (location_code,))
            return cursor.fetchone() is not None

# --- Insert Inventory Transaction ---
def insert_transaction(transaction_data):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
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
            """, (
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
            ))
            conn.commit()

#Scan_verification_Insert logic
def insert_scan_verification(scan_data):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO scan_verifications (
                    item_code, job_number, lot_number,
                    scan_time, scan_id, location,
                    transaction_type, warehouse
                )
                VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s)
            """, (
                scan_data["item_code"],
                scan_data.get("job_number"),
                scan_data.get("lot_number"),
                scan_data["scan_id"],
                scan_data["location"],
                scan_data["transaction_type"],
                scan_data["warehouse"]
            ))
            conn.commit()

# --- Add a New Location ---
def add_location(location_code, warehouse, multi_item_allowed=False, description=None):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO locations (location_code, warehouse, multi_item_allowed, description)
                VALUES (%s, %s, %s, %s)
            """, (location_code, warehouse, multi_item_allowed, description))
            conn.commit()


# --- Delete a Location ---
def delete_location(location_code):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM locations WHERE location_code = %s", (location_code,))
            conn.commit()


# --- Reset Location Inventory (does NOT delete the location itself) ---
def reset_location(location_code):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM current_inventory
                WHERE location = %s
            """, (location_code,))
            conn.commit()


# --- Get Location Details ---
def get_location_details(location_code):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT location_code, warehouse, multi_item_allowed, description
                FROM locations
                WHERE location_code = %s
            """, (location_code,))
            return cursor.fetchone()

# ---Clear current inventory with init csv---
def clear_current_inventory():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM current_inventory")
            conn.commit()

# ---Bulk insert inventory---
def bulk_insert_inventory(rows):
    """
    Expects a list of dicts:
    [
        {"item_code": "JA605", "location": "A", "quantity": 36},
        ...
    ]
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany("""
                INSERT INTO current_inventory (item_code, location, quantity)
                VALUES (%s, %s, %s)
                ON CONFLICT (item_code, location) DO UPDATE
                SET quantity = current_inventory.quantity + EXCLUDED.quantity
            """, [
                (r["item_code"], r["location"], r["quantity"])
                for r in rows
            ])
            conn.commit()


#---insert inventory init log (rows)---
def insert_inventory_init_log(rows):
    """
    Expects a list of dicts:
    [
        {"item_code": "JA605", "location": "A", "quantity": 36, "scan_id": "xxxxx0"},
        ...
    ]
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany("""
                INSERT INTO inventory_init (item_code, location, quantity, scan_id)
                VALUES (%s, %s, %s, %s)
            """, [
                (r["item_code"], r["location"], r["quantity"], r["scan_id"])
                for r in rows
            ])
            conn.commit()
            
#---insert missing location---
def insert_location_if_missing(location_code, warehouse="VV"):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO locations (location_code, warehouse)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (location_code, warehouse))
            conn.commit()

#---Create new user with bcrypt hash---
def create_user(username, plain_password, role):
    hashed_pw = bcrypt.hashpw(plain_password.encode(), bcrypt.gensalt()).decode()
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO users (username, password, role)
                VALUES (%s, %s, %s)
            """, (username, hashed_pw, role))
            conn.commit()

#---Update_user_role---
def update_user_role(user_id, new_role):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE users
                SET role = %s
                WHERE id = %s
            """, (new_role, user_id))
            conn.commit()
            
#---Update_user_password---
def update_user_password(user_id, hashed_pw):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE users
                SET password = %s
                WHERE id = %s
            """, (hashed_pw, user_id))
            conn.commit()


#---Delete a User---
def delete_user(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
            conn.commit()
#---Get All users (for display tab)---
def get_all_users():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, username, role FROM users")
            return cursor.fetchall()
        
#----functions for inventory init csv----
def insert_inventory_init_row(conn, item_code, location, quantity, scan_id):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO inventory_init (item_code, location, quantity, scan_id)
            VALUES (%s, %s, %s, %s)
        """, (item_code, location, quantity, scan_id))
#fit 1
def upsert_current_inventory(conn, item_code, location, quantity):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO current_inventory (item_code, location, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (item_code, location) DO UPDATE
            SET quantity = current_inventory.quantity + EXCLUDED.quantity
        """, (item_code, location, quantity))

#FIT 2
def insert_location_if_not_exists(location_code, warehouse):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO locations (location_code, warehouse)
                VALUES (%s, %s)
                ON CONFLICT (location_code) DO NOTHING
            """, (location_code, warehouse))
            conn.commit()

#fit2...
def insert_inventory_init_row(conn, item_code, location, quantity):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO inventory_init (item_code, location, quantity)
            VALUES (%s, %s, %s)
        """, (item_code, location, quantity))
        conn.commit()
