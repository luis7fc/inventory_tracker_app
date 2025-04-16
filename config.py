#Configuration Structure Script
import streamlit as st
import psycopg2

# --- Shared DB Connection ---
def get_db_connection():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"]
    )

# --- App Metadata ---
APP_NAME = "Inventory Tracker"
DATE_FORMAT = "%Y-%m-%d"

# --- User Roles ---
USER_ROLES = ["admin", "user"]

# --- Warehouses & Locations ---
DEFAULT_LOCATIONS = ["00", "Test"]
STAGING_LOCATIONS = ["STAGING", "TRANSFER_STAGING"]
WAREHOUSES = ["VV", "SAC", "FNO", "Main"]

# --- Transaction Types ---
TRANSACTION_TYPES = [
    "Receiving",
    "Internal Movement",
    "Job Issue",
    "Return",
    "Manual Adjustment"
]

# --- Inventory Defaults ---
DEFAULT_PALLET_QUANTITY = 1
MAX_ITEM_TYPES_IN_STAGING = 10

# --- Admin Override (if still using secret key fallback) ---
ADMIN_OVERRIDE_SECRET_KEY = "admin_override_password"
