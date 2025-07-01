import streamlit as st
from db import get_db_cursor

# --- App Metadata ---
APP_NAME = "Inventory Tracker"
DATE_FORMAT = "%Y-%m-%d"

# --- User Roles ---
USER_ROLES = ["admin", "user"]

# --- Warehouses & Locations ---
DEFAULT_LOCATIONS = ["00", "Test"]
STAGING_LOCATIONS = ["RECEIVING_STAGING", "TRANSFER_STAGING"]
WAREHOUSES = [
    "VVSOLAR", "VVSUNNOVA", "FNOSUNNOVA", "FNOSOLAR",
    "SACSOLAR", "SACSUNNOVA", "IESOLAR", "IEROOFING",
    "VALSOLAR", "VALSUNNOVA", "VVROOFING", "FNOROOFING"
]


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
MAX_ITEM_TYPES_IN_STAGING = 20

# --- Admin Override ---
# Stored in Streamlit secrets under [general] table
ADMIN_OVERRIDE_SECRET_KEY = st.secrets["general"]["admin_password"]
