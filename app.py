import time
import streamlit as st
st.set_page_config(page_title="CRS Inventory Tracker", layout="wide")
from auth import login

# Import existing pages
import pages.receiving              as receiving
import pages.upload_init_csv        as upload_init_csv
import pages.reports                as reports
import pages.users                  as users
import pages.manage_locations       as manage_locations
import pages.sage_r                 as sage_r
import pages.internal_movement      as internal_movement
import pages.landing                as landing
import pages.adjustments            as adjustments
import pages.pallet_tools           as pallet_tools
import pages.chat_ai                as chat_ai
import pages.pulltag_upload         as pulltag_upload
import pages.kitting                as kitting
import pages.testing                as testing
import pages.prewire                as prewire

# ──────────────────────────────────────────────────────────────────────────────
#  Minimal styling helper
# ──────────────────────────────────────────────────────────────────────────────

def apply_minimal_style() -> None:
    st.markdown("""
    <style>
    html, body {
        margin: 0;
        padding: 0;
        scroll-behavior: smooth;
        overflow-x: hidden;
    }

    div.stButton > button,
    div.stDownloadButton > button {
        background-color: #007b66;
        color: #fff;
        border-radius: 6px;
        padding: 0.4rem 1rem;
    }
    div.stButton > button:hover,
    div.stDownloadButton > button:hover {
        background-color: #009977;
    }

    input, select, textarea {
        background-color: #fff;
        color: #111;
    }

    [data-testid="stSidebarNav"] {
        display: none;
    }
    </style>
    """, unsafe_allow_html=True)

# --- Run Login ---
login()
if not st.session_state.get("user"):
    st.stop()

apply_minimal_style()

# --- Define Tabs ---
base_pages = [
    "Home",
    "Inventory",
    "Kitting",
    "Adjustments",
    "Pallet Tools",
    "Receiving",
    "Internal Movement",
    "Pull-tag Upload",
    "Sage Export",
    "Chat AI",
    "Pre-wire"
]

admin_pages = [
    "Upload Init CSV",
    "Manage Locations",
    "Users",
    "Testing",
]

user = st.session_state.get("user", "")
if st.session_state.get('role') == 'admin':
    pages = base_pages + [
        page for page in admin_pages if page != "Testing" or user == "lmoreno"
    ]
else:
    pages = base_pages

st.sidebar.title("📚 Menu")
choice = st.sidebar.radio("", pages)

# Route to the selected page
if choice == "Home":
    landing.run() 
elif choice == "Receiving":
    receiving.run()
elif choice == "Inventory":
    reports.run()
elif choice == "Users":
    users.run()
elif choice == "Manage Locations":
    manage_locations.run()
elif choice == "Sage Export":
    sage_r.run()
elif choice == "Kitting":
    kitting.run()
elif choice == "Pull-tag Upload":
    pulltag_upload.run()
elif choice == "Upload Init CSV":
    upload_init_csv.run()
elif choice == "Internal Movement":
    internal_movement.run()
elif choice == "Adjustments":
    adjustments.run()
elif choice == "Pallet Tools":
    pallet_tools.run()
elif choice == "Chat AI":
    chat_ai.run()
elif choice == "Pre-wire":
    prewire.run()
elif choice == "Testing":
    testing.run()
