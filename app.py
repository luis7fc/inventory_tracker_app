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


# Import new pages
import pages.pulltag_upload         as pulltag_upload
import pages.kitting                as kitting
# import pages.admin_bulk_export    as admin_bulk_export
# ──────────────────────────────────────────────────────────────────────────────
#  Global background helper
# ──────────────────────────────────────────────────────────────────────────────
import base64, pathlib

import base64, pathlib
import streamlit as st

def add_background(png_file: str) -> None:
    """Full-screen PNG background + white text + green buttons."""
    img_path = pathlib.Path(__file__).with_suffix("").parent / png_file
    with open(img_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()

    st.markdown(
        f"""
        <style>
        /* ───────────────────────── 1) BIG SIDEBAR TOGGLE ─────────────── */
        button[aria-label="Collapse sidebar"],
        button[aria-label="Expand sidebar"],
        button[data-testid="collapsedControl"] {{
            position: fixed !important;
            top: 0.75rem; left: 0.75rem;
            width: 40px !important; height: 40px !important;
            padding: 0 !important;
            background: rgba(10,14,30,0.85) !important;
            border: 2px solid #00D67A !important;
            border-radius: 50% !important;
            box-shadow: 0 0 6px rgba(0,0,0,0.6);
            z-index: 1003 !important;
        }}
        button[aria-label="Collapse sidebar"] svg,
        button[aria-label="Expand sidebar"]  svg,
        button[data-testid="collapsedControl"] svg {{
            width: 24px !important; height: 24px !important;
            stroke: #00D67A !important; stroke-width: 3;
        }}

        /* ────────────────────  SIDEBAR STYLE  ───────────────────── */
        div[data-testid="stSidebarNav"],
        section[data-testid="stSidebarNav"] {{            /* hide default nav */
            display: none !important;
        }}
        section[data-testid="stSidebar"],
        div[data-testid="stSidebar"] > div:first-child { /* tint bg */
            background: rgba(10,14,30,0.85) !important;
            backdrop-filter: blur(2px);
        }}


        /* ───────────────────────── 3) TOOLBARS & LAYOUT ─────────────── */
        [data-testid="stToolbar"],
        [data-testid="stHeader"] {{ background: transparent !important; box-shadow:none !important; }}
        html, body,
        [data-testid="stAppViewContainer"],
        .block-container {{ background: transparent !important; }}

        /* ───────────────────────── 4) GLOBAL TEXT COLOUR ────────────── */
        h1, h2, h3, p,
        [data-testid="stMetricValue"],
        [data-testid="stMetricLabel"] {{
            color: #FFFFFF !important;
            text-shadow: 0 0 4px rgba(0,0,0,0.6);
        }}

        /* ───────────────────────── 5) BUTTON THEME ──────────────────── */
        div.stButton > button,
        div.stDownloadButton > button,
        div.stForm > form button {{
            background-color: #00B868 !important;
            color: #FFFFFF !important;
            border: none !important;
            border-radius: 6px !important;
            padding: 0.5rem 1.2rem !important;
        }}
        div.stButton > button:hover,
        div.stDownloadButton > button:hover,
        div.stForm > form button:hover {{
            background-color: #00D67A !important;
        }}
        div.stButton > button:active,
        div.stDownloadButton > button:active,
        div.stForm > form button:active {{
            transform: translateY(1px);
        }}

        /* ───────────────────────── 6) HERO BACKGROUND ──────────────── */
        .bg-div {{
            position: fixed; inset: 0;
            background: url("data:image/png;base64,{b64}") no-repeat center top fixed !important;
            background-size: cover !important;
            z-index: 0 !important;
        }}
        </style>

        <div class="bg-div"></div>
        """,
        unsafe_allow_html=True,
    )


# --- Run Login ---
login()
if not st.session_state.get("user"):
    st.stop()

add_background("assets/logo.png")

# --- Define Tabs ---
base_pages = [
    "Home",
    "Receiving",
    "Reports",
    "Sage Export",
    "Kitting",
    "Internal Movement",
    "Adjustments",
    "Pull-tag Upload",
]

# Admin-only pages
admin_pages = [
    # "Bulk Export",
    "Upload Init CSV",
    "Manage Locations",
    "Users",
]

# Combine pages based on role
if st.session_state.get('role') == 'admin':
    pages = base_pages + admin_pages
else:
    pages = base_pages

page_names = pages


st.sidebar.title("📚 Menu")

choice = st.sidebar.radio("", pages)


#Route to the selected page
if choice == "Home":
    landing.run()
elif choice == "Receiving":
    receiving.run()
elif choice == "Reports":
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
