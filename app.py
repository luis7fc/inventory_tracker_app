import time
import streamlit as st
st.set_page_config(page_title="CRS Inventory Tracker", layout="wide")
from auth import login
import base64
import pathlib

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

def add_background(png_file: str) -> None:
    """Unified background + scroll + contrast-safe theming."""
    if not st.session_state.get("show_bg", True):
        return

    img_path = pathlib.Path(__file__).with_suffix("").parent / png_file
    with open(img_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()

    st.markdown(f"""
    <style>
    /* Root scroll fix with smooth scrolling */
    html, body {{
        height: 100%;
        margin: 0;
        overflow-y: auto !important;
        overflow-x: hidden !important;
        scroll-behavior: smooth !important; /* Enable smooth scrolling */
    }}

    /* Main scrollable layout */
    section.main {{
        min-height: 100vh !important;
        height: auto !important;
        overflow-y: auto !important; /* Use auto to prevent unnecessary scrollbars */
        scroll-behavior: smooth !important; /* Smooth scrolling for main content */
        position: relative;
        z-index: 1;
        padding-bottom: 20px;
    }}
    .block-container {{
        min-height: 100%;
        background: transparent !important;
        box-sizing: border-box;
    }}

    /* Sidebar controls */
    button[aria-label="Collapse sidebar"],
    button[aria-label="Expand sidebar"],
    button[data-testid="collapsedControl"] {{
        position: fixed !important;
        top: 0.75rem; left: 0.75rem;
        width: 40px !important; height: 40px !important;
        background: rgba(10,14,30,0.85) !important;
        border: 2px solid #00D67A !important;
        border-radius: 50% !important;
        z-index: 1003 !important;
    }}
    button svg {{
        stroke: #00D67A !important;
        stroke-width: 3;
    }}

    section[data-testid="stSidebar"],
    div[data-testid="stSidebar"] > div:first-child {{
        background: rgba(10,14,30,0.85) !important;
        backdrop-filter: blur(2px);
    }}

    /* Hide sidebar navigation (redundant page selections) */
    [data-testid="stSidebarNav"],
    [data-testid="stSidebarNavItems"] {{
        display: none !important; /* Hide sidebar navigation */
    }}

    /* Hide sidebar section title (duplicated page label) */
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] [data-testid="stSidebarNav"] + div {{
        display: none !important; /* Additional selector for robustness */
    }}

    /* Global readable text */
    input, select, textarea, label {{
        color: #111 !important;
    }}
    h1, h2, h3, p,
    [data-testid="stMetricValue"],
    [data-testid="stMetricLabel"] {{
        color: #FFFFFF !important;
        text-shadow: 0 0 4px rgba(0,0,0,0.6);
    }}

    /* Fix dark-on-dark metric values */
    [data-testid="stMetric"] * {{
        color: #fff !important;
    }}

    /* Input contrast fix */
    .stTextInput input,
    .stNumberInput input,
    .stSelectbox select,
    textarea {{
        background-color: #f7f7f7 !important;
        color: #111 !important;
        border-radius: 4px;
    }}

    /* Button styling */
    div.stButton > button,
    div.stDownloadButton > button,
    div.stForm > form button {{
        background-color: #00B868 !important;
        color: #FFFFFF !important;
        border-radius: 6px !important;
        padding: 0.5rem 1.2rem !important;
    }}
    div.stButton > button:hover,
    div.stDownloadButton > button:hover,
    div.stForm > form button:hover {{
        background-color: #00D67A !important;
    }}

    /* Scrollbar visibility */
    ::-webkit-scrollbar {{
        width: 14px;
        height: 14px;
    }}
    ::-webkit-scrollbar-track {{
        background: #f5f5f5;
        border-radius: 8px;
    }}
    ::-webkit-scrollbar-thumb {{
        background: #666;
        border-radius: 8px;
        border: 3px solid #f5f5f5;
    }}
    ::-webkit-scrollbar-thumb:hover {{
        background: #444;
    }}

    /* Background image layer */
    .bg-div {{
        position: fixed; inset: 0;
        background: url("data:image/png;base64,{b64}") no-repeat center top fixed !important;
        background-size: cover !important;
        z-index: 0 !important;
    }}
    </style>
    <div class="bg-div"></div>
    """, unsafe_allow_html=True)

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
