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
import pages.pallet_tools           as pallet_tools
import pages.chat_ai                as chat_ai
import pages.pulltag_upload         as pulltag_upload
import pages.kitting                as kitting
import pages.testing                as testing
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

    button[aria-label="Collapse sidebar"],
    button[aria-label="Expand sidebar"],
    button[data-testid="collapsedControl"] {{
        position: fixed !important;
        top: 0.75rem;
        left: 0.75rem;
        width: 40px !important;
        height: 40px !important;
        background: rgba(10,14,30,0.85) !important;
        border: 2px solid #00D67A !important;
        border-radius: 50% !important;
        z-index: 1003 !important;
        font-family: 'Material Icons', sans-serif;
        font-size: 22px;
        color: #00D67A !important;
    }}
    
    button[aria-label="Collapse sidebar"]::after,
    button[aria-label="Expand sidebar"]::after {{
        content: "⮜"; /* fallback arrow, optional to override Material Icon */
        display: block;
        text-align: center;
        font-weight: bold;
        line-height: 40px;
        font-size: 24px;
        color: #00D67A;
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

    /* Hide Streamlit's default sidebar navigation (redundant page selectors) */
    [data-testid="stSidebarNav"] {{
        display: none !important; /* Hide default Streamlit page navigation */
    }}

    /* Ensure custom sidebar content (e.g., radio buttons) is visible */
    section[data-testid="stSidebar"] .stRadio {{
        display: block !important; /* Explicitly show radio buttons */
        margin-top: 1rem;
    }}

    /* Style sidebar title */
    section[data-testid="stSidebar"] h1 {{
        color: #FFFFFF !important;
        text-shadow: 0 0 4px rgba(0,0,0,0.6);
        margin-bottom: 1rem;
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
    "Inventory",
    "Kitting",
    "Adjustments",
    "Pallet Tools",
    "Receiving",
    "Internal Movement",
    "Pull-tag Upload",
    "Sage Export",
    "Chat AI",
]

# Admin-only pages
admin_pages = [
    "Upload Init CSV",
    "Manage Locations",
    "Users",
    "Testing",
]

# Combine pages based on role
user = st.session_state.get("user", "")
if st.session_state.get('role') == 'admin':
    pages = base_pages + [
        page for page in admin_pages if page != "Testing" or user == "lmoreno"
    ]
else:
    pages = base_pages

page_names = pages

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
elif choice == "Testing":
    testing.run()

