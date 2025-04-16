import streamlit as st
st.set_page_config(page_title="Inventory Tracker", layout="wide")

from auth import login
from pages import (
    submit_transaction,
    upload_init_csv,
    reports,
    users,
    manage_locations,
    scan_lookup
)

# --- Run Login ---
login()

# --- Define Tabs ---
pages = ["Submit Transaction", "Reports", "Users", "Manage Locations", "Scan Lookup"]
if st.session_state.role == "admin":
    pages.insert(1, "Upload Init CSV")  # Only show for admins

selected_tab = st.sidebar.radio("Navigate", pages, key="main_navigation")

# --- Route Tabs ---
if selected_tab == "Submit Transaction":
    submit_transaction.run()
elif selected_tab == "Upload Init CSV":
    upload_init_csv.run()
elif selected_tab == "Reports":
    reports.run()
elif selected_tab == "Users":
    users.run()
elif selected_tab == "Manage Locations":
    manage_locations.run()
elif selected_tab == "Scan Lookup":
    scan_lookup.run()
