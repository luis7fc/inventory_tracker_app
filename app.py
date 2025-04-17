import streamlit as st
st.set_page_config(page_title="Inventory Tracker", layout="wide")

from auth import login
import pages.submit_transaction as submit_transaction
import pages.upload_init_csv as upload_init_csv
import pages.reports as reports
import pages.users as users
import pages.manage_locations as manage_locations
import pages.scan_lookup as scan_lookup

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
