import streamlit as st
st.set_page_config(page_title="Inventory Tracker", layout="wide")

from auth import login

# Import existing pages
import pages.submit_transaction as submit_transaction
import pages.upload_init_csv     as upload_init_csv
import pages.reports             as reports
import pages.users               as users
import pages.manage_locations    as manage_locations
import pages.scan_lookup         as scan_lookup

# Import new pages
import pages.pulltag_upload      as pulltag_upload
import pages.job_kitting         as job_kitting
import pages.admin_bulk_export   as admin_bulk_export

# --- Run Login ---
login()

# --- Define Tabs ---
base_pages = [
    "Submit Transaction",
    "Reports",
    "Users",
    "Manage Locations",
    "Scan Lookup",
]

# Admin‐only pages
admin_pages = [
    "Pull-tag Upload",
    "Bulk Export",
    "Upload Init CSV",
]

# Warehouse (or any role) page
kitting_pages = [
    "Job Kitting",
]

pages = []
pages.append("Submit Transaction")

# Only admins get upload/export pages
if st.session_state.role == "admin":
    pages += admin_pages

# Everyone (or you can restrict by another role) gets Job Kitting
pages += kitting_pages

# Then the rest
pages += [
    "Reports",
    "Users",
    "Manage Locations",
    "Scan Lookup",
]

selected_tab = st.sidebar.radio("Navigate", pages, key="main_navigation")

# --- Route Tabs ---
if selected_tab == "Submit Transaction":
    submit_transaction.run()

elif selected_tab == "Pull-tag Upload":
    pulltag_upload.run()

elif selected_tab == "Job Kitting":
    job_kitting.run()

elif selected_tab == "Bulk Export":
    admin_bulk_export.run()

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
