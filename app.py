import streamlit as st
st.set_page_config(page_title="CRS Inventory Tracker", layout="wide")

from auth import login

# Import existing pages
import pages.submit_transaction     as submit_transaction
import pages.upload_init_csv        as upload_init_csv
import pages.reports                as reports
import pages.users                  as users
import pages.manage_locations       as manage_locations
import pages.scan_lookup            as scan_lookup

# Import new pages
import pages.pulltag_upload         as pulltag_upload
import pages.kitting                as kitting
# import pages.admin_bulk_export    as admin_bulk_export

# --- Run Login ---
login()

# --- Define Tabs ---
base_pages = [
    "Submit Transaction",
    "Reports",
    "Users",
    "Manage Locations",
    "Scan Lookup",
    "Kitting",
]

# Admin-only pages
admin_pages = [
    "Pull-tag Upload",
    # "Bulk Export",
    "Upload Init CSV",
]

# Combine pages based on role
if st.session_state.get('user_role') == 'admin':
    pages = base_pages + admin_pages
else:
    pages = base_pages

# Sidebar navigation
page = st.sidebar.selectbox("Navigation", pages)

# Route to the selected page
if page == "Submit Transaction":
    submit_transaction.run()
elif page == "Reports":
    reports.run()
elif page == "Users":
    users.run()
elif page == "Manage Locations":
    manage_locations.run()
elif page == "Scan Lookup":
    scan_lookup.run()
elif page == "Kitting":
    kitting.run()
elif page == "Pull-tag Upload":
    pulltag_upload.run()
elif page == "Upload Init CSV":
    upload_init_csv.run()
