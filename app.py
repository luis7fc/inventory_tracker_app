import streamlit as st
st.set_page_config(page_title="CRS Inventory Tracker", layout="wide")
from auth import login

# Import existing pages
import pages.receiving              as receiving
import pages.upload_init_csv        as upload_init_csv
import pages.reports                as reports
import pages.users                  as users
import pages.manage_locations       as manage_locations
import pages.scan_lookup            as scan_lookup
import pages.internal_movement      as internal_movement
import pages.landing                as landing
import pages.ad_on                  as add_on

# Import new pages
import pages.pulltag_upload         as pulltag_upload
import pages.kitting                as kitting
# import pages.admin_bulk_export    as admin_bulk_export

# --- Run Login ---
login()

# --- Define Tabs ---
base_pages = [
    "Home",
    "Receiving",
    "Reports",
    "Users",
    "Manage Locations",
    "Scan Lookup",
    "Kitting",
    "Internal Movement",
    "Add-On",
]

# Admin-only pages
admin_pages = [
    "Pull-tag Upload",
    # "Bulk Export",
    "Upload Init CSV",
]

# Combine pages based on role
if st.session_state.get('role') == 'admin':
    pages = base_pages + admin_pages
else:
    pages = base_pages

page_names = pages

st.sidebar.image("assets/logoc.png", use_container_width=True)
choice = st.sidebar.selectbox("🔍 Navigate", page_names, index=page_names.index("Home"))
#Map labels to module



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
elif choice == "Scan Lookup":
    scan_lookup.run()
elif choice == "Kitting":
    kitting.run()
elif choice == "Pull-tag Upload":
    pulltag_upload.run()
elif choice == "Upload Init CSV":
    upload_init_csv.run()
elif choice == "Internal Movement":
    internal_movement.run()
elif choice == "Add-On"
    add_on.run()

