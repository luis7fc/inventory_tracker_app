"""
Initialize pages package and optionally re-export tab modules or shared constants.
"""

# Optionally expose tab modules for cleaner imports in app.py
from . import (
    submit_transaction,
    upload_init_csv,
    reports,
    users,
    manage_locations,
    scan_lookup
)

# Example shared constants (optional)
STAGING_LOCATIONS = ["00", "Test", "staging", "transfer_staging"]
