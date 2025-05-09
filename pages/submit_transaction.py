
import streamlit as st
import pandas as pd
import random
from datetime import datetime

from config import STAGING_LOCATIONS
from db import (
    get_db_cursor,
    insert_transaction,
    insert_scan_verification,
    validate_scan_for_transaction,
    update_scan_location,
    delete_scan_location
)

IRISH_TOASTS = [
    "☘️ Sláinte! Transaction submitted successfully!",
    "🍀 Luck o’ the Irish – you did it!",
    "🥃 Cheers, let’s grab a beer – transaction success!",
    "🌈 Pot of gold secured – job well done!",
    "🪙 May your inventory always balance – success!"
]

if st.session_state.get("transaction_success"):
    st.success(random.choice(IRISH_TOASTS))
    st.session_state["transaction_success"] = False

from config import STAGING_LOCATIONS
from db import get_db_cursor, insert_transaction, insert_scan_verification, validate_scan_for_transaction, update_scan_location, delete_scan_location

# --- Warehouse Selection ---
WAREHOUSE_OPTIONS = [
    "VVSOLAR","VVSUNNOVA","FNOSUNNOVA","FNOSOLAR",
    "SACSOLAR","SACSUNNOVA","IESOLAR","IEROOFING",
    "VALSOLAR","VALSUNNOVA","VVROOFING","FNOROOFING","IEROOFING"
]

# --- Helper Function ---
def get_target_location(transaction_type, from_loc, to_loc):
    if transaction_type in ["Receiving", "Return", "Manual Adjustment"]:
        return to_loc
    if transaction_type == "Internal Movement":
        return to_loc
    if transaction_type == "Job Issue":
        return from_loc
    return None


def run():
    st.header("🎞️ Submit Inventory Transaction")

    # Initialize state flags
    for flag in ("reset_scans", "reset_lots", "review_mode"):
        st.session_state.setdefault(flag, False)
    st.session_state.setdefault("scan_inputs", [])

    # --- Inputs ---
    transaction_type = st.selectbox(
        "Transaction Type",
        ["Receiving", "Internal Movement", "Job Issue", "Return", "Manual Adjustment"]
    )
    warehouse = st.selectbox(
        "Select Warehouse",
        options=WAREHOUSE_OPTIONS,
        help="Which warehouse is this transaction from?",
        key="warehouse"
    )
    item_code = st.text_input("Item Code", key="item_code")
    pallet_qty = 1
    po_number = None
    note = ""

    # Branch-specific inputs
    if transaction_type == "Manual Adjustment":
        total_qty = st.number_input("Total Quantity (+/-)", step=1, value=0)
        from_location = None
        to_location = st.text_input("Location", key="to_location")
        note = st.text_area("Adjustment Note", key="note")

    elif transaction_type == "Job Issue":
        job_number = st.text_input("Job Number", key="job_number")
        from_location = st.text_input("Issue From Location", key="from_location")
        if st.session_state.reset_lots:
            for i in range(st.session_state.get("num_lots", 0)):
                st.session_state.pop(f"lot_{i}", None)
                st.session_state.pop(f"lot_qty_{i}", None)
            st.session_state.reset_lots = False
        num_lots = st.number_input("Total Lots", min_value=1, step=1, key="num_lots")
        for i in range(num_lots):
            st.text_input(f"Lot {i+1} Number", key=f"lot_{i}")
            st.number_input(f"Quantity for Lot {i+1}", min_value=0, step=1, key=f"lot_qty_{i}")
        total_qty = sum(
            st.session_state.get(f"lot_qty_{i}", 0)
            for i in range(num_lots)
        )
        st.write(f"**Total Qty (sum of lots):** {total_qty}")
        pallet_qty = st.number_input("Pallet Quantity", min_value=1, value=1, step=1)
        to_location = None

    elif transaction_type == "Return":
        # Mirror Job Issue inputs for Return
        job_number = st.text_input("Job Number", key="job_number")
        from_location = st.text_input("Return From Location", key="from_location")
        if st.session_state.reset_lots:
            for i in range(st.session_state.get("num_lots", 0)):
                st.session_state.pop(f"lot_{i}", None)
                st.session_state.pop(f"lot_qty_{i}", None)
            st.session_state.reset_lots = False
        num_lots = st.number_input("Total Lots", min_value=1, step=1, key="num_lots")
        for i in range(num_lots):
            st.text_input(f"Lot {i+1} Number", key=f"lot_{i}")
            st.number_input(f"Quantity for Lot {i+1}", min_value=0, step=1, key=f"lot_qty_{i}")
        total_qty = sum(
            st.session_state.get(f"lot_qty_{i}", 0)
            for i in range(num_lots)
        )
        st.write(f"**Total Qty to Return (sum of lots):** {total_qty}")
        pallet_qty = st.number_input("Pallet Quantity", min_value=1, value=1, step=1)
        to_location = from_location  # for Return, to_location = original from_location

    else:
        # Receiving or Internal Movement
        total_qty = st.number_input("Total Quantity", step=1, key="total_qty")
        pallet_qty = st.number_input("Pallet Quantity", min_value=1, value=1, step=1, key="pallet_qty")
        if transaction_type == "Receiving":
            po_number = st.text_input("PO Number", key="po_number")
            to_location = st.text_input("Receiving Location", key="to_location")
            from_location = None
        elif transaction_type == "Internal Movement":
            from_location = st.text_input("From Location", key="from_location")
            to_location = st.text_input("To Location", key="to_location")

    # --- Scans ---
    if transaction_type not in ["Manual Adjustment"]:
        expected_scans = total_qty // max(pallet_qty,1)
        st.write(f"**Expected Scans:** {expected_scans}")
        if st.session_state.reset_scans:
            for i in range(expected_scans):
                st.session_state.pop(f"scan_{i}", None)
            st.session_state.reset_scans = False
        st.session_state.scan_inputs = []
        for i in range(expected_scans):
            scan = st.text_input(f"Scan {i+1}", key=f"scan_{i}")
            st.session_state.scan_inputs.append(scan)

    # --- Review & Submit ---
    if not st.session_state.review_mode:
        if st.button("Review Transaction"):
            if transaction_type not in ["Manual Adjustment"] and len(st.session_state.scan_inputs) != expected_scans:
                st.error("Scan count must match expected scan count.")
                st.stop()
            # prepare lot assignments for Job Issue/Return
            if transaction_type in ["Job Issue", "Return"]:
                lot_inputs = []
                for i in range(st.session_state.num_lots):
                    lot_inputs.append({
                        "lot_number": st.session_state.get(f"lot_{i}"),
                        "qty": st.session_state.get(f"lot_qty_{i}",0)
                    })
                assignments = []
                ptr = 0
                for lot in lot_inputs:
                    for _ in range(lot["qty"] // pallet_qty):
                        assignments.append({
                            "scan": st.session_state.scan_inputs[ptr],
                            "lot_number": lot["lot_number"]
                        })
                        ptr += 1
                st.session_state.assignments = assignments
                st.session_state.lot_inputs = lot_inputs
            st.session_state.review_mode = True
            st.rerun()

    else:
        st.subheader("🔎 Review Summary")
        st.write("**Type:**", transaction_type)
        st.write("**Item:**", item_code)
        st.write("**Qty:**", total_qty)
        st.write("**Warehouse:**", warehouse)
        if transaction_type in ["Job Issue", "Return"]:
            st.write("**From Location:**", from_location)
            st.write("**Lots:**")
            st.dataframe(st.session_state.assignments)
        else:
            st.write("**From/To:**", from_location, "/", to_location)
            if transaction_type == "Receiving":
                st.write("**PO:**", po_number)
            if transaction_type == "Manual Adjustment":
                st.write("**Note:**", note)
            if transaction_type not in ["Manual Adjustment"] and transaction_type not in ["Return"]:
                st.write("**Scans:**")
                st.code("\n".join(st.session_state.scan_inputs))

        if st.button("Confirm and Submit"):
            with get_db_cursor() as cursor:
                bypassed_warning = False
                # Inventory updates
                if transaction_type == "Internal Movement":
                    # subtract then add
                    cursor.execute(
                        "UPDATE current_inventory SET quantity = quantity - %s WHERE warehouse=%s AND location=%s AND item_code=%s",
                        (total_qty, warehouse, from_location, item_code)
                    )
                    cursor.execute(
                        "INSERT INTO current_inventory (warehouse, location, item_code, quantity) VALUES (%s,%s,%s,%s) ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity",
                        (warehouse, to_location, item_code, total_qty)
                    )
                elif transaction_type == "Receiving":
                    cursor.execute(
                        "INSERT INTO current_inventory (warehouse, location, item_code, quantity) VALUES (%s,%s,%s,%s) ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity",
                        (warehouse, to_location, item_code, total_qty)
                    )
                elif transaction_type == "Job Issue":
                    cursor.execute(
                        "UPDATE current_inventory SET quantity = quantity - %s WHERE warehouse=%s AND location=%s AND item_code=%s",
                        (total_qty, warehouse, from_location, item_code)
                    )
                elif transaction_type == "Return":
                    # mirror job issue: add back
                    cursor.execute(
                        "INSERT INTO current_inventory (warehouse, location, item_code, quantity) VALUES (%s,%s,%s,%s) ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity",
                        (warehouse, from_location, item_code, total_qty)
                    )
                elif transaction_type == "Manual Adjustment":
                    cursor.execute(
                        "INSERT INTO current_inventory (warehouse, location, item_code, quantity) VALUES (%s,%s,%s,%s) ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity = EXCLUDED.quantity",
                        (warehouse, to_location, item_code, total_qty)
                    )
                # Multi-item guard
                target_loc = get_target_location(transaction_type, from_location, to_location)
                cursor.execute(
                    "SELECT multi_item_allowed FROM locations WHERE location_code=%s AND warehouse=%s",
                    (target_loc, warehouse)
                )
                result = cursor.fetchone()
                is_multi_item = bool(result and result[0])
                if transaction_type != "Manual Adjustment" and not is_multi_item:
                    cursor.execute(
                        "SELECT item_code FROM current_inventory WHERE warehouse=%s AND location=%s AND quantity>0",
                        (warehouse, target_loc)
                    )
                    existing = [r[0] for r in cursor.fetchall()]
                    if existing and any(ic != item_code for ic in existing):
                        st.error(f"Location '{target_loc}' already holds a different item.")
                        st.stop()

            # Insert transactions & scans
            if transaction_type in ["Job Issue", "Return"]:
                for lot in st.session_state.lot_inputs:
                    qty = lot["qty"]
                    insert_transaction({
                        "transaction_type": transaction_type,
                        "item_code": item_code,
                        "quantity": qty,
                        "job_number": job_number,
                        "lot_number": lot["lot_number"],
                        "po_number": po_number,
                        "from_location": from_location,
                        "to_location": None,
                        "from_warehouse": None,
                        "to_warehouse": None,
                        "user_id": st.session_state.user,
                        "bypassed_warning": bypassed_warning,
                        "note": note,
                        "warehouse": warehouse
                    })
                    for assign in st.session_state.assignments:
                        if assign["lot_number"] == lot["lot_number"]:
                            insert_scan_verification({
                                "item_code": item_code,
                                "job_number": job_number,
                                "lot_number": lot["lot_number"],
                                "scan_id": assign["scan"],
                                "location": target_loc,
                                "transaction_type": transaction_type,
                                "warehouse": warehouse,
                                "scanned_by": st.session_state.user
                            })
            else:
                insert_transaction({
                    "transaction_type": transaction_type,
                    "item_code": item_code,
                    "quantity": total_qty,
                    "job_number": st.session_state.get("job_number",""),
                    "lot_number": st.session_state.get("lot_number",""),
                    "po_number": po_number,
                    "from_location": from_location,
                    "to_location": to_location,
                    "from_warehouse": None,
                    "to_warehouse": None,
                    "user_id": st.session_state.user,
                    "bypassed_warning": False,
                    "note": note,
                    "warehouse": warehouse
                })
                if transaction_type != "Manual Adjustment":
                    for scan in st.session_state.scan_inputs:
                        insert_scan_verification({
                            "item_code": item_code,
                            "job_number": st.session_state.get("job_number",""),
                            "lot_number": st.session_state.get("lot_number",""),
                            "scan_id": scan,
                            "location": target_loc,
                            "transaction_type": transaction_type,
                            "warehouse": warehouse,
                            "scanned_by": st.session_state.user
                        })

            st.success("Transaction submitted and recorded. 🍀")
            # Reset state
            st.session_state.review_mode = False
            st.session_state.scan_inputs = []
            st.session_state.reset_scans = True
            st.session_state.reset_lots = True
            st.rerun()

        if st.button("Cancel Review"):
            st.session_state.review_mode = False
            st.rerun()

        # Update scan location records
        if transaction_type in ["Receiving", "Internal Movement", "Return"]:
            for scan_id in st.session_state.scan_inputs:
                update_scan_location(
                transaction_type=transaction_type,
                job_number=job_number,cursor, scan_id, item_code, to_location),
                    (scan_id, item_code, to_location)
                )
        elif transaction_type == "Job Issue":
            for scan_id in st.session_state.scan_inputs:
                delete_scan_location(cursor, scan_id)
                )
