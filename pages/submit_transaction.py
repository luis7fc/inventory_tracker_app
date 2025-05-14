import streamlit as st
import pandas as pd
import random
from datetime import datetime
from typing import Optional, List, Dict

from config import STAGING_LOCATIONS
from db import (
    get_db_cursor,
    insert_transaction,
    insert_scan_verification,
    validate_scan_for_transaction,
    update_scan_location,
    delete_scan_location,
)

# Constants
IRISH_TOASTS = [
    "☘️ Sláinte! Transaction submitted successfully!",
    "🍀 Luck o’ the Irish – you did it!",
    "🥃 Cheers, let’s grab a beer – transaction success!",
    "🌈 Pot of gold secured – job well done!",
    "🪙 May your inventory always balance – success!"
]

WAREHOUSE_OPTIONS = [
    "VVSOLAR", "VVSUNNOVA", "FNOSUNNOVA", "FNOSOLAR",
    "SACSOLAR", "SACSUNNOVA", "IESOLAR", "IEROOFING",
    "VALSOLAR", "VALSUNNOVA", "VVROOFING", "FNOROOFING", "IEROOFING"
]

# Helper Functions
def get_target_location(transaction_type: str, from_loc: Optional[str], to_loc: Optional[str]) -> Optional[str]:
    """Determine the target location based on transaction type."""
    if transaction_type in ["Receiving", "Return", "Manual Adjustment"]:
        return to_loc
    if transaction_type == "Internal Movement":
        return to_loc
    if transaction_type == "Job Issue":
        return from_loc
    return None

def validate_inputs(transaction_type: str, item_code: str, total_qty: int, warehouse: str, 
                   from_location: Optional[str], to_location: Optional[str], 
                   scan_inputs: List[str], expected_scans: int) -> tuple[bool, str]:
    """Validate user inputs before processing."""
    if not item_code:
        return False, "Item Code is required."
    if total_qty <= 0 and transaction_type != "Manual Adjustment":
        return False, "Total Quantity must be greater than 0."
    if transaction_type not in ["Manual Adjustment"] and len([s for s in scan_inputs if s]) != expected_scans:
        return False, f"Expected {expected_scans} scans, but received {len([s for s in scan_inputs if s])}."
    if transaction_type in ["Internal Movement"] and from_location == to_location:
        return False, "From and To locations cannot be the same for Internal Movement."
    return True, ""

def validate_scans(cursor, scan_inputs: List[str], item_code: str, transaction_type: str,
                  from_location: Optional[str], to_location: Optional[str], job_number: Optional[str]) -> tuple[bool, str]:
    """Validate scans using validate_scan_for_transaction."""
    for scan_id in scan_inputs:
        if scan_id:
            try:
                validate_scan_for_transaction(cursor, scan_id, item_code, transaction_type,
                                            from_location, to_location, job_number)
            except ValueError as e:
                return False, str(e)
    return True, ""

def run():
    st.set_page_config(page_title="Inventory Transaction", layout="wide")
    st.header("🎞️ Submit Inventory Transaction")

    # Initialize session state
    if "transaction_success" not in st.session_state:
        st.session_state.transaction_success = False
    for flag in ("reset_scans", "reset_lots", "review_mode", "scan_inputs", "assignments", "lot_inputs"):
        st.session_state.setdefault(flag, [] if flag in ["scan_inputs", "assignments", "lot_inputs"] else False)

    # Display success message
    if st.session_state.transaction_success:
        st.success(random.choice(IRISH_TOASTS))
        st.session_state.transaction_success = False

    # Form
    with st.form("transaction_form"):
        # Inputs
        col1, col2 = st.columns(2)
        with col1:
            transaction_type = st.selectbox("Transaction Type", 
                                          ["Receiving", "Internal Movement", "Job Issue", "Return", "Manual Adjustment"],
                                          key="transaction_type")
            warehouse = st.selectbox("Warehouse", options=WAREHOUSE_OPTIONS, key="warehouse")
            item_code = st.text_input("Item Code", key="item_code")
        
        with col2:
            pallet_qty = st.number_input("Pallet Quantity", min_value=1, value=1, step=1, key="pallet_qty")
            if transaction_type == "Receiving":
                po_number = st.text_input("PO Number", key="po_number")
            elif transaction_type in ["Job Issue", "Return"]:
                job_number = st.text_input("Job Number", key="job_number")

        # Transaction-specific inputs
        total_qty = 0
        from_location = None
        to_location = None
        job_number = None
        po_number = None
        note = ""

        if transaction_type == "Manual Adjustment":
            total_qty = st.number_input("Total Quantity (+/-)", step=1, value=0, key="total_qty")
            to_location = st.text_input("Location", key="to_location")
            note = st.text_area("Adjustment Note", key="note")
        
        elif transaction_type in ["Job Issue", "Return"]:
            from_location = st.text_input("Location", key="from_location")
            if st.session_state.reset_lots:
                st.session_state.lot_inputs = []
                st.session_state.reset_lots = False
            
            num_lots = st.number_input("Total Lots", min_value=1, step=1, key="num_lots")
            lot_inputs = []
            for i in range(num_lots):
                lot_col1, lot_col2 = st.columns(2)
                with lot_col1:
                    lot_number = st.text_input(f"Lot {i+1} Number", key=f"lot_{i}")
                with lot_col2:
                    lot_qty = st.number_input(f"Quantity for Lot {i+1}", min_value=0, step=1, key=f"lot_qty_{i}")
                lot_inputs.append({"lot_number": lot_number, "qty": lot_qty})
            total_qty = sum(lot["qty"] for lot in lot_inputs)
            st.write(f"**Total Qty (sum of lots):** {total_qty}")
            st.session_state.lot_inputs = lot_inputs
            to_location = from_location if transaction_type == "Return" else None
        
        else:  # Receiving or Internal Movement
            total_qty = st.number_input("Total Quantity", min_value=1, step=1, key="total_qty")
            if transaction_type == "Receiving":
                to_location = st.text_input("Receiving Location", key="to_location")
            else:  # Internal Movement
                from_location = st.text_input("From Location", key="from_location")
                to_location = st.text_input("To Location", key="to_location")

        # Scans
        scan_inputs = []
        if transaction_type != "Manual Adjustment":
            expected_scans = total_qty // max(pallet_qty, 1)
            st.write(f"**Expected Scans:** {expected_scans}")
            if st.session_state.reset_scans:
                st.session_state.scan_inputs = []
                st.session_state.reset_scans = False
            
            scan_cols = st.columns(3)
            for i in range(expected_scans):
                with scan_cols[i % 3]:
                    scan = st.text_input(f"Scan {i+1}", key=f"scan_{i}")
                    scan_inputs.append(scan)
            st.session_state.scan_inputs = scan_inputs

        # Review Button
        submitted = st.form_submit_button("Review Transaction")

    # Review Logic
    if submitted and not st.session_state.review_mode:
        is_valid, error_msg = validate_inputs(transaction_type, item_code, total_qty, warehouse, 
                                           from_location, to_location, scan_inputs, expected_scans)
        if not is_valid:
            st.error(error_msg)
            return

        # Validate scans
        if transaction_type != "Manual Adjustment":
            with get_db_cursor() as cursor:
                is_valid, error_msg = validate_scans(cursor, scan_inputs, item_code, transaction_type,
                                                  from_location, to_location, job_number)
                if not is_valid:
                    st.error(error_msg)
                    return

        # Prepare lot assignments for Job Issue/Return
        if transaction_type in ["Job Issue", "Return"]:
            assignments = []
            ptr = 0
            for lot in st.session_state.lot_inputs:
                for _ in range(lot["qty"] // pallet_qty):
                    if ptr < len(scan_inputs):
                        assignments.append({"scan": scan_inputs[ptr], "lot_number": lot["lot_number"]})
                        ptr += 1
            st.session_state.assignments = assignments
        
        st.session_state.review_mode = True
        st.rerun()

    # Review Mode
    if st.session_state.review_mode:
        st.subheader("🔎 Review Summary")
        with st.expander("Transaction Details", expanded=True):
            st.write(f"**Type:** {transaction_type}")
            st.write(f"**Item:** {item_code}")
            st.write(f"**Qty:** {total_qty}")
            st.write(f"**Warehouse:** {warehouse}")
            if transaction_type in ["Job Issue", "Return"]:
                st.write(f"**From Location:** {from_location}")
                if st.session_state.assignments:
                    st.write("**Lot Assignments:**")
                    st.dataframe(pd.DataFrame(st.session_state.assignments))
            else:
                st.write(f"**From/To:** {from_location or 'N/A'} / {to_location or 'N/A'}")
                if transaction_type == "Receiving":
                    st.write(f"**PO:** {po_number or 'N/A'}")
                if transaction_type == "Manual Adjustment":
                    st.write(f"**Note:** {note or 'N/A'}")
                if transaction_type not in ["Manual Adjustment", "Return"] and scan_inputs:
                    st.write("**Scans:**")
                    st.code("\n".join(scan_inputs))

        col_confirm, col_cancel = st.columns(2)
        with col_confirm:
            if st.button("Confirm and Submit"):
                try:
                    with get_db_cursor() as cursor:
                        # Inventory updates
                        target_loc = get_target_location(transaction_type, from_location, to_location)
                        
                        # Check multi-item location
                        if transaction_type != "Manual Adjustment":
                            cursor.execute(
                                "SELECT multi_item_allowed FROM locations WHERE location_code=%s AND warehouse=%s",
                                (target_loc, warehouse)
                            )
                            result = cursor.fetchone()
                            is_multi_item = bool(result and result[0])
                            if not is_multi_item:
                                cursor.execute(
                                    "SELECT item_code FROM current_inventory WHERE warehouse=%s AND location=%s AND quantity>0",
                                    (warehouse, target_loc)
                                )
                                existing = [r[0] for r in cursor.fetchall()]
                                if existing and any(ic != item_code for ic in existing):
                                    st.error(f"Location '{target_loc}' already holds a different item.")
                                    return

                        # Update inventory based on transaction type
                        if transaction_type == "Internal Movement":
                            cursor.execute(
                                "UPDATE current_inventory SET quantity = quantity - %s WHERE warehouse=%s AND location=%s AND item_code=%s",
                                (total_qty, warehouse, from_location, item_code)
                            )
                            cursor.execute(
                                "INSERT INTO current_inventory (warehouse, location, item_code, quantity) VALUES (%s,%s,%s,%s) "
                                "ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity",
                                (warehouse, to_location, item_code, total_qty)
                            )
                        elif transaction_type == "Receiving":
                            cursor.execute(
                                "INSERT INTO current_inventory (warehouse, location, item_code, quantity) VALUES (%s,%s,%s,%s) "
                                "ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity",
                                (warehouse, to_location, item_code, total_qty)
                            )
                        elif transaction_type == "Job Issue":
                            cursor.execute(
                                "UPDATE current_inventory SET quantity = quantity - %s WHERE warehouse=%s AND location=%s AND item_code=%s",
                                (total_qty, warehouse, from_location, item_code)
                            )
                        elif transaction_type == "Return":
                            cursor.execute(
                                "INSERT INTO current_inventory (warehouse, location, item_code, quantity) VALUES (%s,%s,%s,%s) "
                                "ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity",
                                (warehouse, from_location, item_code, total_qty)
                            )
                        elif transaction_type == "Manual Adjustment":
                            cursor.execute(
                                "INSERT INTO current_inventory (warehouse, location, item_code, quantity) VALUES (%s,%s,%s,%s) "
                                "ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity = EXCLUDED.quantity",
                                (warehouse, to_location, item_code, total_qty)
                            )

                        # Insert transactions and scans
                        if transaction_type in ["Job Issue", "Return"]:
                            for lot in st.session_state.lot_inputs:
                                qty = lot["qty"]
                                if qty > 0:
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
                                        "user_id": st.session_state.get("user", "unknown"),
                                        "bypassed_warning": False,
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
                                                "warehouse": warehouse
                                            })
                        else:
                            insert_transaction({
                                "transaction_type": transaction_type,
                                "item_code": item_code,
                                "quantity": total_qty,
                                "job_number": job_number or "",
                                "lot_number": "",
                                "po_number": po_number or "",
                                "from_location": from_location,
                                "to_location": to_location,
                                "from_warehouse": None,
                                "to_warehouse": None,
                                "user_id": st.session_state.get("user", "unknown"),
                                "bypassed_warning": False,
                                "note": note,
                                "warehouse": warehouse
                            })
                            if transaction_type != "Manual Adjustment":
                                for scan in scan_inputs:
                                    if scan:
                                        insert_scan_verification({
                                            "item_code": item_code,
                                            "job_number": job_number or "",
                                            "lot_number": "",
                                            "scan_id": scan,
                                            "location": target_loc,
                                            "transaction_type": transaction_type,
                                            "warehouse": warehouse
                                        })

                        # Update scan locations
                        if transaction_type in ["Receiving", "Internal Movement", "Return"]:
                            for scan_id in scan_inputs:
                                if scan_id:
                                    update_scan_location(
                                        cursor, scan_id, item_code, to_location,
                                        transaction_type=transaction_type, job_number=job_number
                                    )
                        elif transaction_type == "Job Issue":
                            for scan_id in scan_inputs:
                                if scan_id:
                                    delete_scan_location(cursor, scan_id)

                    st.session_state.transaction_success = True
                    # Reset state
                    st.session_state.review_mode = False
                    st.session_state.scan_inputs = []
                    st.session_state.lot_inputs = []
                    st.session_state.assignments = []
                    st.session_state.reset_scans = True
                    st.session_state.reset_lots = True
                    st.rerun()

                except ValueError as e:
                    st.error(f"Validation error: {str(e)}")
                except Exception as e:
                    st.error(f"Failed to submit transaction: {str(e)}")

        with col_cancel:
            if st.button("Cancel Review"):
                st.session_state.review_mode = False
                st.rerun()

if __name__ == "__main__":
    run()
