import streamlit as st
from datetime import datetime
from config import STAGING_LOCATIONS
from db import get_db_cursor, insert_transaction, insert_scan_verification

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
    if "reset_scans" not in st.session_state:
        st.session_state.reset_scans = False
    if "reset_lots" not in st.session_state:
        st.session_state.reset_lots = False
    if "scan_inputs" not in st.session_state:
        st.session_state.scan_inputs = []
    if "review_mode" not in st.session_state:
        st.session_state.review_mode = False

    transaction_type = st.selectbox(
        "Transaction Type",
        ["Receiving", "Internal Movement", "Job Issue", "Return", "Manual Adjustment"]
    )

    # Common inputs
    item_code = st.text_input("Item Code", key="item_code")
    pallet_qty = 1
    warehouse = "VV"
    po_number = ""
    note = ""

    # Branch-specific inputs
    if transaction_type == "Manual Adjustment":
        total_qty = st.number_input("Total Quantity (+/-)", step=1, value=0)
        to_location = st.text_input("Location", key="to_location")
        note = st.text_area("Adjustment Note", key="note")
    elif transaction_type == "Job Issue":
        job_number = st.text_input("Job Number", key="job_number")
        from_location = st.text_input("Issue From Location", key="from_location")
        warehouse = st.text_input("Warehouse Initials (e.g. VV, SAC, FNO)", value="VV", key="warehouse")
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
        st.write(f"**Total Quantity (sum of lots):** {total_qty}")
        pallet_qty = st.number_input("Pallet Quantity", min_value=1, value=1, step=1)
    else:
        total_qty = st.number_input("Total Quantity", step=1, key="total_qty")
        pallet_qty = st.number_input(
            "Pallet Quantity", min_value=1, value=1, step=1, key="pallet_qty"
        )
        if transaction_type in ["Receiving", "Return"]:
            job_number = st.text_input("Job Number", key="job_number")
        if transaction_type == "Receiving":
            po_number = st.text_input("PO Number", key="po_number")
            to_location = st.text_input("Receiving Location", key="to_location")
        elif transaction_type == "Internal Movement":
            from_location = st.text_input("From Location", key="from_location")
            to_location = st.text_input("To Location", key="to_location")
        elif transaction_type == "Return":
            from_location = st.text_input("From Location", key="from_location")
            to_location = st.text_input("Return To Location", key="to_location")
            if not to_location:
                st.error("Return transactions require a return location.")
                st.stop()

    # Scan Inputs (except Manual Adjustment)
    if transaction_type != "Manual Adjustment":
        expected_scans = total_qty // max(pallet_qty, 1)
        st.write(f"**Expected Scans:** {expected_scans}")
        if st.session_state.reset_scans:
            for i in range(expected_scans):
                st.session_state[f"scan_{i}"] = ""
            st.session_state.reset_scans = False
        st.session_state.scan_inputs = []
        for i in range(expected_scans):
            scan_val = st.text_input(f"Scan {i+1}", key=f"scan_{i}")
            st.session_state.scan_inputs.append(scan_val)

    # Review & Confirm Flow
    if not st.session_state.review_mode:
        if st.button("Review Transaction"):
            if transaction_type != "Manual Adjustment" and len(st.session_state.scan_inputs) != expected_scans:
                st.error("Scan count must match expected scan count based on total and pallet qty.")
                st.stop()
            if transaction_type == "Job Issue":
                lot_inputs = []
                for i in range(st.session_state.num_lots):
                    lot_inputs.append({
                        "lot_number": st.session_state.get(f"lot_{i}"),
                        "qty": st.session_state.get(f"lot_qty_{i}", 0)
                    })
                assignments = []
                pointer = 0
                for lot in lot_inputs:
                    for _ in range(lot["qty"] // pallet_qty):
                        assignments.append({
                            "scan": st.session_state.scan_inputs[pointer],
                            "lot_number": lot["lot_number"]
                        })
                        pointer += 1
                st.session_state.assignments = assignments
                st.session_state.lot_inputs = lot_inputs
            st.session_state.review_mode = True
            st.rerun()

    else:
        st.subheader("🔎 Review Summary")
        st.write("**Transaction Type:**", transaction_type)
        st.write("**Item Code:**", item_code)
        st.write("**Quantity:**", total_qty)
        if transaction_type == "Job Issue":
            st.write("**Job Number:**", job_number)
            st.write("**From Location:**", from_location)
            st.write("**Warehouse:**", warehouse)
            st.write("**Lot Distributions:**")
            st.dataframe(st.session_state.assignments)
        else:
            st.write("**Job / Lot:**", st.session_state.get("job_number", ""), st.session_state.get("lot_number", ""))
            st.write("**From / To Location:**", st.session_state.get("from_location", ""), st.session_state.get("to_location", ""))
            st.write("**PO Number:**", po_number)
            st.write("**Warehouse:**", warehouse)
            st.write("**Note:**", note)
            st.write("**Scans:**")
            st.code("\n".join(st.session_state.scan_inputs))

        if st.button("Confirm and Submit"):
            # Perform inline updates in one transaction
            with get_db_cursor() as cursor:
                signed_qty = total_qty
                bypassed_warning = False
                # Internal Movement: subtract from source, add to destination
                if transaction_type == "Internal Movement":
                    signed_qty = -abs(total_qty)
                    cursor.execute(
                        "UPDATE current_inventory SET quantity = quantity - %s WHERE item_code = %s AND location = %s",
                        (total_qty, item_code, from_location)
                    )
                    cursor.execute(
                        "INSERT INTO current_inventory (item_code, location, quantity) VALUES (%s, %s, %s) "
                        "ON CONFLICT (item_code, location) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity",
                        (item_code, to_location, total_qty)
                    )
                    cursor.execute(
                        "SELECT quantity FROM current_inventory WHERE item_code = %s AND location = %s",
                        (item_code, from_location)
                    )
                    result = cursor.fetchone()
                    available = result[0] if result else 0
                    if available < 0:
                        st.warning(f"Inventory negative at {from_location} ({available}). Admin override required.")
                        admin_pass = st.text_input("Enter admin password to override:", type="password")
                        if admin_pass != st.secrets["general"]["admin_password"]:
                            st.error("Incorrect admin password. Transaction blocked.")
                            st.stop()
                        bypassed_warning = True
                # Receiving & Return: add to destination
                elif transaction_type in ["Receiving", "Return"]:
                    cursor.execute(
                        "INSERT INTO current_inventory (item_code, location, quantity) VALUES (%s, %s, %s) "
                        "ON CONFLICT (item_code, location) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity",
                        (item_code, to_location, total_qty)
                    )
                # Job Issue: subtract from source
                elif transaction_type == "Job Issue":
                    cursor.execute(
                        "UPDATE current_inventory SET quantity = quantity - %s WHERE item_code = %s AND location = %s",
                        (total_qty, item_code, from_location)
                    )

                # Multi-item location guard
                target_loc = get_target_location(
                    transaction_type,
                    st.session_state.get("from_location", ""),
                    st.session_state.get("to_location", "")
                )
                cursor.execute(
                    "SELECT multi_item_allowed FROM locations WHERE location_code = %s AND warehouse = %s",
                    (target_loc, warehouse)
                )
                result = cursor.fetchone()
                is_multi_item = bool(result and result[0])
                if transaction_type != "Manual Adjustment" and not is_multi_item:
                    cursor.execute(
                        "SELECT item_code FROM current_inventory WHERE location = %s AND quantity > 0",
                        (target_loc,)
                    )
                    items_present = [row[0] for row in cursor.fetchall()]
                    if items_present and any(existing != item_code for existing in items_present):
                        st.error(
                            f"Location '{target_loc}' already has a different item with nonzero quantity. Only multi-item locations can hold multiple item types."
                        )
                        st.stop()

            # Insert transactions and scans via helper functions
            if transaction_type == "Job Issue":
                for lot in st.session_state.lot_inputs:
                    insert_transaction({
                        "transaction_type": transaction_type,
                        "item_code": item_code,
                        "quantity": lot["qty"],
                        "job_number": job_number,
                        "lot_number": lot["lot_number"],
                        "po_number": po_number,
                        "from_location": st.session_state.from_location,
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
                                "warehouse": warehouse
                            })
            else:
                insert_transaction({
                    "transaction_type": transaction_type,
                    "item_code": item_code,
                    "quantity": total_qty,
                    "job_number": st.session_state.get("job_number", ""),
                    "lot_number": st.session_state.get("lot_number", ""),
                    "po_number": po_number,
                    "from_location": st.session_state.get("from_location", ""),
                    "to_location": st.session_state.get("to_location", ""),
                    "from_warehouse": None,
                    "to_warehouse": None,
                    "user_id": st.session_state.user,
                    "bypassed_warning": bypassed_warning,
                    "note": note,
                    "warehouse": warehouse
                })
                if transaction_type != "Manual Adjustment":
                    for scan in st.session_state.scan_inputs:
                        insert_scan_verification({
                            "item_code": item_code,
                            "job_number": st.session_state.get("job_number", ""),
                            "lot_number": st.session_state.get("lot_number", ""),
                            "scan_id": scan,
                            "location": target_loc,
                            "transaction_type": transaction_type,
                            "warehouse": warehouse
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
