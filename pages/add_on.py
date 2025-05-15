import streamlit as st
from datetime import datetime
from db import (
    get_db_cursor,
    insert_transaction,
    insert_scan_verification,
    update_scan_location,
    insert_pulltag_line,
    validate_scan_for_transaction
)
from config import WAREHOUSES

def get_item_metadata(item_code):
    with get_db_cursor() as cur:
        cur.execute(
            "SELECT cost_code, item_description, uom FROM items_master WHERE item_code = %s",
            (item_code,)
        )
        row = cur.fetchone()
        if row:
            return {"cost_code": row[0], "description": row[1], "uom": row[2]}
        return None

def update_current_inventory(item_code, location, delta_quantity, warehouse):
    with get_db_cursor() as cur:
        cur.execute(
            """SELECT quantity FROM current_inventory 
                WHERE item_code = %s AND location = %s AND warehouse = %s""",
            (item_code, location, warehouse)
        )
        row = cur.fetchone()
        if row:
            new_qty = row[0] + delta_quantity
            cur.execute(
                """UPDATE current_inventory 
                    SET quantity = %s 
                    WHERE item_code = %s AND location = %s AND warehouse = %s""",
                (new_qty, item_code, location, warehouse)
            )
        else:
            cur.execute(
                """INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                    VALUES (%s, %s, %s, %s)""",
                (item_code, location, delta_quantity, warehouse)
            )

def run():
    st.title("➕ Add-On Pulltag (Job Issue/Return)")

    transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"])
    job_number = st.text_input("Job Number").strip()
    lot_number = st.text_input("Lot Number").strip()
    location = st.text_input("Location").strip()
    warehouse = st.selectbox("Warehouse", WAREHOUSES)
    note = st.text_input("Optional Note")

    st.markdown("### Add-On Line Items")
    item_rows = st.data_editor(
        [{"item_code": "", "quantity": 1}],
        num_rows="dynamic",
        use_container_width=True,
        key="add_on_editor"
    )

    if st.button("Submit All Items"):
        now = datetime.now()
        user = st.session_state.get("username", "unknown")

        for row in item_rows:
            item_code = row["item_code"].strip()
            quantity = int(row["quantity"])

            if not item_code or quantity <= 0:
                st.warning("Invalid entry. Skipping blank or zero-quantity rows.")
                continue

            item_meta = get_item_metadata(item_code)
            if not item_meta:
                st.warning(f"Item {item_code} not found in items_master. Skipped.")
                continue

            cost_code = item_meta.get("cost_code")
            description = item_meta.get("description")
            uom = item_meta.get("uom")

            if cost_code != item_code:
                st.info(f"Item {item_code} not scan-tracked. Skipped.")
                continue

            insert_pulltag_line({
                "job_number": job_number,
                "lot_number": lot_number,
                "item_code": item_code,
                "cost_code": cost_code,
                "description": description,
                "quantity": quantity,
                "status": "complete",
                "uploaded_at": now,
                "last_updated": now,
                "warehouse": warehouse,
                "uom": uom,
                "transaction_type": transaction_type
            })

            insert_transaction({
                "transaction_type": transaction_type,
                "item_code": item_code,
                "quantity": quantity,
                "date": now,
                "job_number": job_number,
                "lot_number": lot_number,
                "po_number": None,
                "from_location": location if transaction_type == "ADD" else None,
                "to_location": location if transaction_type == "RETURNB" else None,
                "user_id": user,
                "bypassed_warning": False,
                "note": note,
                "warehouse": warehouse
            })

            for i in range(quantity):
                scan = st.text_input(f"Scan {item_code} [{i+1}]", key=f"scan_{item_code}_{i}").strip()
                if not scan:
                    st.warning(f"Missing scan {i+1} for {item_code}.")
                    continue

                if validate_scan_for_transaction(scan, item_code):
                    insert_scan_verification({
                        "item_code": item_code,
                        "job_number": job_number,
                        "lot_number": lot_number,
                        "scan_time": now,
                        "location": location,
                        "transaction_type": transaction_type,
                        "warehouse": warehouse,
                        "pulltag_id": None,
                        "scanned_by": user
                    })
                    update_scan_location(scan_id=scan, item_code=item_code, location=location, updated_at=now)
                else:
                    st.warning(f"⚠️ Duplicate or invalid scan: {scan}")

            update_current_inventory(item_code, location, quantity if transaction_type == "ADD" else -quantity, warehouse)

        st.success("✅ All valid add-on lines submitted successfully.")
