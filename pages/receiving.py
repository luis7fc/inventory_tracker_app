# pages/receiving.py

import streamlit as st
import math
import random
from collections import Counter
from db import get_db_cursor
from config import WAREHOUSES

# 🍻 Irish toasts for success messages
IRISH_TOASTS = [
    "☘️ Sláinte! Transaction submitted successfully!",
    "🍀 Luck o’ the Irish – you did it!",
    "🥃 Cheers, let’s grab a beer – transaction success!",
    "🌈 Pot of gold secured – job well done!",
    "🪙 May your inventory always balance – success!"
]

# locations where pallet scans should skip uniqueness checks
SKIP_SCAN_CHECK_LOCATIONS = ("VKIT", "SKIT", "FKIT", "IKIT", "GKIT")


def run():
    st.header("📑 Receiving")

    # PO-level input
    po_number = st.text_input("PO Number", key="recv_po_number")

    # Initialize or retrieve existing receive lines (default quantity=1)
    lines = st.session_state.get(
        "recv_lines",
        [{"item_code": "", "quantity": 1, "pallet_qty": 1, "location": "", "scans": []}]
    )

    # Render line items
    for idx, line in enumerate(lines):
        with st.expander(f"Line {idx+1}", expanded=True):
            col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 2, 1])
            line["item_code"] = col1.text_input(
                "Item Code", line.get("item_code", ""), key=f"recv_item_code_{idx}"
            )
            default_qty = max(1, line.get("quantity", 1))
            line["quantity"] = col2.number_input(
                "Quantity", min_value=1, step=1,
                value=default_qty, key=f"recv_quantity_{idx}"
            )
            line["pallet_qty"] = col3.number_input(
                "Pallet Qty", min_value=1, step=1,
                value=line.get("pallet_qty", 1), key=f"recv_pallet_{idx}"
            )
            line["location"] = col4.text_input(
                "Location", line.get("location", ""), key=f"recv_location_{idx}"
            )
            if col5.button("Remove", key=f"recv_remove_{idx}"):
                lines.pop(idx)
                st.session_state["recv_lines"] = lines
                st.rerun()

            expected_scans = math.ceil(line["quantity"] / line["pallet_qty"])
            scans = []
            for j in range(expected_scans):
                scans.append(
                    st.text_input(
                        f"Scan {j+1} of {expected_scans}", key=f"recv_scan_{idx}_{j}"
                    )
                )
            line["scans"] = scans

    # Button to add a new line
    if st.button("Add Line"):
        lines.append({"item_code": "", "quantity": 1, "pallet_qty": 1, "location": "", "scans": []})
        st.session_state["recv_lines"] = lines
        st.rerun()

    # Warehouse selector
    warehouse = st.selectbox("Warehouse", WAREHOUSES, key="recv_warehouse")

    # Confirm & Submit
    if st.button("Confirm & Submit Receiving"):
        # 1) Validate PO Number
        if not po_number:
            st.error("PO Number is required.")
            return

        # 2) Validate each line
        error_msgs = []
        for idx, line in enumerate(lines):
            if not line["item_code"] or line["quantity"] <= 0 or not line["location"]:
                error_msgs.append(f"Line {idx+1}: missing item code, quantity, or location.")

            # Multi-item rule via locations table
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT multi_item_allowed FROM locations WHERE location_code=%s",
                    (line["location"],)
                )
                row = cur.fetchone()
            multi_allowed = bool(row[0]) if row else False
            if not multi_allowed:
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT item_code FROM current_inventory WHERE location=%s AND quantity>0",
                        (line["location"],)
                    )
                    existing = [r[0] for r in cur.fetchall()]
                if existing and any(ec != line["item_code"] for ec in existing):
                    error_msgs.append(
                        f"Line {idx+1}: location '{line['location']}' contains other item(s)."
                    )

            # Scan-count logic
            expected = math.ceil(line["quantity"] / line["pallet_qty"])
            scans = line.get("scans", [])
            if len(scans) != expected or any(not s.strip() for s in scans):
                error_msgs.append(f"Line {idx+1}: scans count mismatch or blank entries.")

            # Scan uniqueness against current_scan_location (skip for KIT locations)
            for scan_id in scans:
                clean_id = scan_id.strip()
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT location FROM current_scan_location WHERE scan_id=%s",
                        (clean_id,)
                    )
                    existing_loc = cur.fetchone()
                if existing_loc:
                    prev_loc = existing_loc[0]
                    # only block if previous location is not a kitting exception
                    if prev_loc not in SKIP_SCAN_CHECK_LOCATIONS:
                        error_msgs.append(
                            f"Scan '{clean_id}' was already processed in {prev_loc}."
                        )

        # Local duplicate scan guard across all lines
        all_scans = all_scans = [s.strip() for line in lines for s in line.get("scans", [])]
        dup_counts = Counter(all_scans)
        duplicates = [scan for scan, count in dup_counts.items() if count > 1]
        if duplicates:
            error_msgs.append(f"Duplicate scan IDs entered: {', '.join(duplicates)}")

        if error_msgs:
            st.error("\n".join(error_msgs))
            return

        # 3) Write to DB with progress bar
        total = len(lines)
        progress = st.progress(0)
        try:
            with get_db_cursor() as cur:
                for idx, line in enumerate(lines):
                    # Insert transaction record
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            transaction_type, item_code, quantity, date,
                            job_number, lot_number, po_number,
                            from_location, to_location,
                            user_id, bypassed_warning, note, warehouse
                        ) VALUES (
                            %s, %s, %s, NOW(),
                            NULL, NULL, %s,
                            NULL, %s,
                            %s, FALSE, '', %s
                        ) RETURNING id
                        """,
                        (
                            "Receiving",
                            line["item_code"], line["quantity"], po_number,
                            line["location"],
                            st.session_state.user,
                            warehouse
                        )
                    )
                    txn_id = cur.fetchone()[0]

                    # Upsert current_inventory with warehouse
                    cur.execute(
                        """
                        INSERT INTO current_inventory (item_code, location, warehouse, quantity)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (item_code, location)
                        DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity
                        """,
                        (line["item_code"], line["location"], warehouse, line["quantity"])
                    )

                    # Insert scans
                    for scan_id in line["scans"]:
                        sid = scan_id.strip()
                        cur.execute(
                            """
                            INSERT INTO scan_verifications (
                                item_code, scan_time, scan_id, job_number, lot_number,
                                location, transaction_type,
                                warehouse, scanned_by
                            ) VALUES (
                                %s, NOW(), %s, %s, %s, %s, %s, %s, %s
                            )
                            """,
                            (
                                line["item_code"], sid, None, None,
                                line["location"], "Receiving",
                                warehouse, st.session_state.user
                            )
                        )
                        # Upsert current_scan_location
                        cur.execute(
                            """
                            INSERT INTO current_scan_location (
                                scan_id, item_code, location, updated_at
                            ) VALUES (%s, %s, %s, NOW())
                            ON CONFLICT (scan_id)
                            DO UPDATE SET
                                item_code  = EXCLUDED.item_code,
                                location   = EXCLUDED.location,
                                updated_at = EXCLUDED.updated_at
                            """,
                            (sid, line["item_code"], line["location"])
                        )

                    # update progress bar
                    progress.progress(int((idx + 1) / total * 100))

            # Show lasting Irish toast until user acknowledges
            toast = random.choice(IRISH_TOASTS)
            st.success(toast)
            if st.button("Continue"):
                for key in ["recv_po_number", "recv_warehouse"]:
                    st.session_state.pop(key, None)
                st.session_state.pop("recv_lines", None)
                st.rerun()

        except Exception as e:
            st.error(f"Failed to submit receiving: {e}")
