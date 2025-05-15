# pages/internal_movement.py

import streamlit as st
import math
import random
from collections import Counter
from db import get_db_cursor
from config import WAREHOUSES
from pages.receiving import SKIP_SCAN_CHECK_LOCATIONS, IRISH_TOASTS


def run():
    st.header("🔀 Internal Movement")

    # Initialize or retrieve existing internal movement lines
    lines = st.session_state.get(
        "im_lines",
        [{"item_code": "", "quantity": 1, "pallet_qty": 1,
          "from_location": "", "to_location": "", "note": "", "scans": []}]
    )

    # Render line items
    for idx, line in enumerate(lines):
        with st.expander(f"Line {idx+1}", expanded=True):
            col1, col2, col3, col4, col5, col6, col7 = st.columns([2, 1, 1, 2, 2, 2, 1])
            line["item_code"] = col1.text_input(
                "Item Code", line.get("item_code", ""), key=f"im_item_code_{idx}"
            )
            line["quantity"] = col2.number_input(
                "Quantity", min_value=1, step=1,
                value=line.get("quantity", 1), key=f"im_quantity_{idx}"
            )
            line["pallet_qty"] = col3.number_input(
                "Pallet Qty", min_value=1, step=1,
                value=line.get("pallet_qty", 1), key=f"im_pallet_qty_{idx}"
            )
            line["from_location"] = col4.text_input(
                "From Location", line.get("from_location", ""), key=f"im_from_location_{idx}"
            )
            line["to_location"] = col5.text_input(
                "To Location", line.get("to_location", ""), key=f"im_to_location_{idx}"
            )
            line["note"] = col6.text_input(
                "Note", line.get("note", ""), key=f"im_note_{idx}"
            )
            if col7.button("Remove", key=f"im_remove_{idx}"):
                lines.pop(idx)
                st.session_state["im_lines"] = lines
                st.rerun()

            # Scan inputs based on pallet quantity logic
            expected_scans = math.ceil(line["quantity"] / line["pallet_qty"])
            scans = []
            for j in range(expected_scans):
                scans.append(
                    st.text_input(
                        f"Scan {j+1} of {expected_scans}",
                        value=(line.get("scans", [])[j] if j < len(line.get("scans", [])) else ""),
                        key=f"im_scan_{idx}_{j}"
                    )
                )
            line["scans"] = scans

    # Add new line button
    if st.button("Add Line"):
        lines.append({"item_code": "", "quantity": 1, "pallet_qty": 1,
                      "from_location": "", "to_location": "", "note": "", "scans": []})
        st.session_state["im_lines"] = lines
        st.rerun()

    # Warehouse selector
    warehouse = st.selectbox("Warehouse", WAREHOUSES, key="im_warehouse")

    # Confirm & Submit Internal Movement
    if st.button("Confirm & Submit Internal Movement"):
        error_msgs = []
        all_scans = []

        # Validation
        for idx, line in enumerate(lines):
            if not line["item_code"] or line["quantity"] <= 0 \
               or not line["from_location"] or not line["to_location"]:
                error_msgs.append(
                    f"Line {idx+1}: missing item code, quantity, from or to location."
                )
            if line["from_location"] == line["to_location"]:
                error_msgs.append(
                    f"Line {idx+1}: from and to location must differ."
                )

            # Enforce single-item rule on to_location
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(quantity),0) FROM current_inventory "
                    "WHERE warehouse=%s AND location=%s AND item_code!=%s",
                    (warehouse, line["to_location"], line["item_code"])
                )
                other_qty = cur.fetchone()[0]
            if other_qty > 0:
                error_msgs.append(
                    f"Line {idx+1}: Location '{line['to_location']}' has other items. "
                    "Please reset via Manage Locations tab."
                )

            # Scan-count validation
            expected = math.ceil(line["quantity"] / line["pallet_qty"])
            scans = [s.strip() for s in line.get("scans", [])]
            if len(scans) != expected or any(not s for s in scans):
                error_msgs.append(
                    f"Line {idx+1}: scans count mismatch; expected {expected}."
                )

            # Scan uniqueness checks
            for sid in scans:
                all_scans.append(sid)
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT location FROM current_scan_location WHERE scan_id=%s",
                        (sid,)
                    )
                    existing = cur.fetchone()
                if existing:
                    prev_loc = existing[0]
                    # Allow if scan at from_location or in skip list
                    if prev_loc not in SKIP_SCAN_CHECK_LOCATIONS and prev_loc != line["from_location"]:
                        error_msgs.append(
                            f"Line {idx+1}: scan '{sid}' already processed at {prev_loc}."
                        )

        # Duplicate scan guard across lines
        dup_counts = Counter(all_scans)
        duplicates = [s for s, count in dup_counts.items() if count > 1]
        if duplicates:
            error_msgs.append(
                f"Duplicate scan IDs entered: {', '.join(duplicates)}"
            )

        if error_msgs:
            st.error("\n".join(error_msgs))
            return

        # Write to DB
        progress = st.progress(0)
        try:
            with get_db_cursor() as cur:
                total = len(lines)
                for idx, line in enumerate(lines):
                    # Insert transaction
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            transaction_type, item_code, quantity, date,
                            job_number, lot_number, po_number,
                            from_location, to_location,
                            user_id, bypassed_warning, note, warehouse
                        ) VALUES (
                            %s, %s, %s, NOW(),
                            NULL, NULL, NULL,
                            %s, %s,
                            %s, FALSE, %s, %s
                        ) RETURNING id
                        """,
                        (
                            "Internal Movement",
                            line["item_code"], line["quantity"],
                            line["from_location"], line["to_location"],
                            st.session_state.user,
                            line.get("note", ""), warehouse
                        )
                    )
                    txn_id = cur.fetchone()[0]

                    # Update current_inventory: subtract and add
                    cur.execute(
                        "UPDATE current_inventory SET quantity = quantity - %s "
                        "WHERE warehouse=%s AND location=%s AND item_code=%s",
                        (
                            line["quantity"], warehouse,
                            line["from_location"], line["item_code"]
                        )
                    )
                    cur.execute(
                        """
                        INSERT INTO current_inventory (
                            warehouse, location, item_code, quantity
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (warehouse, location, item_code)
                        DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity
                        """,
                        (
                            warehouse, line["to_location"],
                            line["item_code"], line["quantity"]
                        )
                    )

                    # Insert scan_verifications and upsert current_scan_location
                    for sid in line["scans"]:
                        cur.execute(
                            """
                            INSERT INTO scan_verifications (
                                item_code, scan_time, scan_id,
                                job_number, lot_number,
                                location, transaction_type,
                                warehouse, scanned_by
                            ) VALUES (
                                %s, NOW(), %s,
                                NULL, NULL,
                                %s, %s,
                                %s, %s
                            )
                            """,
                            (
                                line["item_code"], sid,
                                line["to_location"],
                                "Internal Movement",
                                warehouse, st.session_state.user
                            )
                        )
                        cur.execute(
                            """
                            INSERT INTO current_scan_location (
                                scan_id, item_code, location, updated_at
                            ) VALUES (%s, %s, %s, NOW())
                            ON CONFLICT (scan_id)
                            DO UPDATE SET
                                item_code = EXCLUDED.item_code,
                                location   = EXCLUDED.location,
                                updated_at = EXCLUDED.updated_at
                            """,
                            (sid, line["item_code"], line["to_location"])
                        )
                    progress.progress(int((idx + 1) / total * 100))

            # Show success toast
            toast = random.choice(IRISH_TOASTS)
            st.success(toast)
            if st.button("Continue"):
                for key in ["im_lines", "im_warehouse"]:
                    st.session_state.pop(key, None)
                st.rerun()
        except Exception as e:
            st.error(f"Failed to submit internal movement: {e}")
