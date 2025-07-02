import streamlit as st
import math
import random
from collections import Counter, defaultdict
from db import get_db_cursor
from config import WAREHOUSES
from pages.receiving import IRISH_TOASTS

def run():
    st.header("🔀 Internal Movement")

    lines = st.session_state.get(
        "im_lines",
        [{"item_code": "", "quantity": 1, "pallet_qty": 1,
          "from_location": "", "to_location": "", "note": "", "scans": []}]
    )

    for idx, line in enumerate(lines):
        with st.expander(f"Line {idx+1}", expanded=True):
            col1, col2, col3, col4, col5, col6, col7 = st.columns([2, 1, 1, 2, 2, 2, 1])
            line["item_code"] = col1.text_input("Item Code", line.get("item_code", ""), key=f"im_item_code_{idx}")
            line["quantity"] = col2.number_input("Quantity", min_value=1, step=1, value=line.get("quantity", 1), key=f"im_quantity_{idx}")
            line["pallet_qty"] = col3.number_input("Pallet Qty", min_value=1, step=1, value=line.get("pallet_qty", 1), key=f"im_pallet_qty_{idx}")
            line["from_location"] = col4.text_input("From Location", line.get("from_location", ""), key=f"im_from_location_{idx}")
            line["to_location"] = col5.text_input("To Location", line.get("to_location", ""), key=f"im_to_location_{idx}")
            line["note"] = col6.text_input("Note", line.get("note", ""), key=f"im_note_{idx}")
            if col7.button("Remove", key=f"im_remove_{idx}"):
                lines.pop(idx)
                st.session_state["im_lines"] = lines
                st.rerun()

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

    if st.button("Add Line"):
        lines.append({"item_code": "", "quantity": 1, "pallet_qty": 1,
                      "from_location": "", "to_location": "", "note": "", "scans": []})
        st.session_state["im_lines"] = lines
        st.rerun()

    warehouse = st.selectbox("Warehouse", WAREHOUSES, key="im_warehouse")

    if st.button("Confirm & Submit Internal Movement"):
        error_msgs = []
        all_scans = []

        request_totals = defaultdict(int)
        for line in lines:
            key = (line["item_code"], line["from_location"])
            request_totals[key] += line["quantity"]

        for (item, from_loc), total_qty in request_totals.items():
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(quantity,0) FROM current_inventory "
                    "WHERE warehouse=%s AND location=%s AND item_code=%s",
                    (warehouse, from_loc, item)
                )
                available = cur.fetchone()[0]
            if total_qty > available:
                error_msgs.append(
                    f"Insufficient stock for item '{item}' in '{from_loc}'. "
                    f"Requested total {total_qty}, available {available}."
                )

        for idx, line in enumerate(lines):
            item = line["item_code"]
            qty = line["quantity"]
            from_loc = line["from_location"]
            to_loc = line["to_location"]

            if not item or qty <= 0 or not from_loc or not to_loc:
                error_msgs.append(f"Line {idx+1}: missing item code, quantity, from or to location.")
            if from_loc == to_loc:
                error_msgs.append(f"Line {idx+1}: from and to location must differ.")

            # Validate from_location exists
            with get_db_cursor() as cur:
                cur.execute("SELECT 1 FROM locations WHERE location_code = %s", (from_loc,))
                if cur.fetchone() is None:
                    error_msgs.append(
                        f"Line {idx+1}: source location '{from_loc}' does not exist in system. Please verify or correct it."
                    )

            # Validate to_location exists and is in correct warehouse
            with get_db_cursor() as cur:
                cur.execute("SELECT warehouse FROM locations WHERE location_code = %s", (to_loc,))
                result = cur.fetchone()
                if result is None:
                    error_msgs.append(
                        f"Line {idx+1}: destination location '{to_loc}' does not exist in system. Please create it via Manage Locations tab."
                    )
                elif result[0] != warehouse:
                    error_msgs.append(
                        f"Line {idx+1}: destination location '{to_loc}' belongs to warehouse '{result[0]}', "
                        f"but selected warehouse is '{warehouse}'. Use Warehouse Transfer module instead."
                    )

            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(quantity),0) FROM current_inventory "
                    "WHERE warehouse=%s AND location=%s AND item_code!=%s",
                    (warehouse, to_loc, item)
                )
                other_qty = cur.fetchone()[0]
            if other_qty > 0:
                error_msgs.append(
                    f"Line {idx+1}: Location '{to_loc}' has other items. "
                    "Please reset via Manage Locations tab."
                )

            expected = math.ceil(qty / line["pallet_qty"])
            scans = [s.strip() for s in line.get("scans", [])]
            if len(scans) != expected or any(not s for s in scans):
                error_msgs.append(f"Line {idx+1}: scans count mismatch; expected {expected}.")

            for s in scans:
                all_scans.append(s)
                with get_db_cursor() as cur:
                    cur.execute("SELECT location, item_code FROM current_scan_location WHERE scan_id = %s", (s,))
                    scan_loc_result = cur.fetchone()
                if scan_loc_result:
                    prev_loc, prev_item = scan_loc_result
                    if prev_loc == to_loc:
                        error_msgs.append(
                            f"Line {idx+1}: scan '{s}' is already in destination location {to_loc}. "
                            "Cannot re-transfer an item to the same place."
                        )
                    elif prev_loc != from_loc:
                        error_msgs.append(
                            f"Line {idx+1}: scan '{s}' is currently at {prev_loc} — expected at {from_loc}. "
                            "Scans must be physically present at source before movement."
                        )
                    elif prev_item != item:
                        error_msgs.append(
                            f"Line {idx+1}: scan '{s}' is tagged to item {prev_item}, not {item}. "
                            "Item mismatch must be resolved before move."
                        )
                else:
                    with get_db_cursor() as cur:
                        cur.execute(
                            "SELECT location, item_code FROM scan_verifications "
                            "WHERE scan_id = %s ORDER BY scan_time DESC LIMIT 1",
                            (s,)
                        )
                        last_seen = cur.fetchone()
                    if last_seen:
                        last_loc, last_item = last_seen
                        error_msgs.append(
                            f"Line {idx+1}: scan '{s}' not in live scan table. Last seen at {last_loc} (item {last_item}). Validate via Manage Scans before reuse."
                        )
                    else:
                        error_msgs.append(
                            f"Line {idx+1}: scan '{s}' not recognized in system. Invalid or stale scan ID."
                        )

        dup_counts = Counter(all_scans)
        duplicates = [s for s, count in dup_counts.items() if count > 1]
        if duplicates:
            error_msgs.append(f"Duplicate scan IDs entered: {', '.join(duplicates)}")

        if error_msgs:
            st.error("\n".join(error_msgs))
            return

        progress = st.progress(0)
        try:
            with get_db_cursor() as cur:
                total = len(lines)
                for idx, line in enumerate(lines):
                    item = line["item_code"]
                    qty = line["quantity"]
                    from_loc = line["from_location"]
                    to_loc = line["to_location"]

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
                            item, qty,
                            from_loc, to_loc,
                            st.session_state.user,
                            line.get("note", ""), warehouse
                        )
                    )
                    txn_id = cur.fetchone()[0]

                    cur.execute(
                        "UPDATE current_inventory SET quantity = quantity - %s "
                        "WHERE warehouse=%s AND location=%s AND item_code=%s",
                        (qty, warehouse, from_loc, item)
                    )
                    cur.execute(
                        """
                        INSERT INTO current_inventory (
                            warehouse, location, item_code, quantity
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (warehouse, location, item_code)
                        DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity
                        """,
                        (warehouse, to_loc, item, qty)
                    )

                    for s in line["scans"]:
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
                            (item, s, to_loc, "Internal Movement", warehouse, st.session_state.user)
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
                            (s, item, to_loc)
                        )
                    progress.progress(int((idx + 1) / total * 100))

            toast = random.choice(IRISH_TOASTS)
            st.success(toast)
            if st.button("Continue"):
                for key in ["im_lines", "im_warehouse"]:
                    st.session_state.pop(key, None)
                st.rerun()
        except Exception as e:
            st.error(f"Failed to submit internal movement: {e}")
