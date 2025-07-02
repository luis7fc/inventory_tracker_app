import streamlit as st
import math
import random
from collections import Counter, defaultdict
from db import get_db_cursor
from config import WAREHOUSES
from pages.receiving import IRISH_TOASTS
from internal_movement import run as run_warehouse_movement

# Unified Transfer Operations page with subtabs for warehouse and cross-warehouse movements
def run():
    st.title("🔁 Transfer Operations")
    # Choose between same-warehouse and cross-warehouse flows
    subtab = st.radio(
        "Select Transfer Type",
        ["🏭 Warehouse Movement", "🚚 Cross-Warehouse Transfer"],
        horizontal=True,
        key="transfer_mode"
    )

    if subtab == "🏭 Warehouse Movement":
        # Delegate to existing internal movement logic
        run_warehouse_movement()
        return

    # 🚚 Cross-Warehouse Transfer logic
    st.header("🚚 Transfers Between Warehouses")
    lines = st.session_state.get(
        "transfer_lines",
        [{
            "item_code": "", "quantity": 1, "pallet_qty": 1,
            "from_location": "", "from_warehouse": "",
            "to_location": "", "to_warehouse": "",
            "note": "", "scans": []
        }]
    )

    # Render each transfer line
    for idx, line in enumerate(lines):
        with st.expander(f"Line {idx+1}", expanded=True):
            # Top row: item, qty, pallet
            c1, c2, c3 = st.columns([2, 1, 1])
            line["item_code"] = c1.text_input(
                "Item Code", line.get("item_code", ""), key=f"tr_item_code_{idx}"
            )
            line["quantity"] = c2.number_input(
                "Quantity", min_value=1, step=1, value=line.get("quantity", 1), key=f"tr_quantity_{idx}"
            )
            line["pallet_qty"] = c3.number_input(
                "Pallet Qty", min_value=1, step=1, value=line.get("pallet_qty", 1), key=f"tr_pallet_qty_{idx}"
            )
            # Second row: from loc/wh and to loc/wh
            c4, c5 = st.columns(2)
            line["from_location"] = c4.text_input(
                "From Location", line.get("from_location", ""), key=f"tr_from_loc_{idx}"
            )
            line["from_warehouse"] = c5.selectbox(
                "From Warehouse", WAREHOUSES, index=WAREHOUSES.index(line.get("from_warehouse")) if line.get("from_warehouse") in WAREHOUSES else 0,
                key=f"tr_from_wh_{idx}"
            )
            c6, c7 = st.columns(2)
            line["to_location"] = c6.text_input(
                "To Location", line.get("to_location", ""), key=f"tr_to_loc_{idx}"
            )
            line["to_warehouse"] = c7.selectbox(
                "To Warehouse", WAREHOUSES, index=WAREHOUSES.index(line.get("to_warehouse")) if line.get("to_warehouse") in WAREHOUSES else 0,
                key=f"tr_to_wh_{idx}"
            )
            # Note
            line["note"] = st.text_input(
                "Note", line.get("note", ""), key=f"tr_note_{idx}"
            )
            # Remove line
            if st.button("Remove Line", key=f"tr_remove_{idx}"):
                lines.pop(idx)
                st.session_state["transfer_lines"] = lines
                st.rerun()
            # Scan inputs based on pallet logic
            expected = math.ceil(line["quantity"] / line["pallet_qty"])
            scans = []
            for j in range(expected):
                scans.append(
                    st.text_input(
                        f"Scan {j+1} of {expected}",
                        value=(line.get("scans", [])[j] if j < len(line.get("scans", [])) else ""),
                        key=f"tr_scan_{idx}_{j}"
                    )
                )
            line["scans"] = scans

    # Add new transfer line
    if st.button("Add Transfer Line"):
        lines.append({
            "item_code": "", "quantity": 1, "pallet_qty": 1,
            "from_location": "", "from_warehouse": "",
            "to_location": "", "to_warehouse": "",
            "note": "", "scans": []
        })
        st.session_state["transfer_lines"] = lines
        st.experimental_rerun()

    # Submission
    if st.button("Submit Transfer"):
        errors = []
        all_scans = []
        totals = defaultdict(int)
        # Aggregate quantities by source
        for ln in lines:
            totals[(ln["item_code"], ln["from_warehouse"], ln["from_location"])] += ln["quantity"]
        # Check availability
        for (it, wh, loc), qty in totals.items():
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(quantity,0) FROM current_inventory WHERE warehouse=%s AND location=%s AND item_code=%s",
                    (wh, loc, it)
                )
                avail = cur.fetchone()[0]
            if qty > avail:
                errors.append(f"Insufficient stock of {it} at {loc} in {wh}: have {avail}, need {qty}.")
        # Detailed per-line validation
        for idx, ln in enumerate(lines):
            it, qty = ln["item_code"], ln["quantity"]
            floc, tloc = ln["from_location"], ln["to_location"]
            fwh, twh = ln["from_warehouse"], ln["to_warehouse"]
            # Check fields
            if not it or qty<=0 or not floc or not tloc:
                errors.append(f"Line {idx+1}: missing item, quantity, or locations.")
            # Validate locations exist and map to warehouses
            for loc, wh, lbl in [(floc,fwh,'from'),(tloc,twh,'to')]:
                with get_db_cursor() as cur:
                    cur.execute("SELECT warehouse FROM locations WHERE location_code=%s", (loc,))
                    res = cur.fetchone()
                if not res:
                    errors.append(f"Line {idx+1}: {lbl} location '{loc}' not found.")
                elif res[0] != wh:
                    errors.append(f"Line {idx+1}: {lbl} location '{loc}' belongs to {res[0]}, not {wh}.")
            # Scan count
            expected = math.ceil(qty/ln['pallet_qty'])
            scans = [s.strip() for s in ln['scans']]
            if len(scans)!=expected or any(not s for s in scans):
                errors.append(f"Line {idx+1}: expected {expected} scans for {it}.")
            # Collect scans for duplicates
            all_scans.extend(scans)
        # Duplicate scan IDs
        dup = [s for s,c in Counter(all_scans).items() if c>1]
        if dup:
            errors.append("Duplicate scan IDs: "+", ".join(dup))
        # Report errors
        if errors:
            st.error("\n".join(errors))
            return
        # Execute transfer
        try:
            with get_db_cursor() as cur:
                for ln in lines:
                    it, qty = ln['item_code'], ln['quantity']
                    floc, tloc = ln['from_location'], ln['to_location']
                    fwh, twh = ln['from_warehouse'], ln['to_warehouse']
                    # Transaction record
                    cur.execute(
                        "INSERT INTO transactions (transaction_type,item_code,quantity,date,from_location,to_location,user_id,note,warehouse) "
                        "VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s)",
                        ('Transfer', it, qty, floc, tloc, st.session_state.user, ln['note'], twh)
                    )
                    # Update inventories
                    cur.execute(
                        "UPDATE current_inventory SET quantity=quantity-%s WHERE warehouse=%s AND location=%s AND item_code=%s",
                        (qty,fwh,floc,it)
                    )
                    cur.execute(
                        "INSERT INTO current_inventory (warehouse,location,item_code,quantity) VALUES(%s,%s,%s,%s) "
                        "ON CONFLICT (warehouse,location,item_code) DO UPDATE SET quantity=current_inventory.quantity+EXCLUDED.quantity",
                        (twh,tloc,it,qty)
                    )
                    # Record scans
                    for s in ln['scans']:
                        cur.execute(
                            "INSERT INTO scan_verifications (item_code,scan_time,scan_id,location,transaction_type,warehouse,scanned_by) "
                            "VALUES (%s,NOW(),%s,%s,%s,%s,%s)",
                            (it,s,tloc,'Transfer',twh,st.session_state.user)
                        )
                        cur.execute(
                            "INSERT INTO current_scan_location (scan_id,item_code,location,updated_at) VALUES(%s,%s,%s,NOW()) "
                            "ON CONFLICT(scan_id) DO UPDATE SET item_code=EXCLUDED.item_code,location=EXCLUDED.location,updated_at=EXCLUDED.updated_at",
                            (s,it,tloc)
                        )
            st.success(random.choice(IRISH_TOASTS))
            if st.button("Done"):
                st.session_state.pop("transfer_lines",None)
                st.rerun()
        except Exception as e:
            st.error(f"Transfer failed: {e}")
