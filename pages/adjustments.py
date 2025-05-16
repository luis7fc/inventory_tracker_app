import streamlit as st
from datetime import datetime
from db import get_db_cursor
from config import WAREHOUSES

# Updated insert_pulltag_line with warehouse and kitted status
def insert_pulltag_line(cur, job_number, lot_number, item_code, quantity, transaction_type="Job Issue", warehouse=None, status="kitted"):
    sql = """
    INSERT INTO pulltags
      (job_number, lot_number, item_code, quantity,
       description, cost_code, uom, status, transaction_type, warehouse)
    SELECT
      %s,        -- job_number
      %s,        -- lot_number
      item_code,
      %s,        -- quantity
      item_description,
      cost_code,
      uom,
      %s,        -- status
      %s,        -- transaction_type
      %s         -- warehouse
    FROM items_master
    WHERE item_code = %s
    RETURNING id
    """
    cur.execute(sql, (job_number, lot_number, quantity, status, transaction_type, warehouse, item_code))
    return cur.fetchone()[0]

# Enhanced finalize_add with scan validation logic and pallet logic
def finalize_add(scans_needed, scan_inputs, job_lot_queue, from_location, to_location=None, scanned_by=None, progress_callback=None, warehouse=None):
    if not warehouse:
        raise ValueError("Warehouse was not provided to finalize_add.")

    total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
    done = 0

    with get_db_cursor() as cur:
        for item_code, lots in scans_needed.items():
            total_needed = sum(lots.values())

            for (job, lot), need in lots.items():
                assign = min(need, total_needed)
                if assign == 0:
                    continue

                trans_type = "Return" if to_location and not from_location else "Job Issue"
                loc_value = to_location or from_location
                sb = scanned_by

                cur.execute(f"""
                    INSERT INTO transactions
                        (transaction_type, date, warehouse, {"to_location" if trans_type == "Return" else "from_location"},
                         job_number, lot_number, item_code, quantity, user_id)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """, (trans_type, warehouse, loc_value, job, lot, item_code, assign, sb))

                pallet_id = scan_inputs.get(f"pallet_{item_code}_{job}_{lot}", "").strip()
                pallet_qty = int(scan_inputs.get(f"pallet_qty_{item_code}_{job}_{lot}", "1"))

                if pallet_id and pallet_qty > 1 and trans_type == "Job Issue":
                    cur.execute("SELECT location FROM current_scan_location WHERE scan_id = %s", (pallet_id,))
                    found = cur.fetchone()
                    if not found:
                        raise Exception(f"Pallet ID {pallet_id} not found in current_scan_location.")
                    if found[0] != from_location:
                        raise Exception(f"Pallet ID {pallet_id} is currently in {found[0]}, not {from_location}.")
                    cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (pallet_id,))
                    cur.execute("""
                        INSERT INTO scan_verifications
                          (item_code, scan_id, job_number, lot_number,
                           scan_time, location, transaction_type, warehouse, scanned_by)
                        VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                    """, (item_code, pallet_id, job, lot, from_location, trans_type, warehouse, sb))
                    done += 1
                else:
                    for idx in range(1, assign + 1):
                        sid = scan_inputs.get(f"scan_{item_code}_{idx}", "").strip()
                        if not sid:
                            continue

                        cur.execute("SELECT location FROM current_scan_location WHERE scan_id = %s", (sid,))
                        existing = cur.fetchone()

                        if trans_type == "Return":
                            if existing:
                                raise Exception(f"Scan {sid} already exists in {existing[0]}.")
                            cur.execute("""
                                INSERT INTO current_scan_location (scan_id, item_code, location)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (scan_id) DO UPDATE SET location = EXCLUDED.location
                            """, (sid, item_code, to_location))
                        else:
                            if existing and existing[0] != from_location:
                                raise Exception(f"Scan {sid} located in {existing[0]}, expected {from_location}. Missing internal movement?")
                            cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                        cur.execute("""
                            INSERT INTO scan_verifications
                              (item_code, scan_id, job_number, lot_number,
                               scan_time, location, transaction_type, warehouse, scanned_by)
                            VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                        """, (item_code, sid, job, lot, loc_value, trans_type, warehouse, sb))

                        done += 1
                        if progress_callback:
                            pct = int(done / total_scans * 100)
                            progress_callback(pct)

                delta = assign if trans_type == "Return" else -assign
                cur.execute("""
                    INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                        SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, (item_code, loc_value, delta, warehouse))

                total_needed -= assign
                if total_needed <= 0:
                    break

def run():
    st.title("🛠️ Post-Kitting Adjustments")

    transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"], help="Choose 'ADD' to allocate new material or 'RETURNB' to remove extras.")
    warehouse = st.selectbox("Warehouse", WAREHOUSES)
    location = st.text_input("Location", placeholder="e.g., STAGE-A")
    note = st.text_input("Transaction Note (Optional)")

    if "adjustments" not in st.session_state:
        st.session_state.adjustments = []

    with st.expander("➕ Add Adjustment Row"):
        job = st.text_input("Job Number")
        lot = st.text_input("Lot Number")
        code = st.text_input("Item Code")
        qty = st.number_input("Quantity", min_value=1, value=1, step=1)

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                with get_db_cursor() as cur:
                    cur.execute("SELECT item_description FROM items_master WHERE item_code = %s", (code.strip(),))
                    result = cur.fetchone()
                    desc = result[0] if result else "(Unknown Item)"
                st.session_state.adjustments.append({
                    "job_number": job.strip(),
                    "lot_number": lot.strip(),
                    "item_code": code.strip(),
                    "quantity": int(qty),
                    "description": desc
                })
            else:
                st.warning("Please complete all fields before adding.")

    if st.session_state.adjustments:
        st.markdown("### 📋 Adjustments Preview")
        for i, row in enumerate(st.session_state.adjustments):
            cols = st.columns([3, 3, 3, 2, 3, 1])
            cols[0].markdown(f"**Job:** {row['job_number']}")
            cols[1].markdown(f"**Lot:** {row['lot_number']}")
            cols[2].markdown(f"**Item:** {row['item_code']}")
            cols[3].markdown(f"**Qty:** {row['quantity']}")
            cols[4].markdown(f"**Desc:** {row['description']}")
            if cols[5].button("❌", key=f"remove_{i}"):
                st.session_state.adjustments.pop(i)
                st.experimental_rerun()

        if st.button("Submit Adjustments"):
            now = datetime.now()
            user = st.session_state.get("username", "unknown")
            scans_needed = {}
            job_lot_queue = []
            confirmed_rows = []

            for row in st.session_state.adjustments:
                job = row["job_number"]
                lot = row["lot_number"]
                code = row["item_code"]
                qty = row["quantity"]

                with get_db_cursor() as cur:
                    cur.execute("""
                        SELECT item_code, item_description FROM items_master 
                        WHERE item_code = %s AND cost_code = item_code
                    """, (code,))
                    result = cur.fetchone()

                    if not result:
                        st.info(f"ℹ️ Item {code} is not scan-tracked or not found. Skipped.")
                        continue

                    insert_pulltag_line(cur, job, lot, code, qty, transaction_type=transaction_type, warehouse=warehouse, status="kitted")

                job_lot_queue.append((job, lot))
                scans_needed.setdefault(code, {}).setdefault((job, lot), 0)
                scans_needed[code][(job, lot)] += qty
                confirmed_rows.append({"Job": job, "Lot": lot, "Item": code, "Qty": qty, "Type": transaction_type})

            if not scans_needed:
                st.warning("No valid adjustments submitted.")
                st.stop()

            st.markdown("### ✅ Valid Rows Submitted")
            st.dataframe(confirmed_rows, use_container_width=True)

            st.markdown("### 🔍 Scan Required Items")
with st.form("scan_form"):
    scan_inputs = {}
    for item_code, lots in scans_needed.items():
        for (job, lot), qty in lots.items():
            st.write(f"**{item_code} — Job: {job}, Lot: {lot} — Total Scans: {qty}**")
            scan_inputs[f"pallet_{item_code}_{job}_{lot}"] = st.text_input(f"Optional Pallet ID for {item_code} (Job {job}, Lot {lot})", key=f"pallet_{item_code}_{job}_{lot}")
            scan_inputs[f"pallet_qty_{item_code}_{job}_{lot}"] = st.number_input(
                f"Pallet Quantity (or 1 for individual scans)", min_value=1, value=1, step=1, key=f"pallet_qty_{item_code}_{job}_{lot}"
            )
            for i in range(1, qty + 1):
                key = f"scan_{item_code}_{i}"
                scan_inputs[key] = st.text_input(f"Scan {i} for {item_code}", key=key)

    submitted = st.form_submit_button("Finalize Adjustments")

if submitted:
                if not location:
                    st.error("Please enter a Location before finalizing.")
                else:
                    sb = st.session_state.get("username", "unknown")
                    progress_bar = st.progress(0)

                    with st.spinner("Processing adjustments..."):
                        def update_progress(pct: int):
                            progress_bar.progress(pct)

                        finalize_add(
                            scans_needed,
                            scan_inputs,
                            job_lot_queue,
                            from_location=location if transaction_type == "ADD" else None,
                            to_location=location if transaction_type == "RETURNB" else None,
                            scanned_by=sb,
                            progress_callback=update_progress,
                            warehouse=warehouse
                        )

                    st.success("✅ Adjustments finalized and inventory updated.")
                    st.session_state.adjustments.clear()
