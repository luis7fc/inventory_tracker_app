import streamlit as st
from datetime import datetime
from db import get_db_cursor, insert_pulltag_line
from config import WAREHOUSES

def finalize_add(scans_needed, scan_inputs, job_lot_queue, from_location, to_location=None, scanned_by=None, progress_callback=None):
    """
    Process scans for Job Issues, Returns, and (if needed) Internal Movements:
    - Insert into transactions (with dynamic from/to location)
    - Insert into scan_verifications (+ current_scan_location for Returns)
    - Update current_inventory (+/– based on transaction type)
    """
    #compute total work for progress reporting
    total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
    done = 0
    
    with get_db_cursor() as cur:
        for item_code, lots in scans_needed.items():
            total_needed = sum(lots.values())

            for (job, lot), need in lots.items():
                assign = min(need, total_needed)
                if assign == 0:
                    continue

                # 1) Determine transaction type, qty, and which location field/value to use
                if from_location is not None and to_location is None:
                    trans_type = "Job Issue"
                    loc_field  = "from_location"
                    loc_value  = from_location
                    qty        = assign
                elif to_location is not None and from_location is None:
                    trans_type = "Return"
                    loc_field  = "to_location"
                    loc_value  = to_location
                    qty        = abs(assign)

                else:
                    raise ValueError(
                        "finalize_scans: must provide one of from or to location"
                        )

                # 2) Fetch warehouse from pulltags
                cur.execute(
                    "SELECT warehouse "
                    "  FROM pulltags "
                    " WHERE job_number = %s AND lot_number = %s AND item_code = %s "
                    " LIMIT 1",
                    (job, lot, item_code)
                )
                wh = cur.fetchone()
                warehouse = wh[0] if wh else None
                sb = st.session_state.user
                sb = scanned_by

                # 3) Insert into transactions with dynamic location column
                sql = f"""
                    INSERT INTO transactions
                        (transaction_type,
                         date,
                         warehouse,
                         {loc_field},
                         job_number,
                         lot_number,
                         item_code,
                         quantity,
                         user_id)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql,
                    (trans_type,
                     warehouse,
                     loc_value,
                     job,
                     lot,
                     item_code,
                     qty,
                     sb)
                )

                # 4) Insert each scan into scan_verifications (and current_scan_location for Returns)
                # 4) Insert each scan into scan_verifications (and current_scan_location for Returns)
                for idx in range(1, qty + 1):
                    sid = scan_inputs.get(f"scan_{item_code}_{idx}", "").strip()

                    #sanity-check input
                    if not sid:
                        raise Exception(f"Missing scan ID for {item_code} #{idx}")
                    
                    # use the passed-in scanned_by instead of session access
                    sb = scanned_by  

                    # determine how many times this scan has been issued vs returned
                    cur.execute(
                        "SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s AND transaction_type = 'Job Issue'",
                        (sid,)
                    )
                    issues = cur.fetchone()[0]
                    cur.execute(
                        "SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s AND transaction_type = 'Return'",
                        (sid,)
                    )
                    returns = cur.fetchone()[0]

                    # guard against over-issuing or over-returning
                    if trans_type == "Job Issue":
                        if issues - returns > 0:
                            raise Exception(f"Scan {sid} already issued; return required before reuse.")
                    else:  # Return
                        # allow first return even without prior issues; only block if all recorded issues have been returned
                        if issues > 0 and returns >= issues:
                            raise Exception(f"Scan {sid} already fully returned; cannot return again.")

                    # record the scan (now with timestamp and location)
                    cur.execute(
                        """
                        INSERT INTO scan_verifications
                          (item_code, scan_id, job_number, lot_number,
                           scan_time, location, transaction_type, warehouse, scanned_by)
                        VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                        """,
                        (item_code, sid, job, lot, loc_value, trans_type, warehouse, sb)
                    )

                    if trans_type == "Return":
                        #record that this scan is now back at loc_value
                        cur.execute(
                            """
                            INSERT INTO current_scan_location
                              (scan_id,item_code, location)
                            VALUES (%s,%s,%s)
                            ON CONFLICT DO NOTHING
                            """,
                            (sid, item_code, loc_value)
                        )

                    elif trans_type == "Job Issue":
                        #remove it from current_scan_location, since it's being issued out of inventory
                        cur.execute(
                            "DELETE FROM current_scan_location WHERE scan_id = %s",
                            (sid,)
                        )

                    #bump the progress
                        done += 1
                        if progress_callback:
                            #give a 0-100 integer percent
                            pct = int(done/total_scans * 100)
                            progress_callback(pct)

                        
                #5) UPSERT into current_inventory: subtract for Issues, add for Returns
                delta = qty if trans_type == "Return" else -qty
                cur.execute(
                    """
                    INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                        SET quantity = current_inventory.quantity + EXCLUDED.quantity
                    """,
                    (item_code, loc_value, delta, warehouse,)
                )

                total_needed -= qty
                if total_needed <= 0:
                    break

def run():
    st.markdown("""
        <style>
        [data-testid="stSidebarNav"] { display: none; }
        </style>
    """, unsafe_allow_html=True)

    st.title("🛠️ Post-Kitting Adjustments")

    st.markdown("""
    Use this tool to **add or remove material after original kitting is complete**. 
    Designed for mobile and desktop — enter one row at a time.
    """)

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

                    insert_pulltag_line(cur, job, lot, code, qty, transaction_type=transaction_type)

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
            scan_inputs = {}
            for item_code, lots in scans_needed.items():
                total = sum(lots.values())
                st.write(f"**{item_code}** — Total Scans: {total}")
                for i in range(1, total + 1):
                    key = f"scan_{item_code}_{i}"
                    scan_inputs[key] = st.text_input(f"Scan {i} for {item_code}", key=key)

            if st.button("Finalize Adjustments"):
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
                            progress_callback=update_progress
                        )

                    st.success("✅ Adjustments finalized and inventory updated.")
                    st.session_state.adjustments.clear()
