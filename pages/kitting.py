import streamlit as st
from fpdf import FPDF
import tempfile
from psycopg2 import IntegrityError
from db import (
    get_pulltag_rows,
    submit_kitting,
    get_db_cursor,
    insert_pulltag_line,
    update_pulltag_line,
    delete_pulltag_line,
)
import os

#Helper Functions:

#1) Generate PDF Function

def generate_finalize_summary_pdf(summary_data):

    output_path = os.path.join(tempfile.gettempdir(), "final_scan_summary.pdf")

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=10)

    pdf.cell(270, 10, txt="CRS Final Scan Summary Report", ln=True, align="C")
    pdf.ln(5)

    headers = ["Job Number", "Lot Number", "Item Code", "Description", "Scan ID", "Qty"]
    col_widths = [35, 30, 30, 100, 50, 20]

    for i, header in enumerate(headers):
        pdf.cell(col_widths[i], 10, header, border=1)
    pdf.ln()

    for row in summary_data:
        row_data = [
            row.get("job_number", ""),
            row.get("lot_number", ""),
            row.get("item_code", ""),
            row.get("item_description", ""),
            row.get("scan_id") or "- not scanned -",
            str(row.get("qty", 1))
        ]
        for i, val in enumerate(row_data):
            pdf.cell(col_widths[i], 10, str(val), border=1)
        pdf.ln()

    pdf.output(output_path)
    return output_path


#2) Scan Verification -> Inventory upserts
def finalize_scans(scans_needed, scan_inputs, job_lot_queue, from_location, to_location=None,
                   scanned_by=None, progress_callback=None):

    total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
    expected_count = total_scans
    actual_count = len(scan_inputs)

    if actual_count != expected_count:
        raise Exception(f"Expected {expected_count} scans but received {actual_count}. Please recheck scan list.")

    #flat_scan_list = list(scan_inputs.values())
    #done = 0

    with get_db_cursor() as cur:
        # Group scan_inputs by item_code
        scans_by_item = {}
        for k, sid in scan_inputs.items():
            if isinstance(k, str):
                parts = k.split("_")
                if len(parts) >= 3:
                    item = parts[2]
                    scans_by_item.setdefault(item, []).append(sid.strip())
        
        # Process per item_code
        for item_code, lots in scans_needed.items():
            scan_list = scans_by_item.get(item_code, [])
            scan_index = 0
        
            total_needed = sum(lots.values())
            for (job, lot), need in lots.items():
                assign = min(need, total_needed)
                if assign == 0:
                    continue
        
                if from_location and not to_location:
                    trans_type = "Job Issue"
                    loc_field, loc_value = "from_location", from_location
                    qty = assign
                elif to_location and not from_location:
                    trans_type = "Return"
                    loc_field, loc_value = "to_location", to_location
                    qty = abs(assign)
                else:
                    raise ValueError("finalize_scans requires exactly one of from/to_location")
        
                cur.execute("""
                    SELECT warehouse FROM pulltags
                    WHERE job_number = %s AND lot_number = %s AND item_code = %s
                    LIMIT 1
                """, (job, lot, item_code))
                warehouse = cur.fetchone()[0]
                sb = scanned_by
        
                cur.execute(f"""
                    INSERT INTO transactions (
                        transaction_type, date, warehouse, {loc_field},
                        job_number, lot_number, item_code, quantity, user_id
                    ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """, (trans_type, warehouse, loc_value, job, lot, item_code, qty, sb))
        
                for idx in range(1, qty + 1):
                    if scan_index >= len(scan_list):
                        raise Exception(f"Not enough scans for item {item_code} — expected {qty}, got {scan_index}")
                    sid = scan_list[scan_index]
                    scan_index += 1
                    if not sid:
                        raise Exception(f"Missing scan ID for {item_code} #{idx} in {job}-{lot}")
        
                    cur.execute("SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s AND transaction_type = 'Job Issue'", (sid,))
                    issues = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s AND transaction_type = 'Return'", (sid,))
                    returns = cur.fetchone()[0]
        
                    if trans_type == "Job Issue" and issues - returns > 0:
                        raise Exception(f"Scan {sid} already issued.")
                    elif trans_type == "Return" and issues > 0 and returns >= issues:
                        raise Exception(f"Scan {sid} already returned.")
        
                    try:
                        cur.execute("""
                            INSERT INTO scan_verifications (
                                item_code, scan_id, job_number, lot_number,
                                scan_time, location, transaction_type, warehouse, scanned_by
                            ) VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                        """, (item_code, sid, job, lot, loc_value, trans_type, warehouse, sb))
                    except IntegrityError:
                        raise Exception(f"Duplicate scan ID '{sid}' detected — already logged.")
        
                    if trans_type == "Return":
                        cur.execute("""
                            INSERT INTO current_scan_location (scan_id, item_code, location)
                            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                        """, (sid, item_code, loc_value))
                    else:
                        cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))
        
                    if progress_callback:
                        pct = int((scan_index / total_scans) * 100)
                        progress_callback(pct)
        
                cur.execute("""
                    UPDATE pulltags
                    SET status = %s
                    WHERE job_number = %s AND lot_number = %s AND item_code = %s
                """, ('kitted' if trans_type == 'Job Issue' else 'returned', job, lot, item_code))
        
                delta = qty if trans_type == "Return" else -qty
                cur.execute("""
                    INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                    SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, (item_code, loc_value, delta, warehouse))
        
                total_needed -= qty
                if total_needed <= 0:
                    break


# -----------------------------------------------------------------------------
# Main Streamlit App Entry
# -----------------------------------------------------------------------------

def run():
    st.markdown(
        """
        <style>
        html, body, [data-testid="stAppViewContainer"] {
            overflow-y: scroll !important;
        }
        ::-webkit-scrollbar {
            width: 12px;
        }
        ::-webkit-scrollbar-track {
            background: #f0f0f0;
        }
        ::-webkit-scrollbar-thumb {
            background-color: #888;
            border-radius: 6px;
            border: 3px solid #f0f0f0;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #555;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


    st.title("📦 Job Kitting")
    if st.button("🔄 Reset Page"):
        for key in ["job_lot_queue", "kitting_inputs"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()



    tx_type = st.selectbox(
        "Transaction Type",
        ["Issue", "Return"],
        help="Select 'Issue' to pull stock from this location, or 'Return' to credit it here."
    )
    location = st.text_input(
        "Location",
        help="If Issue: this is your from_location; if Return: this is your to_location."
    ).strip()

    if 'job_lot_queue' not in st.session_state:
        st.session_state.job_lot_queue = []
    if 'kitting_inputs' not in st.session_state:
        st.session_state.kitting_inputs = {}

    with st.form("add_joblot", clear_on_submit=True):
        job = st.text_input("Job Number")
        lot = st.text_input("Lot Number")
        if st.form_submit_button("Add Job/Lot"):
            if job and lot:
                pair = (job.strip(), lot.strip())
                if pair not in st.session_state.job_lot_queue:
                    st.session_state.job_lot_queue.append(pair)
                else:
                    st.warning("This Job/Lot is already queued.")
            else:
                st.error("Both Job Number and Lot Number are required.")

    for job, lot in st.session_state.job_lot_queue:
        st.markdown(f"---\n**Job:** {job} | **Lot:** {lot}")

        if st.button(f"Remove {lot}", key=f"remove_{job}_{lot}"):
            st.session_state.job_lot_queue = [p for p in st.session_state.job_lot_queue if p != (job, lot)]
            st.rerun()

        st.markdown("### ➕ Add New Kitted Item")
        with st.form(f"add_new_line_{job}_{lot}", clear_on_submit=True):
            new_code = st.text_input(
                "Item Code", 
                placeholder="Scan or type item_code",
                key=f"new_code_{job}_{lot}"
            )
            new_qty = st.number_input(
                "Quantity Kitted", 
                min_value=1, 
                step=1,
                key=f"new_qty_{job}_{lot}"
            )
            add_clicked = st.form_submit_button("Add Item")

        if add_clicked:
            inserted = False
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT item_description FROM items_master WHERE item_code = %s",
                    (new_code,)
                )
                row = cur.fetchone()
                if row:
                    adjusted_qty = -abs(new_qty) if tx_type == "Return" else new_qty
                    insert_pulltag_line(
                        cur,
                        job,
                        lot,
                        new_code,
                        adjusted_qty,
                        location,
                        transaction_type="Job Issue" if tx_type == "Issue" else "Return"
                    )
                    inserted = True
            if not inserted:
                st.error(f"`{new_code}` not found in items_master!")
            else:
                st.success(f"Added {new_qty} × `{new_code}` to {job}-{lot}.")
                st.rerun()

        rows = [r for r in get_pulltag_rows(job, lot) if r['transaction_type'] in ('Job Issue', 'Return')]

        if not rows:
            st.info("No pull-tags found for this combination.")
            continue

        # Only block edits if it's an Issue and the job/lot is already kitted/processed
        if tx_type == "Issue" and any(r['status'] in ('kitted', 'processed') for r in rows):
            st.warning(f"Job {job}, Lot {lot} is locked from Issue edits (status: kitted/processed).")
            continue


        headers = ["Code", "Desc", "Req", "UOM", "Kit", "Cost Code", "Status"]
        with st.container():
            cols = st.columns([1, 3, 1, 1, 1, 1, 1])
            for col, hdr in zip(cols, headers):
                col.markdown(f"**{hdr}**")
            #
        MAX_PULLTAGS = 30
        visible_rows = rows[-MAX_PULLTAGS:]
        if len(rows) > MAX_PULLTAGS:
            st.info(f"Showing last {MAX_PULLTAGS} of {len(rows)} pulltags.")

        headers = ["Code", "Desc", "Req", "UOM", "Kit", "Cost Code", "Status"]
        with st.container():
            cols = st.columns([1, 3, 1, 1, 1, 1, 1])
            for col, hdr in zip(cols, headers):
                col.markdown(f"**{hdr}**")

            for row in visible_rows:
                cols = st.columns([1, 3, 1, 1, 1, 1, 1])
                cols[0].write(row['item_code'])
                cols[1].write(row['description'])
                cols[2].write(row['qty_req'])
                cols[3].write(row['uom'])

                key = f"kit_{job}_{lot}_{row['item_code']}_{row['id']}"
                min_qty = -999 if tx_type == "Return" else 0
                default = max(row['qty_req'], min_qty)

                if st.checkbox(f"Edit Qty", key=f"edit_{job}_{lot}_{row['id']}", value=False):
                    st.session_state.kitting_inputs[key] = cols[4].number_input(
                        label="Kit Qty",
                        min_value=min_qty,
                        max_value=999,
                        value=st.session_state.kitting_inputs.get(key, default),
                        key=key,
                        label_visibility="collapsed"
                    )
                else:
                    cols[4].write(st.session_state.kitting_inputs.get(key, default))

                cols[5].write(row['cost_code'])
                cols[6].write(row['status'])

        if st.button(f"Submit Kitting for {job}-{lot}", key=f"submit_{job}_{lot}"):

            kits = {}
            for k, qty in st.session_state.kitting_inputs.items():
                if isinstance(k, str) and k.startswith("kit_"):
                    parts = k.split("_")
                    if len(parts) >= 5:
                        _, j, l, code, pid = parts
                        if j == job and l == lot:
                            kits[(j, l, code, int(pid))] = qty


            with get_db_cursor() as cur:
                existing = [r['item_code'] for r in rows]

                for (j, l, code, pid), qty in kits.items():
                    if code not in existing and qty != 0:
                        adjusted_qty = -abs(qty) if tx_type == "Return" else abs(qty)
                        insert_pulltag_line(cur, job, lot, code, adjusted_qty, transaction_type="Job Issue" if tx_type == "Issue" else "Return")

                for r in rows:
                    new_qty = kits.get((job, lot, r['item_code'], r['id']), r['qty_req'])
                    adjusted_qty = -abs(new_qty) if tx_type == "Return" else new_qty
                    if adjusted_qty == 0 and r['status'] != 'returned' and r['transaction_type'] != 'RETURNB':
                        delete_pulltag_line(cur, r['id'])

                    elif adjusted_qty != r['qty_req']:
                        update_pulltag_line(cur, r['id'], adjusted_qty)
                        
                st.success(f"Kitting updated for {job}-{lot}.")
                            
    scans_needed = {}
    for job, lot in st.session_state.job_lot_queue:
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT p.item_code, p.quantity
                FROM pulltags p
                JOIN items_master im ON p.item_code = im.item_code
                WHERE p.job_number = %s
                  AND p.lot_number = %s
                  AND im.scan_required = TRUE
                  AND p.transaction_type IN ('Job Issue', 'Return')  -- exclude ADD, RETURNB

                """,
                (job, lot)
            )
            for item_code, qty in cur.fetchall():
                scans_needed.setdefault(item_code, {}).setdefault((job, lot), 0)
                scans_needed[item_code][(job, lot)] += abs(qty)

    MAX_SCAN_DISPLAY = 20

    st.markdown("---")
    st.subheader("🔍 Live Scan Buffer")

    # Initialize scan buffer
    if "scan_buffer" not in st.session_state:
        st.session_state.scan_buffer = []

    def commit_scan():
        val = st.session_state.scan_live.strip()
        if val:
            # Attach scan with inferred metadata here
            st.session_state.scan_buffer.append(("UNK", "UNK", "UNK", val))
            st.session_state.scan_live = ""

    st.text_input("📷 Scan Item Here", key="scan_live", on_change=commit_scan)

    if scans_needed:
        st.subheader("🧾 Items Needing Scan")
        for item_code, job_lots in scans_needed.items():
            for (job, lot), qty in job_lots.items():
                st.markdown(f"- `{item_code}` for **Job {job} / Lot {lot}** — **{qty}** scan(s) needed")
    else:
        st.info("No items require scanning for the selected Job/Lot.")


    # Display recent scans
    if st.session_state.scan_buffer:
        st.caption(f"Last {min(len(st.session_state.scan_buffer), MAX_SCAN_DISPLAY)} of {len(st.session_state.scan_buffer)} scans:")
        for idx, scan in enumerate(st.session_state.scan_buffer[-MAX_SCAN_DISPLAY:], 1):
            st.text(f"{idx}. {scan}")
    else:
        st.info("No scans yet. Start by scanning items above.")

    # Finalize Scans (maps scan_buffer into scan_inputs dict)
    if st.button("Finalize Scans"):
        if not location:
            st.error("Please enter a Location before finalizing scans.")
        else:
            scan_inputs = {}
            for idx, (job, lot, item_code, sid) in enumerate(st.session_state.scan_buffer, 1):
                scan_inputs[f"{job}_{lot}_{item_code}_{idx}"] = sid



            sb = st.session_state.user
            progress_bar = st.progress(0)
            with st.spinner("Processing scans…"):
                def update_progress(pct: int):
                    progress_bar.progress(pct)

                if tx_type == "Issue":
                    finalize_scans(
                        scans_needed,
                        scan_inputs,
                        st.session_state.job_lot_queue,
                        from_location=location,
                        to_location=None,
                        scanned_by=sb,
                        progress_callback=update_progress
                    )
                else:
                    finalize_scans(
                        scans_needed,
                        scan_inputs,
                        st.session_state.job_lot_queue,
                        from_location=None,
                        to_location=location,
                        scanned_by=sb,
                        progress_callback=update_progress
                    )

            st.success("Scans processed and inventory updated.")

            summary_path = os.path.join(tempfile.gettempdir(), "final_scan_summary.pdf")
            if os.path.exists(summary_path):
                with open(summary_path, "rb") as f:
                    st.download_button(
                        label="📄 Download Final Scan Summary",
                        data=f,
                        file_name="final_scan_summary.pdf",
                        mime="application/pdf"
                    )
                st.info("You may now download the summary.")

