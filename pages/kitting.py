import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
from fpdf import FPDF
import tempfile
from psycopg2 import IntegrityError
from db import (
    get_pulltag_rows,
    get_db_cursor,
    insert_pulltag_line,
    update_pulltag_line,
    delete_pulltag_line,
)
import os

EDIT_ANCHOR = "scan-edit"          # <--- add me near the top of the file (global)
#Helper Functions

#error message structure
def user_error(msg: str):
    st.error(f"❌ {msg}")
    st.stop()
    
#confirming scan location is sound    
def validate_scan_location(cur, scan_id, trans_type, expected_location=None):
    cur.execute("SELECT location FROM current_scan_location WHERE scan_id = %s", (scan_id,))
    row = cur.fetchone()
    if trans_type == "Job Issue":
        if not row:
            raise Exception(f"Scan {scan_id} is not registered to any location.")
        if row[0] != expected_location:
            raise Exception(f"Scan {scan_id} is at {row[0]}, not {expected_location}.")
    elif trans_type == "Return" and row:
        raise Exception(f"Scan {scan_id} is already assigned to location {row[0]}. Cannot return again.")
    elif trans_type not in ("Job Issue", "Return"):
        raise ValueError(f"Unsupported transaction type: {trans_type}")

def generate_finalize_summary_pdf(summary_data, verified_by=None, verified_on=None):

    output_path = os.path.join(tempfile.gettempdir(), "final_scan_summary.pdf")

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=10)
    pdf.cell(270, 10, txt="CRS Final Scan Summary Report", ln=True, align="C")
    pdf.ln(5)
    if verified_by or verified_on:
        user_text = f"Verified By: {verified_by or 'N/A'}"
        time_text = f"Date: {verified_on or 'N/A'}"
        pdf.cell(270, 10, f"{user_text} | {time_text}", ln=True, align="C")
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

#2) Scan Verification -> Inventory upserts -> pdf construct
def finalize_scans(scans_needed, scan_inputs, job_lot_queue, from_location, to_location=None,
                   scanned_by=None, progress_callback=None):
    total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
    actual_count = len(scan_inputs)
    if actual_count != total_scans:
        st.error(f"❌  Expected **{total_scans}** scans but received **{actual_count}**.")
        st.stop()

    processed = 0
    with get_db_cursor() as cur:
        scans_by_item = {}
        for k, sid in scan_inputs.items():
            parts = k.split("_")
            if len(parts) >= 3:
                scans_by_item.setdefault(parts[2], []).append(sid.strip())

        for item_code, lots in scans_needed.items():
            scan_list = scans_by_item.get(item_code, [])
            scan_index = 0
            total_needed = sum(lots.values())
            for (job, lot), need in lots.items():
                qty = need
                if from_location and not to_location:
                    trans_type = "Job Issue"
                    loc_field, loc_value = "from_location", from_location
                elif to_location and not from_location:
                    trans_type = "Return"
                    loc_field, loc_value = "to_location", to_location
                else:
                    raise ValueError("Must provide either from_location or to_location")

                cur.execute("SELECT warehouse FROM pulltags WHERE job_number = %s AND lot_number = %s AND item_code = %s LIMIT 1", (job, lot, item_code))
                warehouse = cur.fetchone()[0]

                cur.execute(f"""
                    INSERT INTO transactions (
                        transaction_type, date, warehouse, {loc_field},
                        job_number, lot_number, item_code, quantity, user_id
                    ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """, (trans_type, warehouse, loc_value, job, lot, item_code, qty, scanned_by))

                for idx in range(1, qty + 1):
                    if scan_index >= len(scan_list):
                        raise Exception(f"Not enough scans for {item_code}")
                    sid = scan_list[scan_index]
                    scan_index += 1
                    validate_scan_location(cur, sid, trans_type, from_location if trans_type == "Job Issue" else None)

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
                        """, (item_code, sid, job, lot, loc_value, trans_type, warehouse, scanned_by))
                    except IntegrityError:
                        raise Exception(f"Duplicate scan ID '{sid}' detected")

                    if trans_type == "Return":
                        cur.execute("INSERT INTO current_scan_location (scan_id, item_code, location) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (sid, item_code, loc_value))
                    else:
                        cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                    processed += 1
                    if progress_callback:
                        progress_callback(int((processed / total_scans) * 100))

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
    
    st.title("📦 Job Kitting")
    if st.button("🔄 Reset Page"):
        for key in ["job_lot_queue", "kitting_inputs", "scan_buffer","scan_live"]:
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
                        transaction_type="Job Issue" if tx_type == "Issue" else "Return",
                        note="Updated"
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
        if tx_type == "Issue" and any(r['status'] in ('kitted', 'exported') for r in rows):
            st.warning(f"Job {job}, Lot {lot} is locked from Issue edits (status: kitted/exported).")
            continue

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
                            kits[(j, l, code, pid)] = qty  # treat pulltag ID as string, not int

            with get_db_cursor() as cur:
                existing = [r['item_code'] for r in rows]

                for (j, l, code, pid), qty in kits.items():
                    if code not in existing and qty != 0:
                        adjusted_qty = -abs(qty) if tx_type == "Return" else abs(qty)
                        insert_pulltag_line(cur, job, lot, code, adjusted_qty, transaction_type="Job Issue" if tx_type == "Issue" else "Return", note="Updated")

                for r in rows:
                    new_qty = kits.get((job, lot, r['item_code'], r['id']), r['qty_req'])
                    adjusted_qty = -abs(new_qty) if tx_type == "Return" else new_qty
                    if adjusted_qty == 0 and r['status'] != 'returned' and r['transaction_type'] != 'RETURNB':
                        delete_pulltag_line(cur, r['id'])

                    elif adjusted_qty != r['qty_req']:
                        update_pulltag_line(cur, r['id'], adjusted_qty)

                bulk_status = "kitted" if tx_type == "Issue" else "returned"

                cur.execute("""
                    UPDATE pulltags
                    SET status = %s
                    WHERE job_number = %s
                      AND lot_number = %s
                      AND transaction_type = %s
                      AND status NOT IN ('kitted', 'returned')
                """, (
                    bulk_status,
                    job,
                    lot,
                    "Job Issue" if tx_type == "Issue" else "Return"
                ))        
                        
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

    # Init scan buffer if not present
    if "scan_buffer" not in st.session_state:
        st.session_state.scan_buffer = []
    
    # Ensure buffer only contains valid 4-tuples
    st.session_state.scan_buffer = [
        entry for entry in st.session_state.scan_buffer
        if isinstance(entry, tuple) and len(entry) == 4
    ]
    
    # Safely count matching scans
    next_item = None
    for item_code, job_lots in scans_needed.items():
        total_needed = sum(job_lots.values())
        total_scanned = len([
            sid for entry in st.session_state.scan_buffer
            for job, lot, code, sid in [entry]
            if code == item_code
        ])
        if total_scanned < total_needed:
            next_item = item_code
            remaining = total_needed - total_scanned
            break
        
    # UI toggle
    st.markdown(f"<div id='{EDIT_ANCHOR}'></div>", unsafe_allow_html=True)
    edit_mode = st.toggle("✏️ Edit Scan Entries", value=False)
    
    # Editable Table View
    if edit_mode:
        st.subheader("✏️ Edit Scans Before Verifying")
        for i, (job, lot, item_code, sid) in enumerate(st.session_state.scan_buffer):
            cols = st.columns([1, 2, 2])
            cols[0].write(item_code)
            new_sid = cols[1].text_input(f"Scan {i+1}", value=sid, key=f"edit_scan_{i}")
            if cols[2].button("❌ Remove", key=f"remove_scan_{i}"):
                st.session_state.scan_buffer.pop(i)
                st.rerun()
            else:
                st.session_state.scan_buffer[i] = (job, lot, item_code, new_sid.strip())
    
    else:
        # Guided Scan UI
        if next_item:
            st.markdown(f"### 🔄 Scan item: **`{next_item}`** ({remaining} remaining)")
        else:
            st.success("✅ All required scans collected.")
    
        def commit_scan_guided():
            val = st.session_state.scan_live.strip()
            if val and next_item:
                if st.session_state.job_lot_queue:
                    job, lot = st.session_state.job_lot_queue[0]
                else:
                    job, lot = "UNK", "UNK"
                
                st.session_state.scan_buffer.append((job, lot, next_item, val))

                st.session_state.scan_live = ""
    
        st.text_input("📷 Scan Item Here", key="scan_live", on_change=commit_scan_guided)
    
        if st.session_state.scan_buffer:
            st.caption("Recent scans:")
            for idx, (_, _, item_code, sid) in enumerate(st.session_state.scan_buffer[-10:], 1):
                st.text(f"{idx}. {item_code} → {sid}")
    
        # Finalize Scans only if not editing
        if st.button("✅ Verify Scans"):
            if not location:
                st.error("Please enter a Location before verifying.")
            else:
                scan_inputs = {}
                for idx, (job, lot, item_code, sid) in enumerate(st.session_state.scan_buffer, 1):
                    scan_inputs[f"{job}_{lot}_{item_code}_{idx}"] = sid
    
                sb = st.session_state.get("user","unknown")
                progress_bar = st.progress(0)
                with st.spinner("Processing scans…"):
                    def update_progress(pct: int):
                        progress_bar.progress(pct)
                    try:
                        
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
                    except Exception as e:
                        #Friendly banner + link that scrolls the user back to the edit box
                        st.error(
                            f"❌ {e}  \n\n"
                            f"[➡️ Jump to **Edit Scan Entries**](#{EDIT_ANCHOR})"
                        )
                        # forces the browser to respect the anchor even if the banner is low down
                        st.experimental_set_query_params(anchor=EDIT_ANCHOR)
                        st.stop()
                            
                st.success("Scans verified and inventory updated.")
               
                item_map = {}
                with get_db_cursor() as cur:
                    cur.execute("SELECT item_code, item_description FROM items_master")
                    item_map = dict(cur.fetchall())

                # Create lookup from scan_buffer
                scan_map = {
                    (j, l, code): sid
                    for j, l, code, sid in st.session_state.scan_buffer
                }
                
                # Build a list of rows: one per scan_id
               # summary_rows = []
                #for (job, lot, item_code, scan_id) in st.session_state.scan_buffer:
                 #   for r in get_pulltag_rows(job, lot):
                  #      if r["item_code"] == item_code and r["transaction_type"] in ("Job Issue", "Return"):
                   #         summary_rows.append({
                    #            "job_number": r["job_number"],
                     #           "lot_number": r["lot_number"],
                      #          "item_code": r["item_code"],
                       #         "item_description": r["description"],
                        #        "scan_id": scan_id,
                         #       "qty": 1
                          #  })

                #Summary Rows for all items including non-scanned
                pulls = get_pulltag_rows(job, lot)
                scan_lookup = {}
                for j, l, code, sid in st.session_state.scan_buffer:
                    scan_lookup.setdefault((j, l, code), []).append(sid)
                 
                summary_rows = []
                for r in pulls:
                    if r["transaction_type"] not in ("Job Issue", "Return"):
                        continue
                    key = (r["job_number"], r["lot_number"], r["item_code"])
                    sid_list = scan_lookup.get(key, [])
                 
                    if sid_list:                               # one row *per* scan
                        for sid in sid_list:
                            summary_rows.append({
                                "job_number": r["job_number"],
                                "lot_number": r["lot_number"],
                                "item_code": r["item_code"],
                                "item_description": r["description"],
                                "scan_id": sid,
                                "qty": 1
                            })
                    else:                                      # single row for non-scanned line
                        summary_rows.append({
                            "job_number": r["job_number"],
                            "lot_number": r["lot_number"],
                            "item_code": r["item_code"],
                            "item_description": r["description"],
                            "scan_id": "- not scanned -",
                            "qty": abs(r["qty_req"])
                        })
                #Generate PDF using full pulltag data
                generate_finalize_summary_pdf(
                    summary_rows,
                    verified_by=sb,
                    verified_on=datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M")
                )

                summary_path = os.path.join(tempfile.gettempdir(), "final_scan_summary.pdf")
                if os.path.exists(summary_path):
                    st.success("✅ Scan summary ready! You can download the PDF below.")
                    with open(summary_path, "rb") as f:
                        st.download_button(
                            label="📄 Download Final Scan Summary",
                            data=f,
                            file_name="final_scan_summary.pdf",
                            mime="application/pdf"
                        )
