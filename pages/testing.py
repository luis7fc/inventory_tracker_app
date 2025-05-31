#FOR TESTING NEW MODULES OR TESTING REFACTORS
# CRS Inventory Tracker – Job Kitting (Multilot Finalization with Scan Logic and Summary PDF)
import streamlit as st
import pandas as pd
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
 
EDIT_ANCHOR = "scan-edit"
 
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
 
def finalize_all():
    summary_rows = []
    user = st.session_state.get("user", "unknown")
    location = st.session_state.get("location", "")
    scan_buffer = st.session_state.get("scan_buffer", [])
    buffer_map = {}
    for job, lot, code, sid in scan_buffer:
        buffer_map.setdefault((job, lot, code), []).append(sid)
 
    for job, lot in st.session_state.job_lot_queue:
        df = st.session_state.pulltag_editor_df.get((job, lot))
        if df is None or df.empty:
            continue
        with get_db_cursor() as cur:
            for _, row in df.iterrows():
                item_code = row['item_code']
                qty = int(row['kitted_qty'])
                tx_type = row['transaction_type']
                warehouse = row.get('warehouse') or 'MAIN'
                loc_field = 'from_location' if tx_type == 'Job Issue' else 'to_location'
                loc_value = location
 
                cur.execute(f"""
                    INSERT INTO transactions (
                        transaction_type, date, warehouse, {loc_field},
                        job_number, lot_number, item_code, quantity, user_id
                    ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """, (tx_type, warehouse, loc_value, job, lot, item_code, qty, user))
 
                scan_list = buffer_map.get((job, lot, item_code), [])
                if row['scan_required'] and qty > 0:
                    if len(scan_list) != qty:
                        raise Exception(f"Scan mismatch for {item_code} in {job}-{lot}. Expected {qty}, found {len(scan_list)}")
 
                for sid in scan_list:
                    cur.execute("""
                        INSERT INTO scan_verifications (
                            item_code, scan_id, job_number, lot_number,
                            scan_time, location, transaction_type, warehouse, scanned_by
                        ) VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                    """, (item_code, sid, job, lot, loc_value, tx_type, warehouse, user))
 
                    if tx_type == "Return":
                        cur.execute("INSERT INTO current_scan_location (scan_id, item_code, location) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (sid, item_code, loc_value))
                    else:
                        cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))
 
                    summary_rows.append({
                        "job_number": job,
                        "lot_number": lot,
                        "item_code": item_code,
                        "item_description": row['description'],
                        "scan_id": sid,
                        "qty": 1
                    })
 
                if not scan_list:
                    summary_rows.append({
                        "job_number": job,
                        "lot_number": lot,
                        "item_code": item_code,
                        "item_description": row['description'],
                        "scan_id": "- not scanned -",
                        "qty": abs(qty)
                    })
 
                cur.execute("""
                    UPDATE pulltags
                    SET status = %s
                    WHERE job_number = %s AND lot_number = %s AND item_code = %s
                """, ('kitted' if tx_type == 'Job Issue' else 'returned', job, lot, item_code))
 
                delta = qty if tx_type == "Return" else -qty
                cur.execute("""
                    INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                    SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, (item_code, loc_value, delta, warehouse))
 
    pdf_path = generate_finalize_summary_pdf(summary_rows, verified_by=user, verified_on=datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M"))
    if os.path.exists(pdf_path):
        with open(pdf_path, "rb") as f:
            st.download_button("📄 Download Final Scan Summary", f, file_name="final_scan_summary.pdf", mime="application/pdf")
 
def run():
    st.title("📦 Multi-Lot Job Kitting")
    if 'job_lot_queue' not in st.session_state:
        st.session_state.job_lot_queue = []
 
    with st.form("add_joblot", clear_on_submit=True):
        job = st.text_input("Job Number")
        lot = st.text_input("Lot Number")
        if st.form_submit_button("Add Job/Lot"):
            if job and lot:
                pair = (job.strip(), lot.strip())
                if pair not in st.session_state.job_lot_queue:
                    st.session_state.job_lot_queue.append(pair)
 
    for job, lot in st.session_state.job_lot_queue:
        if (job, lot) not in st.session_state.pulltag_editor_df:
            rows = get_pulltag_rows(job, lot)
            df = pd.DataFrame(rows)
            if not df.empty:
                df = df[df['transaction_type'].isin(['Job Issue', 'Return'])].copy()
                df['kitted_qty'] = df['qty_req']
                df['notes'] = ""
                df['scan_required'] = df.get('scan_required', False)
                df['transaction_type'] = df['transaction_type']
                df['warehouse'] = df['warehouse'] if 'warehouse' in df else 'MAIN'
            st.session_state.pulltag_editor_df[(job, lot)] = df
 
        st.subheader(f"📋 Edit Pulltags for {job}-{lot}")
        st.data_editor(st.session_state.pulltag_editor_df[(job, lot)], use_container_width=True, key=f"editor_{job}_{lot}")
 
    if st.button("✅ Finalize All Kitting"):
        finalize_all()
 
if __name__ == "__main__":
    run()
