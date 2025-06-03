import streamlit as st
import pandas as pd
import re
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from fpdf import FPDF
from io import BytesIO
import logging
import uuid
from psycopg2 import OperationalError, IntegrityError
from db import (
    get_pulltag_rows,
    get_db_cursor,
    insert_pulltag_line,
    update_pulltag_line,
    delete_pulltag_line,
)

# Configure logging
logging.basicConfig(level=logging.INFO, filename='kitting_app.log')
logger = logging.getLogger(__name__)

EDIT_ANCHOR = "scan-edit"

class ScanMismatchError(Exception):
    pass

def get_timezone():
    try:
        return ZoneInfo(st.secrets.get("APP_TIMEZONE", "America/Los_Angeles"))
    except ZoneInfoNotFoundError:
        logger.warning("Timezone not found, falling back to UTC.")
        return ZoneInfo("UTC")

def generate_finalize_summary_pdf(summary_data, verified_by=None, verified_on=None):
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
    output = BytesIO()
    pdf.output(output)
    output.seek(0)
    return output

def validate_input(job, lot):
    if not (job and lot):
        st.error("Job Number and Lot Number cannot be empty.")
        return False
    if not (re.match(r'^[A-Za-z0-9\\-]+$', job) and re.match(r'^[A-Za-z0-9\\-]+$', lot)):
        st.error("Job Number and Lot Number must be alphanumeric (dashes allowed).")
        return False
    return True

def initialize_session_state():
    defaults = {
        'session_id': str(uuid.uuid4()),
        'job_lot_queue': [],
        'pulltag_editor_df': {},
        'location': '',
        'scan_buffer': [],
        'user': 'unknown'
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def finalize_all():
    logger.info(f"Starting finalization for user {st.session_state.get('user', 'unknown')}")
    summary_rows = []
    user = st.session_state.get("user", "unknown")
    location = st.session_state.get("location", "")
    scan_buffer = st.session_state.get("scan_buffer", [])
    buffer_map = {}
    for job, lot, code, sid in scan_buffer:
        buffer_map.setdefault((job, lot, code), []).append(sid)

    transaction_data = []
    scan_data = []
    inventory_data = []
    pulltag_updates = []
    total_steps = sum(len(st.session_state.pulltag_editor_df.get((job, lot), pd.DataFrame())) for job, lot in st.session_state.job_lot_queue)
    progress_bar = st.progress(0)
    current_step = 0

    try:
        with get_db_cursor() as cur, cur.connection:
            for job, lot in st.session_state.job_lot_queue:
                df = st.session_state.pulltag_editor_df.get((job, lot))
                if df is None or df.empty:
                    continue
                df['warehouse'] = df['warehouse'].fillna('MAIN')
                df['kitted_qty'] = df['kitted_qty'].astype(int)

                for _, row in df.iterrows():
                    item_code = row['item_code']
                    qty = row['kitted_qty']
                    tx_type = row['transaction_type']
                    warehouse = row['warehouse']
                    loc_field = 'from_location' if tx_type == 'Job Issue' else 'to_location'
                    loc_value = location

                    transaction_data.append((tx_type, warehouse, loc_value, job, lot, item_code, qty, user))

                    scan_list = buffer_map.get((job, lot, item_code), [])
                    if row['scan_required'] and qty > 0:
                        if len(set(scan_list)) != qty:
                            raise ScanMismatchError(f"Scan mismatch for {item_code} in {job}-{lot}. Expected {qty} unique scans, found {len(set(scan_list))}")

                    for sid in scan_list:
                        scan_data.append((item_code, sid, job, lot, loc_value, tx_type, warehouse, user))
                        if tx_type == "Return":
                            inventory_data.append((sid, item_code, loc_value))
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

                    pulltag_updates.append(('kitted' if tx_type == 'Job Issue' else 'returned', job, lot, item_code))
                    delta = qty if tx_type == "Return" else -qty
                    inventory_data.append((item_code, loc_value, delta, warehouse))

                    current_step += 1
                    progress_bar.progress(current_step / total_steps)

            if transaction_data:
                cur.executemany("""
                    INSERT INTO transactions (
                        transaction_type, date, warehouse, from_location,
                        job_number, lot_number, item_code, quantity, user_id
                    ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """, [(d[0], d[1], d[2] if d[0] == 'Job Issue' else None, d[2] if d[0] == 'Return' else None, d[3], d[4], d[5], d[6], d[7]) for d in transaction_data])

            if scan_data:
                cur.executemany("""
                    INSERT INTO scan_verifications (
                        item_code, scan_id, job_number, lot_number,
                        scan_time, location, transaction_type, warehouse, scanned_by
                    ) VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                """, scan_data)

            if inventory_data:
                cur.executemany("""
                    INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                    SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, [(d[0], d[1], d[2], d[3]) for d in inventory_data if len(d) == 4])

                cur.executemany("""
                    INSERT INTO current_scan_location (scan_id, item_code, location)
                    VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """, [(d[0], d[1], d[2]) for d in inventory_data if len(d) == 3])

            if pulltag_updates:
                cur.executemany("""
                    UPDATE pulltags
                    SET status = %s
                    WHERE job_number = %s AND lot_number = %s AND item_code = %s
                """, pulltag_updates)

            cur.connection.commit()
            logger.info("Finalization completed successfully")
    except ScanMismatchError as e:
        cur.connection.rollback()
        st.error(str(e))
        logger.error(f"Finalization failed: {str(e)}")
        return
    except (OperationalError, IntegrityError) as e:
        cur.connection.rollback()
        st.error(f"Database error: {str(e)}")
        logger.error(f"Database error: {str(e)}")
        return
    except Exception as e:
        cur.connection.rollback()
        st.error(f"Error during finalization: {str(e)}")
        logger.error(f"Finalization failed: {str(e)}")
        return

    pdf_output = generate_finalize_summary_pdf(summary_rows, verified_by=user, verified_on=datetime.now(get_timezone()).strftime("%Y-%m-%d %H:%M"))
    st.download_button("📄 Download Final Scan Summary", pdf_output, file_name="final_scan_summary.pdf", mime="application/pdf")

def run():
    initialize_session_state()
    st.title("📦 Multi-Lot Job Kitting")
    st.session_state.location = st.text_input("Staging Location", value=st.session_state.location, key="kitting_location")

    with st.form("add_joblot", clear_on_submit=True):
        job = st.text_input("Job Number")
        lot = st.text_input("Lot Number")
        if st.form_submit_button("Add Job/Lot"):
            if validate_input(job.strip(), lot.strip()):
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
                df['warehouse'] = df['warehouse'].fillna('MAIN')
                st.session_state.pulltag_editor_df[(job, lot)] = df

        st.subheader(f"📋 Edit Pulltags for {job}-{lot}")
        df_key = f"editor_{job}_{lot}_{st.session_state.session_id}"
        edited_df = st.data_editor(st.session_state.pulltag_editor_df[(job, lot)].copy(), use_container_width=True, key=df_key)
        if edited_df is not None and not edited_df.equals(st.session_state.pulltag_editor_df[(job, lot)]):
            st.session_state.pulltag_editor_df[(job, lot)] = edited_df

        filtered_df = st.session_state.pulltag_editor_df[(job, lot)]
        scan_required_rows = filtered_df[(filtered_df['scan_required']) & (filtered_df['kitted_qty'] > 0)]

        if not scan_required_rows.empty:
            with st.expander(f"🔍 Scan Inputs for {job}-{lot}", expanded=False):
                for _, row in scan_required_rows.iterrows():
                    item_code = row['item_code']
                    qty_required = int(row['kitted_qty'])
                    current_scans = [
                        s[3] for s in st.session_state.scan_buffer
                        if s[0] == job and s[1] == lot and s[2] == item_code
                    ]

                    st.write(f"**Item:** {item_code} | Required: {qty_required} | Scanned: {len(current_scans)}")
                    scans_raw = st.text_area(
                        f"Paste scan IDs for {item_code} (comma or newline-separated)",
                        value="",
                        key=f"scan_text_{job}_{lot}_{item_code}"
                    )

                    if st.button(f"Import Scans for {item_code}", key=f"scan_import_{job}_{lot}_{item_code}"):
                        new_scans = [s.strip() for s in scans_raw.replace(",", "\n").splitlines() if s.strip()]
                        new_scans = list(dict.fromkeys(new_scans))
                        existing = set(current_scans)
                        to_add = [s for s in new_scans if s not in existing]

                        if len(existing) + len(to_add) > qty_required:
                            st.warning(f"Too many scans. Already have {len(existing)}, trying to add {len(to_add)}, but only {qty_required} allowed.")
                        else:
                            for sid in to_add:
                                st.session_state.scan_buffer.append((job, lot, item_code, sid))
                            st.success(f"Added {len(to_add)} new scans for {item_code}")

    if st.session_state.job_lot_queue:
        if st.button("✅ Finalize All Kitting"):
            st.session_state.confirm_kitting = True

        if st.session_state.get("confirm_kitting"):
            if st.button("Confirm Finalization"):
                finalize_all()
                st.session_state.confirm_kitting = False
    else:
        st.warning("No jobs/lots to finalize.")

if __name__ == "__main__":
    run()
