import streamlit as st
import pandas as pd
from io import StringIO
from db import get_db_cursor

# Manual parsing based on fixed field positions, handling embedded commas and quotes

def parse_to_records(txt_file):
    """
    Parse a TXT file and return a list of dicts for each valid IL row without inserting.
    Fields: warehouse, item_code, quantity, uom, description, job_number, lot_number, cost_code
    """
    raw = txt_file.read()
    text = raw.decode("utf-8", errors="replace")
    records = []

    for line in text.splitlines():
        if not line.startswith("IL,"):
            continue
        cols = line.split(",")
        if len(cols) < 11:
            continue

        TRAILING = 9
        L = len(cols)
        desc_parts = cols[5: L - TRAILING]
        description = ",".join(desc_parts).strip()
        if description.startswith('"') and description.endswith('"'):
            description = description[1:-1]
        description = description.strip()

        warehouse  = cols[1].strip()
        item_code  = cols[2].strip()
        qty_str    = cols[3].strip()
        uom        = cols[4].strip()
        job_number = cols[-6].strip()
        lot_number = cols[-5].strip()
        cost_code  = cols[-4].strip()

        try:
            quantity = int(qty_str)
        except ValueError:
            continue
        if quantity <= 0:
            continue

        records.append({
            "warehouse": warehouse,
            "item_code": item_code,
            "quantity": quantity,
            "uom": uom,
            "description": description,
            "job_number": job_number,
            "lot_number": lot_number,
            "cost_code": cost_code
        })
    txt_file.seek(0)
    return records

def parse_and_insert(records):
    insert_count = 0
    skipped = []

    with get_db_cursor() as cursor:
        for rec in records:
            cursor.execute(
                """
                SELECT 1 FROM pulltags
                WHERE job_number = %s AND lot_number = %s AND item_code = %s
                  AND transaction_type IN ('Job Issue', 'Return')
                """,
                (rec['job_number'], rec['lot_number'], rec['item_code'])
            )
            if cursor.fetchone():
                skipped.append(f"{rec['job_number']} / {rec['lot_number']} / {rec['item_code']}")
                continue

            cursor.execute(
                """
                INSERT INTO pulltags
                  (warehouse, item_code, quantity, uom, description,
                   job_number, lot_number, cost_code, transaction_type, status, note)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'Job Issue','pending','Imported')
                """,
                (
                    rec['warehouse'], rec['item_code'], rec['quantity'],
                    rec['uom'], rec['description'], rec['job_number'],
                    rec['lot_number'], rec['cost_code']
                )
            )
            insert_count += 1

    return insert_count, skipped

def run():
    st.title("\U0001F4C2 Bulk Pull-tag TXT Upload")

    if 'processed_files' not in st.session_state:
        st.session_state.processed_files = set()

    uploaded_files = st.file_uploader(
        "Upload Pull-tag .TXT Files",
        accept_multiple_files=True,
        type=["txt"]
    )
    if not uploaded_files:
        return

    new_files = [f for f in uploaded_files if f.name not in st.session_state.processed_files]
    if not new_files:
        st.info("No new files to preview; all uploaded files have been processed.")
        return

    all_records = []
    for f in new_files:
        all_records.extend(parse_to_records(f))
    if not all_records:
        st.warning("No valid IL rows found in selected files.")
        return

    df = pd.DataFrame(all_records)
    st.subheader("Editable Parsed Pull-tag Rows")
    edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True)

    if st.button("Commit Parsed Pull-tags to DB"):
        total_inserted = 0
        inserted, skipped = parse_and_insert(edited_df.to_dict("records"))
        total_inserted += inserted

        if skipped:
            st.warning(f"⚠️ Skipped {len(skipped)} duplicate rows:")
            st.code("\n".join(skipped))

        for f in new_files:
            st.session_state.processed_files.add(f.name)

        st.success(f"✅ Inserted {total_inserted} rows into pulltags")
        st.info("Done processing and committing pull-tag files.")

        if st.button("✅ Click here to clear the page."):
            st.session_state.pop("processed_files", None)
            st.rerun()

        st.caption("This will reset the page so you can upload a new set of pulltags.")
