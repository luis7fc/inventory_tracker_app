import streamlit as st
import csv
import pandas as pd
from io import StringIO
from db import get_db_cursor


def parse_and_insert(txt_file):
    """
    Insert parsed pull-tag IL rows from txt_file into the pulltags table.
    Returns the number of inserted rows.
    """
    raw = txt_file.read()
    text = raw.decode("utf-8", errors="replace")
    reader = csv.reader(
        StringIO(text), delimiter=',', quotechar='"', doublequote=True
    )
    insert_count = 0

    with get_db_cursor() as cursor:
        for row in reader:
            if not row or row[0] != "IL":
                continue
            row += [""] * 15
            warehouse   = row[1]
            item_code   = row[2]
            qty_str     = row[3]
            uom         = row[4]
            description = row[5]
            job_number  = row[9]
            lot_number  = row[10]
            cost_code   = row[11]
            if description:
                description = description.replace('""', '"').strip('"').strip()
            try:
                quantity = int(qty_str)
            except (ValueError, TypeError):
                continue
            if quantity <= 0:
                continue
            cursor.execute(
                """
                INSERT INTO pulltags
                  (warehouse, item_code, quantity, uom, description,
                   job_number, lot_number, cost_code)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (warehouse, item_code, quantity,
                 uom, description, job_number, lot_number, cost_code)
            )
            insert_count += 1
    return insert_count


def parse_to_records(txt_file):
    """
    Parse txt_file and return list of dicts for IL rows without inserting.
    """
    records = []
    raw = txt_file.read()
    text = raw.decode("utf-8", errors="replace")
    reader = csv.reader(
        StringIO(text), delimiter=',', quotechar='"', doublequote=True
    )
    for row in reader:
        if not row or row[0] != "IL":
            continue
        row += [""] * 15
        try:
            quantity = int(row[3])
        except (ValueError, TypeError):
            continue
        if quantity <= 0:
            continue
        description = row[5]
        if description:
            description = description.replace('""', '"').strip('"').strip()
        records.append({
            "warehouse": row[1],
            "item_code": row[2],
            "quantity": quantity,
            "uom": row[4],
            "description": description,
            "job_number": row[9],
            "lot_number": row[10],
            "cost_code": row[11]
        })
    # reset cursor to start for possible insert later
    txt_file.seek(0)
    return records


def run():
    st.title("📂 Bulk Pull-tag TXT Upload")

    if 'processed_files' not in st.session_state:
        st.session_state.processed_files = set()

    uploaded_files = st.file_uploader(
        "Upload Pull-tag .TXT Files",
        accept_multiple_files=True,
        type=["txt"]
    )

    if not uploaded_files:
        return

    # Identify new files not yet processed
    new_files = [f for f in uploaded_files if f.name not in st.session_state.processed_files]
    if not new_files:
        st.info("No new files to preview; all uploaded files have been processed.")
        return

    # Preview parsed data for new files
    all_records = []
    for txt_file in new_files:
        records = parse_to_records(txt_file)
        all_records.extend(records)
    if all_records:
        df = pd.DataFrame(all_records)
        st.subheader("Preview Parsed Pull-tag Rows")
        st.dataframe(df)
    else:
        st.warning("No valid IL rows found in selected files.")
        return

    # Commit button
    if st.button("Commit Parsed Pull-tags to DB"):
        total_inserted = 0
        for txt_file in new_files:
            total_inserted += parse_and_insert(txt_file)
            st.session_state.processed_files.add(txt_file.name)
        st.success(f"✅ Inserted {total_inserted} rows into pulltags")
        st.info("Done processing and committing pull-tag files.")
