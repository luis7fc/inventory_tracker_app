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
        # Only process lines that start with IL,
        if not line.startswith("IL,"):
            continue
        cols = line.split(",")
        # We expect at least 11 columns (before considering embedded commas in description)
        if len(cols) < 11:
            continue

        # Fixed trailing fields count: conversion_factor, equipment_id, equipment_cost_code,
        # job_number, lot_number, cost_code, category, requisition_number, issue_date
        TRAILING = 9
        L = len(cols)

        # Extract description from cols[5] up to cols[L-TRAILING-1]
        desc_parts = cols[5: L - TRAILING]
        description = ",".join(desc_parts).strip()
        # Remove outer quotes if double-quoted
        if description.startswith('"') and description.endswith('"'):
            description = description[1:-1]
        # Strip any whitespace
        description = description.strip()

        # Map other fields
        warehouse  = cols[1].strip()
        item_code  = cols[2].strip()
        qty_str    = cols[3].strip()
        uom        = cols[4].strip()
        job_number = cols[-6].strip()
        lot_number = cols[-5].strip()
        cost_code  = cols[-4].strip()

        # Validate quantity
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
    # Reset file pointer for potential reuse
    txt_file.seek(0)
    return records


def parse_and_insert(txt_file):
    """
    Insert parsed IL rows from txt_file into the pulltags table.
    """
    records = parse_to_records(txt_file)
    insert_count = 0
    with get_db_cursor() as cursor:
        for rec in records:
            cursor.execute(
                """
                INSERT INTO pulltags
                  (warehouse, item_code, quantity, uom, description,
                   job_number, lot_number, cost_code)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    rec['warehouse'], rec['item_code'], rec['quantity'],
                    rec['uom'], rec['description'], rec['job_number'],
                    rec['lot_number'], rec['cost_code']
                )
            )
            insert_count += 1
    return insert_count


def run():
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )

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

    # Only consider new files
    new_files = [f for f in uploaded_files if f.name not in st.session_state.processed_files]
    if not new_files:
        st.info("No new files to preview; all uploaded files have been processed.")
        return

    # Preview parsed data
    all_records = []
    for f in new_files:
        all_records.extend(parse_to_records(f))
    if not all_records:
        st.warning("No valid IL rows found in selected files.")
        return

    df = pd.DataFrame(all_records)
    st.subheader("Preview Parsed Pull-tag Rows")
    st.dataframe(df)

    # Commit button
    if st.button("Commit Parsed Pull-tags to DB"):
        total_inserted = 0
        for f in new_files:
            total_inserted += parse_and_insert(f)
            st.session_state.processed_files.add(f.name)
        st.success(f"✅ Inserted {total_inserted} rows into pulltags")
        st.info("Done processing and committing pull-tag files.")
