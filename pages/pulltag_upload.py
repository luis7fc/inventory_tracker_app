import streamlit as st
import csv
from io import StringIO
from db import get_db_cursor

# Page configuration
st.set_page_config(page_title="Bulk Pull-tag TXT Upload", layout="wide")
st.title("📂 Bulk Pull-tag TXT Upload")

# File uploader for multiple .txt files
uploaded_files = st.file_uploader(
    "Upload Pull-tag .TXT Files",
    accept_multiple_files=True,
    type=["txt"]
)


def parse_and_insert(txt_file):
    """
    Parse a Sage-style pull-tag TXT file, extracting 'IL' lines and inserting
    into the pulltags table with columns:
      warehouse, item_code, quantity, uom, description,
      job_number, lot_number, cost_code
    Properly handles embedded double-quotes in descriptions.
    Returns the number of inserted rows.
    """
    text = txt_file.read().decode("utf-8")
    reader = csv.reader(
        StringIO(text), delimiter=',', quotechar='"', doublequote=True
    )
    insert_count = 0

    with get_db_cursor() as cursor:
        for row in reader:
            # Only process IL lines
            if not row or row[0] != "IL":
                continue

            # Ensure at least 15 columns to avoid index errors
            row += [""] * 15

            # Map fields by position
            warehouse    = row[1]
            item_code    = row[2]
            qty_str      = row[3]
            uom          = row[4]
            description  = row[5]
            job_number   = row[9]
            lot_number   = row[10]
            cost_code    = row[11]

            # Clean up description quotes
            if description:
                # Replace CSV-escaped double-quotes with a single quote
                description = description.replace('""', '"')
                # Strip any leading/trailing extra quotes or whitespace
                description = description.strip('"').strip()

            # Validate quantity
            try:
                quantity = int(qty_str)
            except ValueError:
                continue
            if quantity <= 0:
                continue

            # Insert into pulltags
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


# On upload, parse each file and insert rows
if uploaded_files:
    total_inserted = 0
    for txt_file in uploaded_files:
        inserted = parse_and_insert(txt_file)
        total_inserted += inserted
    st.success(f"✅ Inserted {total_inserted} rows into pulltags")
    st.info("Done processing all uploaded pull-tag files.")
