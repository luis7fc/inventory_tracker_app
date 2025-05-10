import streamlit as st
from db import get_db_cursor
import io
import csv

def run():
    """Entry point for the Pull-tag Upload page."""
    st.title("📂 Pull-tag TXT Upload")

    # File uploader for multiple .txt files
    uploaded_files = st.file_uploader(
        "Upload Pull-tag TXT Files",
        type=['txt'],
        accept_multiple_files=True
    )

    def parse_and_insert(txt_bytes):
        """Parse raw TXT bytes and insert IL rows into the pulltags table."""
        text = txt_bytes.decode('utf-8', errors='replace')
        reader = csv.reader(io.StringIO(text), delimiter=',', quotechar='"', skipinitialspace=True)
        inserted = 0
        parsed_rows = []
        with get_db_cursor() as cursor:
            for row in reader:
                if not row or row[0] != 'IL':
                    continue
                # Map columns by index
                location    = row[1]
                item_code   = row[2]
                # Quantity conversion
                try:
                    quantity = int(row[3])
                except ValueError:
                    try:
                        quantity = float(row[3]) if row[3] else 0
                    except ValueError:
                        quantity = 0
                uom         = row[4].strip()
                description = row[5].strip()
                job_number  = row[9]
                lot_number  = row[10]
                cost_code   = row[11]

                # Insert into pulltags table
                cursor.execute(
                    "INSERT INTO pulltags (location, item_code, quantity, uom, description, job_number, lot_number, cost_code) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (location, item_code, quantity, uom, description, job_number, lot_number, cost_code)
                )
                inserted += 1
                parsed_rows.append({
                    'location': location,
                    'item_code': item_code,
                    'quantity': quantity,
                    'uom': uom,
                    'description': description,
                    'job_number': job_number,
                    'lot_number': lot_number,
                    'cost_code': cost_code
                })
        return inserted, parsed_rows

    # Upload button logic
    if st.button("Upload & Parse Files"):
        if not uploaded_files:
            st.warning("Please upload at least one .txt file.")
        else:
            total_inserted = 0
            for f in uploaded_files:
                bytes_data = f.read()
                count, rows = parse_and_insert(bytes_data)
                total_inserted += count
                st.success(f"Processed '{f.name}': inserted {count} rows.")
                if rows:
                    st.dataframe(rows)
            st.success(f"🎉 Successfully inserted {total_inserted} items into the database!")
