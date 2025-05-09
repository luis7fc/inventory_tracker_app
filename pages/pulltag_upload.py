import streamlit as st
from db import get_db_cursor

def run():
    """Entry point for the Pull-tag Upload page."""
    st.title("📂 Pull-tag TXT Upload")

    # File uploader for multiple .txt files
    uploaded_files = st.file_uploader(
        "Upload Pull-tag TXT Files",
        accept_multiple_files=True,
        type=['txt']
    )

    # Parser + inserter
    def parse_and_insert(txt_file):
        content = txt_file.read().decode("utf-8")
        lines = content.splitlines()
        insert_count = 0

        for line in lines:
            if line.startswith("IL"):
                parts = line.split(",")
                warehouse     = parts[1].strip()
                item_code     = parts[2].strip()
                quantity      = int(parts[3])
                description   = parts[5].replace('"', '').strip()
                job_number    = parts[9].strip()
                lot_number    = parts[10].strip()
                cost_code     = parts[11].strip()
                scan_required = (item_code == cost_code)

                with get_db_cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO pulltags (
                            job_number, lot_number, item_code,
                            cost_code, description, quantity,
                            scan_required, status, warehouse
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (
                            job_number,
                            lot_number,
                            item_code,
                            cost_code,
                            description,
                            quantity,
                            scan_required,
                            warehouse
                        )
                    )
                insert_count += 1

        return insert_count

    # Upload button logic
    if st.button("Upload & Parse Files"):
        if not uploaded_files:
            st.warning("Please upload at least one .txt file.")
        else:
            total_inserted = sum(parse_and_insert(f) for f in uploaded_files)
            st.success(f"Successfully uploaded and inserted {total_inserted} items into the database! 🎉")
