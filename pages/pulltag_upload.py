import streamlit as st
from db import get_db_cursor

def run():
    st.title("📂 Bulk Pull-tag TXT Upload")

    uploaded_files = st.file_uploader(
        "Upload Pull-tag TXT Files", accept_multiple_files=True, type=['txt']
    )

    def parse_and_insert(txt_file):
        lines = txt_file.read().decode("utf-8").splitlines()
        insert_count = 0

        for line in lines:
            if line.startswith("IL"):
                parts       = line.split(",")
                item_code   = parts[2].strip()
                cost_code   = parts[11].strip()
                description = parts[5].replace('"', '').strip()
                quantity    = int(parts[3])
                job_number  = parts[9].strip()
                lot_number  = parts[10].strip()
                scan_required = (item_code == cost_code)

                with get_db_cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO pulltags (
                            job_number, lot_number, item_code,
                            cost_code, description, quantity,
                            scan_required, status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                        ON CONFLICT DO NOTHING;
                        """,
                        (
                            job_number,
                            lot_number,
                            item_code,
                            cost_code,
                            description,
                            quantity,
                            scan_required
                        )
                    )
                insert_count += 1

        return insert_count

    if st.button("Upload & Parse Files"):
        if not uploaded_files:
            st.warning("Please upload at least one .txt file.")
        else:
            total_inserted = 0
            for txt_file in uploaded_files:
                total_inserted += parse_and_insert(txt_file)
            st.success(f"Successfully uploaded and inserted {total_inserted} items into the database! 🎉")


