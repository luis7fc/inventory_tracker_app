import streamlit as st
import openai
import pandas as pd
from db import get_db_cursor  # your db connector

openai.api_key = st.secrets["openai"]["api_key"]

# Optional: define a system prompt that gives GPT your schema knowledge
SCHEMA_CONTEXT = """
You are an AI analyst that generates SQL queries for a PostgreSQL database. 
Available tables include:

- transactions (item_code, quantity, transaction_type, date, job_number, lot_number, to_location, from_location, warehouse, user_id)
- locations (location_code, description, warehouse, multi_item_allowed)
- items_master (cost_code, item_code, item_description, uom, scan_required)
- inventory_init (item_code, location, quantity, scan_id)
- current_scan_location (scan_id, item_code, location, updated_at)
- scan_verifications (scan_id, item_code, job_number, lot_number, scan_time, location, transaction_type, warehouse, scanned_by)
- current_inventory (item_code, location, quantity, warehouse)
- pulltags (job_number, lot_number, item_code, quantity, scan_required, transaction_type, uploaded_at, last_updated, uom, status, note)
- inventory_init (item_code, location, quantity, scan_id)

You only generate SELECT queries.
"""

def run():
    st.title("📊 AI Report Assistant")
    st.markdown("Ask a question about inventory, scans, or transactions:")

    user_prompt = st.text_area("Prompt", placeholder="e.g. Show total quantity by warehouse for item JA405")

    if st.button("🧠 Run Report") and user_prompt:
        with st.spinner("Generating SQL..."):
            try:
                # Step 1: Ask GPT for the SQL query
                messages = [
                    {"role": "system", "content": SCHEMA_CONTEXT},
                    {"role": "user", "content": user_prompt}
                ]

                response = openai.ChatCompletion.create(
                    model="gpt-4",
                    messages=messages,
                    temperature=0.2,
                    max_tokens=1000
                )

                sql_query = response.choices[0].message.content.strip()
                st.code(sql_query, language="sql")

                # Step 2: Run the SQL query
                with get_db_cursor() as cursor:
                    cursor.execute(sql_query)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]

                if rows:
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
                    st.download_button("Download as CSV", df.to_csv(index=False), "report.csv")
                else:
                    st.info("Query ran successfully but returned no results.")

            except Exception as e:
                st.error(f"❌ Error: {e}")
