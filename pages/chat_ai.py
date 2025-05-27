import streamlit as st
import openai
import pandas as pd
from db import get_db_cursor
from datetime import datetime

openai.api_key = st.secrets["openai"]["api_key"]

# System prompt: schema reference and safety constraints
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

You are only allowed to generate safe SQL queries using the SELECT statement. 
Do not use INSERT, UPDATE, DELETE, DROP, ALTER, or any other modifying operation. 
Do not include semicolons or comments.
"""

def run():
    st.title("📊 AI Report Assistant")
    st.markdown("Ask a question about inventory, scans, or transactions:")

    user_prompt = st.text_area("Prompt", placeholder="e.g. Show total quantity by warehouse for item JA405")

    if st.button("🧠 Run Report") and user_prompt:
        with st.spinner("Generating SQL..."):
            try:
                user_id = st.session_state.get("user", "unknown")

                # Ask GPT
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
                usage = response['usage']
                prompt_tokens = usage['prompt_tokens']
                completion_tokens = usage['completion_tokens']
                total_tokens = prompt_tokens + completion_tokens
                cost_estimate = (prompt_tokens * 0.03 + completion_tokens * 0.06) / 1000

                # Validate query
                if not sql_query.lower().startswith("select"):
                    st.error("⚠️ Only SELECT queries are allowed. This one was blocked.")
                    return

                if any(word in sql_query.lower() for word in ["insert", "update", "delete", "drop", "alter", "create", "truncate"]):
                    st.error("❌ Unsafe SQL command detected. Query was blocked.")
                    return

                st.code(sql_query, language="sql")

                # Run query
                with get_db_cursor() as cursor:
                    cursor.execute(sql_query)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]

                # Log prompt and usage
                with get_db_cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO ai_prompt_logs (
                            prompt, response, time_stamp, user_id,
                            prompt_tokens, completion_tokens
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (user_prompt, sql_query, datetime.now(), user_id, prompt_tokens, completion_tokens)
                    )

                # Show results
                if rows:
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
                    st.download_button("Download as CSV", df.to_csv(index=False), "report.csv")
                else:
                    st.info("Query ran successfully but returned no results.")

                # Show usage
                st.info(f"🧮 Tokens used: {prompt_tokens} prompt + {completion_tokens} completion = {total_tokens} total")
                st.caption(f"💵 Estimated cost: ${cost_estimate:.4f}")

                # Tally for session
                if 'total_tokens_used' not in st.session_state:
                    st.session_state.total_tokens_used = 0
                st.session_state.total_tokens_used += total_tokens
                st.caption(f"📊 Session total: {st.session_state.total_tokens_used} tokens")

            except Exception as e:
                st.error(f"❌ Error: {e}")
