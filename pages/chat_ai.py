import streamlit as st
import pandas as pd
import re
from io import StringIO
from datetime import datetime
from openai import OpenAI
from db import get_db_cursor

"""
AI‑Powered Inventory Assistant v5.1 (full script)
-------------------------------------------------
* Built‑in glossary + user extension.
* Session‑only CSV/XLSX upload (first 100 rows sent to GPT for context).
* 9 predefined scenarios + free prompt.
* Strict SELECT‑only SQL guard.
* Results dataframe & optional CSV/TXT downloads.
"""

# ─── Config ────────────────────────────────────────────────────────────────
client = OpenAI(api_key=st.secrets["openai"]["api_key"])
MAX_CSV_LINES     = 100   # cap CSV rows passed to LLM
MAX_GLOSSARY_LINES = 60   # cap combined glossary lines

# ─── Built‑in glossary ─────────────────────────────────────────────────────
DEFAULT_GLOSSARY = """
VVSOLAR = Vacaville warehouse (primary distribution hub)
SACSOLAR = Sacramento warehouse
FNOSOLAR = Fresno warehouse
IESOLAR = Corona warehouse
GAFMAT = Cost code in items master for GAF material
RUNMAT = Cost code in items master table for Sunrun material


scan_id = Unique barcode UUID representing an inventory item; must be recorded via scan_verification for all transactions except Manual Adjustments
kitted = State where materials are picked, scanned, and staged; reflected in pulltags.status as 'kitted' or 'exported'
staging = Intermediate location used to consolidate items for shipment or inspection; may hold mixed SKUs if multi_item_allowed is True
pulltag = Unique Sage‑compatible line item representing a job‑lot‑item transaction; persisted in the pulltags table and used to track transaction lifecycle
ADD = Post‑kitting addition that increases inventory; inserts into pulltags with cost_code set to item_code and enforces scan verification
RETURNB = Post‑kitting return that re‑logs scan_ids and updates inventory; only inserts scan_verifications if scan_id was not previously used
Manual Adjustment = Admin‑only transaction bypassing scan requirements; requires justification in the note field and records user ID
multi_item_allowed = Boolean field in locations; controls whether mixed item_codes can be stored together; enforced during inventory moves
pallet_id = Group identifier linking multiple scan_ids into a single unit; typically used when handling bulk or staged material
csv_upload = Temporary data import via user‑supplied CSV; not persisted in database and used for ad‑hoc operations or comparisons
"""

# ─── DB schema + rules prompt ──────────────────────────────────────────────
SCHEMA_CONTEXT = """
You are an AI analyst for a PostgreSQL inventory system.

Allowed tables:
- transactions (item_code, quantity, transaction_type, date, job_number, lot_number, to_location, from_location, warehouse, user_id)
- locations (location_code, description, warehouse, multi_item_allowed)
- items_master (cost_code, item_code, item_description, uom, scan_required)
- inventory_init (item_code, location, quantity, scan_id)
- current_scan_location (scan_id, item_code, location, updated_at)
- scan_verifications (scan_id, item_code, job_number, lot_number, scan_time, location, transaction_type, warehouse, scanned_by)
- current_inventory (item_code, location, quantity, warehouse)
- pulltags (job_number, lot_number, item_code, quantity, scan_required, transaction_type, uploaded_at, last_updated, uom, status, note, scheduled_date)

**SQL Rules**
- Only produce `SELECT` statements.
- No data‑modifying commands.
- No semicolons or SQL comments.

Scenarios to support include audits, predictive pulltags, forecasting, TXT export, adjustment validation, pallet utilisation, anomaly heatmap, staging monitor, and free SQL.
"""

# ─── Scenario catalogue ────────────────────────────────────────────────────
SCENARIOS = [
    {"label": "Smart Transaction Audits",        "prompt": "Show lots where scan count doesn’t match pulltag quantity last week."},
    {"label": "Predictive Pulltag Generator",     "prompt": "Pre‑fill pulltags for Job 77124 based on past kits and remaining demand."},
    {"label": "Non‑Scanned Material Forecasting", "prompt": "Forecast material needs for the next 14 days based on pulltags not yet kitted."},
    {"label": "TXT Pulltag Composer",            "prompt": "Create a Sage‑formatted pulltag export for Job 88100, 5 of ITM456 and ITM789, from LOC001."},
    {"label": "Automated Adjustment Validator",   "prompt": "Show all ADD/RETURNB transactions that skipped scan verification or reused scan_ids in May."},
    {"label": "Pallet Utilisation Analyzer",      "prompt": "How many pallets were reused in the last 30 days? Any cross‑job reuse?"},
    {"label": "Inventory Anomaly Heatmap",        "prompt": "Which locations have unusual movements this month?"},
    {"label": "Staging Health Monitor",           "prompt": "Is any staging area overfilled right now?"},
    {"label": "Natural Language (free)",          "prompt": ""}
]

# ─── Utility helpers ───────────────────────────────────────────────────────

def extract_block(text: str, tag: str) -> str | None:
    """Return contents of first fenced code‑block labelled <tag>."""
    m = re.search(rf"```{tag}\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else None


def df_to_limited_csv(df: pd.DataFrame, max_rows: int = MAX_CSV_LINES) -> str:
    if len(df) > max_rows:
        df = df.head(max_rows)
    buf = StringIO(); df.to_csv(buf, index=False)
    return buf.getvalue()


def build_glossary_context() -> str | None:
    """Combine default glossary with user additions, dedupe by term."""
    user_text = st.session_state.get("glossary_text", "")
    lines = [ln.strip() for ln in DEFAULT_GLOSSARY.strip().splitlines() if ln.strip()]
    if user_text:
        lines += [ln.strip() for ln in user_text.splitlines() if ln.strip()]
    if not lines:
        return None
    # deduplicate by term (before '=')
    seen = set(); deduped = []
    for ln in lines:
        term = ln.split("=")[0].strip().lower()
        if term not in seen:
            deduped.append(ln); seen.add(term)
    if len(deduped) > MAX_GLOSSARY_LINES:
        deduped = deduped[:MAX_GLOSSARY_LINES]
    return "Domain glossary (interpret shorthand accordingly):\n" + "\n".join(deduped)

# ─── Main app ──────────────────────────────────────────────────────────────

def run():
    st.title("🤖 AI Inventory Assistant")

    # Sidebar – Upload & Glossary ------------------------------------------------
    with st.sidebar:
        st.header("📤 Session‑Only Upload")
        up_file = st.file_uploader("CSV or Excel", type=["csv", "xlsx", "xls"], help="Data is kept in memory and passed to the AI for context only.")
        if up_file is not None:
            try:
                df_up = pd.read_csv(up_file) if up_file.name.lower().endswith(".csv") else pd.read_excel(up_file)
                st.success(f"Loaded {len(df_up)} rows × {df_up.shape[1]} cols")
                st.dataframe(df_up.head())
                st.session_state["uploaded_df"] = df_up
            except Exception as e:
                st.error(f"Upload error: {e}")
        else:
            st.session_state.pop("uploaded_df", None)

        with st.expander("📚 Add / Override Glossary (session)"):
            user_gloss = st.text_area("Term = definition (one per line)", value=st.session_state.get("glossary_text", ""), height=150)
            if st.button("Save Glossary"):
                st.session_state["glossary_text"] = user_gloss
                st.success("Glossary saved for this session.")

    # Scenario & prompt ----------------------------------------------------------
    scenario_label = st.selectbox("Choose a scenario", [s["label"] for s in SCENARIOS], index=0)
    scenario      = next(s for s in SCENARIOS if s["label"] == scenario_label)
    user_prompt   = st.text_area("Prompt", value=scenario["prompt"], height=120)

    # Run button -----------------------------------------------------------------
    if st.button("🧠 Run") and user_prompt.strip():
        with st.spinner("Thinking …"):
            try:
                # Build messages list ------------------------------------------------
                messages = [{"role": "system", "content": SCHEMA_CONTEXT}]

                gloss_ctx = build_glossary_context()
                if gloss_ctx:
                    messages.append({"role": "system", "content": gloss_ctx})

                df_session = st.session_state.get("uploaded_df")
                if df_session is not None:
                    csv_snip = df_to_limited_csv(df_session)
                    messages.append({"role": "system", "content": f"Additional context CSV (first {MAX_CSV_LINES} rows):\n<CSV_START>\n{csv_snip}\n<CSV_END>\nData not in database."})

                messages.append({"role": "user", "content": user_prompt.strip()})

                # LLM call ----------------------------------------------------------
                resp   = client.chat.completions.create(model="gpt-4-turbo", messages=messages, temperature=0.2, max_tokens=1400)
                raw    = resp.choices[0].message.content.strip()
                usage  = resp.usage
                cost   = (usage.prompt_tokens*0.03 + usage.completion_tokens*0.06) / 1000

                # Handle GPT refusal ------------------------------------------------
                if raw.startswith("❌"):
                    st.error(raw); return

                sql_query = extract_block(raw, "sql")
                txt_blob  = extract_block(raw, "txt")
                insights  = re.sub(r"```.*?```", "", raw, flags=re.DOTALL).strip()

                # Display insights --------------------------------------------------
                if insights:
                    st.subheader("💡 AI Insights")
                    st.markdown(insights)

                # Execute and show SQL ---------------------------------------------
                if sql_query:
                    st.subheader("Generated SQL")
                    st.code(sql_query, language="sql")
                    with get_db_cursor() as cur:
                        cur.execute(sql_query)
                        rows = cur.fetchall(); cols = [d[0] for d in cur.description]
                    df = pd.DataFrame(rows, columns=cols)
                    st.dataframe(df)
                    if not df.empty:
                        st.download_button("Download CSV", df.to_csv(index=False), "report.csv")
                else:
                    st.info("⚠️ No SQL query returned.")

                # TXT download ------------------------------------------------------
                if txt_blob:
                    st.subheader("Sage Export (.txt)")
                    st.text_area("Preview", txt_blob, height
