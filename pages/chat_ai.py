import streamlit as st
import pandas as pd
import re
import contextlib
from io import StringIO
from datetime import datetime
from openai import OpenAI
import psycopg2

"""
AI‑Powered Inventory Assistant v5.3 (Enhanced)
-------------------------------------------------
* Built‑in glossary + user extension with override capability.
* Session‑only CSV/XLSX upload (first 100 rows sent to GPT for context).
* 9 predefined scenarios + free prompt.
* Flexible SELECT‑only SQL guard allowing multi-statement SELECT queries.
* Read-only cursor for AI queries and separate read-write cursor for logging.
* Results dataframe & optional CSV/TXT downloads.
"""

# ─── Config ────────────────────────────────────────────────────────────────
client = OpenAI(api_key=st.secrets["openai"]["api_key"])
MAX_CSV_LINES     = 100   # cap CSV rows passed to LLM
MAX_GLOSSARY_LINES = 60   # cap combined glossary lines

# ─── Database cursor functions ─────────────────────────────────────────────
@contextlib.contextmanager

def get_readonly_cursor():
    """Yields a read-only cursor for SELECT queries, no commit needed."""
    conn = psycopg2.connect(
        host=st.secrets["DB_HOST"],
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_READONLY_USER"],  # Assumes read-only user in secrets
        password=st.secrets["DB_READONLY_PASSWORD"],
        port=st.secrets.get("DB_PORT", 5432),
        options="-c statement_timeout=30000"  # 30-second query timeout
    )
    conn.set_session(readonly=True)  # Enforce read-only at connection level
    try:
        cursor = conn.cursor()
        yield cursor
    finally:
        cursor.close()
        conn.close()

@contextlib.contextmanager
def get_db_cursor():
    """Yields a cursor for read-write operations (e.g., logging)."""
    conn = psycopg2.connect(
        host=st.secrets["DB_HOST"],
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        port=st.secrets.get("DB_PORT", 5432)
    )
    try:
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─── Built‑in glossary ─────────────────────────────────────────────────────
DEFAULT_GLOSSARY = """
VVSOLAR = Vacaville warehouse (primary distribution hub)
SACSOLAR = Sacramento warehouse
FNOSOLAR = Fresno warehouse
IESOLAR = Corona warehouse
GAFMAT  = Cost code in items_master for GAF material
RUNMAT  = Cost code in items_master for Sunrun material
pulltag changes = pulltag rows for transaction type job issue with a comment other than default note - imported
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
- Multiple SELECT statements are allowed, separated by semicolons.
- No data‑modifying commands.
- No SQL comments.

Scenarios to support include audits, predictive pulltags, forecasting, TXT export, adjustment validation, pallet utilisation, anomaly heatmap, staging monitor, and free SQL.
"""

FULL_CONTEXT_FROM_CHAD = """
You are an AI-powered inventory assistant for CRS Inventory Tracker.
Your role is to respond like a seasoned systems engineer — direct, accurate, and efficient.
You help warehouse managers, analysts, and admins understand and query inventory behavior.

---

🧠 RESPONSE STYLE:
- Be concise and structured.
- Always explain logic before or after giving SQL.
- Offer next steps or validation tips where useful.
- Never guess — if unsure, suggest what data to check.

---

📦 CORE CONCEPTS:
- job_number and lot_number are always TEXT.
- scan_id is unique and tied to an inventory item.
- pulltags track material flow; their status changes from pending → kitted → exported → returned/adjusted.
- RETURNB inserts only if scan_id is *not already logged*.
- ADD requires that scan_id exists in current_scan_location.
- Manual Adjustments skip scan validation but must include a note and user_id.
- multi_item_allowed in locations controls whether multiple SKUs can exist at a location.
- staging areas can temporarily hold multiple SKUs unless overfilled (>10 items).

---

🗃️ TABLE DEFINITIONS:
CREATE TABLE pulltags (
  job_number TEXT,
  lot_number TEXT,
  item_code TEXT,
  quantity INTEGER,
  scan_required BOOLEAN,
  transaction_type TEXT,
  uploaded_at TIMESTAMP,
  last_updated TIMESTAMP,
  uom TEXT,
  status TEXT,
  note TEXT,
  scheduled_date DATE
);

CREATE TABLE current_inventory (
  item_code TEXT,
  location TEXT,
  quantity INTEGER,
  warehouse TEXT
);

CREATE TABLE scan_verifications (
  scan_id TEXT,
  item_code TEXT,
  job_number TEXT,
  lot_number TEXT,
  scan_time TIMESTAMP,
  location TEXT,
  transaction_type TEXT,
  warehouse TEXT,
  scanned_by TEXT
);

CREATE TABLE current_scan_location (
  scan_id TEXT,
  item_code TEXT,
  location TEXT,
  updated_at TIMESTAMP
);

---

✅ EXAMPLES:
"Why do I see a scan_id twice in my RETURNB?"
→ Because RETURNB only inserts new scan_ids. If it was already scanned in a Job Issue, it's skipped.

"Compare pulltag quantity vs scanned items for lot 70001"
→ Use this SQL:
```sql
SELECT pt.job_number, pt.lot_number, pt.item_code,
       SUM(pt.quantity) AS expected,
       COUNT(sv.scan_id) AS scanned
FROM pulltags pt
LEFT JOIN scan_verifications sv
  ON pt.job_number = sv.job_number
 AND pt.lot_number = sv.lot_number
 AND pt.item_code = sv.item_code
WHERE pt.lot_number = '70001'
GROUP BY pt.job_number, pt.lot_number, pt.item_code;
```

---

Always return helpful insights, not just SQL.
Explain behavior if data is missing, mismatched, or blocked by logic.
You are not just a query bot — you are a diagnostic expert.
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
    m = re.search(rf"```{tag}\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # fallback: extract SQL-looking text even without fencing
    m2 = re.search(r"SELECT .*?FROM .*?(?:GROUP BY .*?|ORDER BY .*?|$)", text, re.IGNORECASE | re.DOTALL)
    return m2.group(0).strip() if m2 else None

def df_to_limited_csv(df: pd.DataFrame, max_rows: int = MAX_CSV_LINES) -> str:
    if len(df) > max_rows:
        df = df.head(max_rows)
    buf = StringIO(); df.to_csv(buf, index=False)
    return buf.getvalue()

def build_glossary_context() -> str | None:
    user_text = st.session_state.get("glossary_text", "")
    user_lines = [ln.strip() for ln in user_text.splitlines() if ln.strip()]
    default_lines = [ln.strip() for ln in DEFAULT_GLOSSARY.strip().splitlines() if ln.strip()]
    lines = user_lines + default_lines  # User lines first to allow overriding
    if not lines:
        return None
    seen = set(); deduped = []
    for ln in lines:
        term = ln.split("=")[0].strip().lower()
        if term not in seen:
            deduped.append(ln); seen.add(term)
    if len(deduped) > MAX_GLOSSARY_LINES:
        deduped = deduped[:MAX_GLOSSARY_LINES]
    glossary_text = "\n".join(f"- {line}" for line in deduped)  # Bulleted list for clarity
    return "Domain glossary (interpret shorthand accordingly):\n" + glossary_text

# ─── Main app ──────────────────────────────────────────────────────────────

def run():
    st.title("🤖 AI Inventory Assistant")

    # Sidebar – Upload & Glossary -------------------------------------------
    with st.sidebar:
        user_id = st.session_state.get("user")
        if not user_id:
            st.error("🔒 You must be logged in to access this assistant.")
            st.stop()
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
            user_gloss = st.text_area("Term = definition (one per line)", value=st.session_state.get("glossary_text", ""), height=150, help="Enter terms to override defaults or add new ones.")
            if st.button("Save Glossary"):
                st.session_state["glossary_text"] = user_gloss
                st.success("Glossary saved for this session.")

        st.sidebar.divider()  # Divider after upload and glossary

    # Scenario & prompt ------------------------------------------------------
    scenario_label = st.selectbox("Choose a scenario", [s["label"] for s in SCENARIOS], index=0)
    scenario      = next(s for s in SCENARIOS if s["label"] == scenario_label)
    user_prompt   = st.text_area("Prompt", value=scenario["prompt"], height=120, help="Use single or multiple SELECT statements for queries.")

    st.divider()  # Divider after scenario selection

    # Run button -------------------------------------------------------------
    if st.button("🧠 Run") and user_prompt.strip():
        with st.spinner("Thinking …"):
            try:
                # Prepare CSV snippet if uploaded
                csv_snip = ""
                if "uploaded_df" in st.session_state:
                    csv_snip = df_to_limited_csv(st.session_state["uploaded_df"])

                # Build messages list
                messages = [
                    {"role": "system", "content": FULL_CONTEXT_FROM_CHAD},
                    {"role": "system", "content": SCHEMA_CONTEXT}
                ]

                gloss_ctx = build_glossary_context()
                if gloss_ctx:
                    messages.append({"role": "system", "content": gloss_ctx})

                if csv_snip:
                    messages.append({
                        "role": "system",
                        "content": f"Additional context from uploaded CSV (first {MAX_CSV_LINES} rows):\n<CSV_START>\n{csv_snip}\n<CSV_END>\nThis data is not in the database."
                    })

                messages.append({"role": "user", "content": user_prompt.strip()})

                # ── LLM call ───────────────────────────────────────────────
                resp = client.chat.completions.create(
                    model="gpt-4-turbo",
                    messages=messages,
                    temperature=0.2,
                    max_tokens=1400
                )

                raw   = resp.choices[0].message.content.strip()
                usage = resp.usage
                cost  = (usage.prompt_tokens*0.03 + usage.completion_tokens*0.06) / 1000

                if raw.startswith("❌"):
                    st.error(raw)
                    return

                sql_query = extract_block(raw, "sql")
                txt_blob  = extract_block(raw, "txt")
                insights  = re.sub(r"```.*?```", "", raw, flags=re.DOTALL).strip()

                if insights:
                    st.subheader("💡 AI Insights")
                    st.markdown(insights)

                if sql_query:
                    st.subheader("Generated SQL")
                    st.code(sql_query, language="sql")

                    with get_readonly_cursor() as cur:
                        cur.execute(sql_query)
                        rows = cur.fetchall()
                        cols = [d[0] for d in cur.description]
                    df_out = pd.DataFrame(rows, columns=cols)
                    st.dataframe(df_out)
                    if not df_out.empty:
                        st.download_button("Download CSV", df_out.to_csv(index=False), "report.csv")
                else:
                    st.info("⚠️ No SQL query returned.")

                if txt_blob:
                    st.subheader("Sage Export (.txt)")
                    st.text_area("Preview", txt_blob, height=200)
                    st.download_button("Download TXT", txt_blob, file_name="pulltag_export.txt")

                st.divider()  # Divider after output

                # ── Logging ────────────────────────────────────────────────
                user_id = st.session_state.get("user", "unknown")
                with get_db_cursor() as cur:
                    cur.execute(
                        """INSERT INTO ai_prompt_logs
                           (prompt, response, time_stamp, user_id, prompt_tokens, completion_tokens)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        (user_prompt, raw, datetime.now(), user_id, usage.prompt_tokens, usage.completion_tokens)
                    )

                st.caption(f"Tokens: {usage.total_tokens} (cost ≈ ${cost:.4f})")
                st.session_state["total_tokens_used"] = st.session_state.get("total_tokens_used", 0) + usage.total_tokens

            except Exception as exc:
                st.error(f"❌ {exc}")

if __name__ == "__main__":
    run()
