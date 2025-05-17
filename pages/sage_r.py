import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Tuple
import re

from db import get_db_cursor  # shared connection helper

# ==============================================================================
# Sage Re‑export page (replacement for Scan Lookup)
# Primary filter: Job → Lot queue (same UX as Kitting)
# Optional filters: transaction_type list, last_updated range
# Outputs: single combined .txt file ready for Sage import AND
#          flags exported rows by setting pulltags.status = 'exported'
# ==============================================================================

DEFAULT_TX_TYPES = ["Job Issue", "ADD", "RETURNB", "Return"]

# ──────────────────────────────────────────────────────────────────────────────
# Helper: compose Sage‑formatted TXT string
# ──────────────────────────────────────────────────────────────────────────────

def build_txt(df: pd.DataFrame, title: str, header_dt: date) -> str:
    """Return a Sage‑importable pull‑tag TXT string."""
    fmt_date = header_dt.strftime("%m-%d-%y")

    # Header block (matches sample)
    lines: List[str] = [
        f'I,{title},{fmt_date},{fmt_date},,"",,,,,,,,,\n',
        ',,,, ,"",,,,,,,,,\n'
    ]

    for _, row in df.iterrows():
        loc         = row.get("location")     or ""
        item_code   = row["item_code"]
        qty         = row["quantity"]
        uom         = row.get("uom")          or ""
        desc        = row.get("description")  or ""
        cost_code   = row.get("cost_code")    or item_code  # safe fall‑back
        job         = row["job_number"]
        lot         = row["lot_number"]
        desc_q = f'"{desc}"'
        il = (
            f'IL,{loc},{item_code},{qty},{uom},{desc_q},1,,,'
            f'{job},{lot},{cost_code},M,,{fmt_date}\n'
        )
        lines.append(il)

    return ''.join(lines)

# ──────────────────────────────────────────────────────────────────────────────
# Helper: build WHERE clause & parameter list for both SELECT + UPDATE
# ──────────────────────────────────────────────────────────────────────────────

def _where_params(pairs: List[Tuple[str, str]],
                  tx_types: List[str],
                  date_from: date | None,
                  date_to:   date | None):
    clauses: List[str] = []
    params:  List      = []

    # Composite job/lot list
    pair_clauses = ' OR '.join(['(job_number = %s AND lot_number = %s)'] * len(pairs))
    clauses.append(f'({pair_clauses})')
    for j, l in pairs:
        params.extend([j, l])

    if tx_types:
        clauses.append('transaction_type = ANY(%s)')
        params.append(tx_types)

    if date_from and date_to:
        clauses.append('last_updated >= %s AND last_updated < %s')
        params.extend([date_from, date_to])

    where_sql = ' AND '.join(clauses)
    return where_sql, params

# ──────────────────────────────────────────────────────────────────────────────
# Helper: fetch rows & (optionally) mark them exported
# ──────────────────────────────────────────────────────────────────────────────

def fetch_pulltags(pairs: List[Tuple[str, str]],
                   tx_types: List[str],
                   date_from: date | None,
                   date_to:   date | None,
                   mark_exported: bool = False) -> pd.DataFrame:
    if not pairs:
        return pd.DataFrame()

    where_sql, params = _where_params(pairs, tx_types, date_from, date_to)

    sel_sql = f"""
        SELECT id, job_number, lot_number, item_code, quantity,
               uom, description, cost_code, location,
               transaction_type, warehouse, last_updated, status
        FROM   pulltags
        WHERE  {where_sql}
        ORDER BY job_number, lot_number, item_code;
    """

    with get_db_cursor() as cur:
        cur.execute(sel_sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        # Optional UPDATE – set status = 'exported'
        if mark_exported and rows:
            upd_sql = f"UPDATE pulltags SET status = 'exported' WHERE {where_sql};"
            cur.execute(upd_sql, params)

    return pd.DataFrame(rows, columns=cols)

# ──────────────────────────────────────────────────────────────────────────────
# Streamlit entry point
# ──────────────────────────────────────────────────────────────────────────────

def run():
    st.title("📤 Sage Pull‑Tag Re‑export")

    # Session storage for queue
    if 'job_lot_queue' not in st.session_state:
        st.session_state.job_lot_queue: List[Tuple[str, str]] = []  # type: ignore

    # ── Job / Lot queue widgets ───────────────────────────────────────────
    j_col, l_col, btn_col = st.columns([1, 1, 0.4])
    job_input = j_col.text_input("Job #", key="job_in")
    lot_input = l_col.text_input("Lot #", key="lot_in")
    if btn_col.button("➕ Add", key="add_pair"):
        if job_input.strip() and lot_input.strip():
            st.session_state.job_lot_queue.append((job_input.strip(), lot_input.strip()))
            st.session_state.job_in = ""
            st.session_state.lot_in = ""

    # Display queue & clear option
    if st.session_state.job_lot_queue:
        st.markdown("**Queued Job/Lot pairs:**")
        for idx, (j, l) in enumerate(st.session_state.job_lot_queue):
            st.write(f"{idx+1}. Job {j} – Lot {l}")
        if st.button("🗑️ Clear queue"):
            st.session_state.job_lot_queue.clear()

    st.divider()

    # ── Filters ───────────────────────────────────────────────────────────
    tx_types = st.multiselect("Transaction types", DEFAULT_TX_TYPES, default=DEFAULT_TX_TYPES)

    date_filter = st.checkbox("Filter by last_updated date range")
    date_from = date_to = None
    if date_filter:
        drange = st.date_input("Select start and end dates (inclusive)",
                               value=(date.today().replace(day=1), date.today()))
        if isinstance(drange, tuple) and len(drange) == 2:
            date_from, date_to_inclusive = drange
            # convert inclusive end -> exclusive end (+1 day)
            date_to = date_to_inclusive + timedelta(days=1)
        else:
            st.warning("Please choose a start and end date.")
            st.stop()

    st.divider()

    # ── Header inputs ─────────────────────────────────────────────────────
    title = st.text_input("Title for header (appears after 'I,' line)")

    # ── Generate TXT ──────────────────────────────────────────────────────
    if st.button("🚀 Generate TXT"):
        if not st.session_state.job_lot_queue:
            st.warning("Add at least one Job/Lot pair first.")
            st.stop()
        if not title:
            st.warning("Provide a title for the TXT header.")
            st.stop()

        df = fetch_pulltags(
            pairs          = st.session_state.job_lot_queue,
            tx_types       = tx_types,
            date_from      = date_from,
            date_to        = date_to,
            mark_exported  = True  # <-- flag rows after export
        )

        if df.empty:
            st.error("No pull‑tag rows matched the selected filters.")
            st.stop()

        header_dt = df["last_updated"].min().date() if "last_updated" in df else date.today()

        txt_blob = build_txt(df, title, header_dt)

        # Friendly filename
        file_title = re.sub(r"\W+", "_", title.strip()).strip("_")
        filename = f"{file_title}.txt"

        st.download_button(
            "📥 Download Sage TXT",
            data      = txt_blob,
            file_name = filename,
            mime      = "text/plain",
            key       = "download_txt"
        )

        st.success(f"Generated and flagged {len(df)} IL lines as exported.")
        with st.expander("Preview first 10 lines"):
            st.text("".join(txt_blob.splitlines(True)[:12]))
