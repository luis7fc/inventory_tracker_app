import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from typing import List, Tuple
import re

from db import get_db_cursor  # shared connection helper

# ==============================================================================
# Sage Re‑export page (replacement for Scan Lookup)
# Primary filter: Job → Lot queue (same UX as Kitting)
# Optional: filter by transaction_type and last_updated date range
# After export, pulltags rows included are marked status='exported'
# ==============================================================================

DEFAULT_TX_TYPES = ["Job Issue", "ADD", "RETURNB", "Return"]

# ───────────────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────────────


def mark_exported(cur, rows: List[Tuple[str, str]]):
    """Set status='exported' for all rows whose (job, lot) tuples are in rows."""
    if not rows:
        return
    cur.execute(
        """
        UPDATE pulltags
           SET status = 'exported'
         WHERE (job_number, lot_number) IN %s
        """,
        (tuple(rows),),
    )


def query_pulltags(cur, pairs: List[Tuple[str, str]], tx_types: List[str], from_dt: datetime, to_dt: datetime):
    """Return a pandas DataFrame of pulltag rows matching the filters."""
    if not pairs:
        return pd.DataFrame()
    cur.execute(
        """
        SELECT job_number, lot_number, item_code, quantity,
               uom, description, cost_code, location,
               transaction_type, warehouse, last_updated
          FROM pulltags
         WHERE (job_number, lot_number) IN %s
           AND transaction_type = ANY(%s)
           AND last_updated >= %s
           AND last_updated < %s
         ORDER BY job_number, lot_number, item_code
        """,
        (tuple(pairs), tx_types, from_dt, to_dt),
    )
    cols = [d.name for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def build_txt(df: pd.DataFrame, title: str, kit_date: date) -> str:
    """Convert filtered DataFrame -> Sage‑compatible TXT string."""
    fmt_date = kit_date.strftime("%m-%d-%y")

    lines = [
        f'I,{title},{fmt_date},{fmt_date},,"",,,,,,,,,\n',
        ',,,,,"",,,,,,,,,\n',
        ';line ID,location,item code,quantity,unit of measure,"description",conversion factor,equipment id,equipment cost code,job,lot,cost code,category,requisition number,issue date\n',
    ]

    for _, r in df.iterrows():
        location   = r.get("location") or ""
        item_code  = r["item_code"]
        qty        = r["quantity"]
        uom        = r.get("uom") or ""
        desc       = r.get("description") or ""
        job        = r["job_number"]
        lot        = r["lot_number"]
        cost_code  = r.get("cost_code") or ""

        il = (
            f'IL,{location},{item_code},{qty},{uom},"{desc}",1,,,'
            f'{job},{lot},{cost_code},M,,{fmt_date}\n'
        )
        lines.append(il)

    return ''.join(lines)

# ───────────────────────────────────────────────────────────────────────────────
# Main page entry
# ───────────────────────────────────────────────────────────────────────────────

def run():
    st.title("📤 Sage Pull‑Tag Re‑export")

    # Ensure session state structures exist
    if "job_lot_queue" not in st.session_state:
        st.session_state.job_lot_queue = []

    # Sidebar queue input ------------------------------------------------------
    with st.sidebar:
        st.subheader("📋 Select Job & Lot")
        job_input = st.text_input("Job #", key="job_in")
        lot_input = st.text_input("Lot #", key="lot_in")

        def _add_pair():
            if job_input.strip() and lot_input.strip():
                st.session_state.job_lot_queue.append((job_input.strip(), lot_input.strip()))
                st.session_state.job_in = ""
                st.session_state.lot_in = ""
        st.button("➕ Add", on_click=_add_pair)

        if st.session_state.job_lot_queue:
            st.write("### Current queue")
            for j, l in st.session_state.job_lot_queue:
                st.write(f"• **Job {j} – Lot {l}**")
            if st.button("🗑️ Clear Queue"):
                st.session_state.job_lot_queue = []

    # Main controls ------------------------------------------------------------
    title = st.text_input("TXT Title", placeholder="e.g. FNOSOLAR 4-2 Kits")
    col1, col2 = st.columns(2)
    with col1:
        from_date = st.date_input("From (inclusive)")
    with col2:
        to_date = st.date_input("To (exclusive)")

    tx_types = st.multiselect("Transaction Types", DEFAULT_TX_TYPES, default=DEFAULT_TX_TYPES)

    if st.button("🚀 Generate TXT"):
        if not st.session_state.job_lot_queue:
            st.error("Add at least one Job/Lot pair to the queue.")
            return
        if not title:
            st.error("Please enter a TXT title.")
            return
        # Resolve date range
        if from_date and to_date and from_date >= to_date:
            st.error("'From' date must be before 'To' date.")
            return
        if not from_date:
            from_date = date(1970, 1, 1)
        if not to_date:
            to_date = date.today() + timedelta(days=1)

        with get_db_cursor() as cur:
            df = query_pulltags(cur, st.session_state.job_lot_queue, tx_types, datetime.combine(from_date, datetime.min.time()), datetime.combine(to_date, datetime.min.time()))
            if df.empty:
                st.warning("No rows matched your filters.")
                return
            txt_blob = build_txt(df, title, from_date)
            mark_exported(cur, st.session_state.job_lot_queue)

        st.success(f"Exported {len(df)} rows – status updated to 'exported'.")
        st.download_button("📥 Download .txt", txt_blob, file_name=f"{re.sub(r'\W+', '_', title)}.txt", mime="text/plain")
        st.dataframe(df)
