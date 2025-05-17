import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from typing import List, Tuple
import re

from db import get_db_cursor  # shared DB helper

# ==============================================================================
# Sage Re‑export Page (replaces Scan Lookup)
# ----------------------------------------------------------------------------
# Primary filter hierarchy: Job ➜ Lot (queue like Kitting)
# Optional filters: transaction_type list, last_updated date range
# Output: single combined .txt file ready for Sage import + status flag update
# ==============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# Session‑state initialiser
# ─────────────────────────────────────────────────────────────────────────────

def _init_session_state() -> None:
    """Ensure required session-state keys exist."""
    ss = st.session_state

    # ⚠️ 1.  Don’t mix attribute assignment with type annotations
    #        (obj.x: int = ... is a SyntaxError at runtime).
    if "job_lot_queue" not in ss:
        ss.job_lot_queue = []                          # type: List[Tuple[str, str]]

    if "job_buffer" not in ss:
        ss.job_buffer = ""

    if "lot_buffer" not in ss:
        ss.lot_buffer = ""

# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def query_pulltags(
    job_lot_pairs: List[Tuple[str, str]],
    tx_types: List[str],
    from_dt: datetime,
    to_dt_exclusive: datetime,
) -> pd.DataFrame:
    """Return pulltags rows matching the filters.

    * `from_dt` inclusive, `to_dt_exclusive` exclusive (UTC‑agnostic)
    * Uses Postgres row‑value IN ((job,lot), ...) pattern for simplicity.
    """
    if not job_lot_pairs:
        return pd.DataFrame()

    with get_db_cursor() as cur:
        sql = """
            SELECT id, job_number, lot_number, item_code, quantity,
                   uom, description, cost_code, location,
                   transaction_type, warehouse, last_updated
            FROM   pulltags
            WHERE  (job_number, lot_number) IN %s
              AND  transaction_type = ANY (%s)
              AND  last_updated >= %s
              AND  last_updated <  %s
            ORDER  BY job_number, lot_number, item_code
        """
        cur.execute(sql, (tuple(job_lot_pairs), tx_types, from_dt, to_dt_exclusive))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]

    return pd.DataFrame(rows, columns=cols)


def mark_exported(ids: List[int]) -> None:
    """Set status='exported' for the given pulltags IDs."""
    if not ids:
        return
    with get_db_cursor() as cur:
        cur.execute(
            "UPDATE pulltags SET status = %s WHERE id = ANY(%s)",
            ("exported", ids),
        )


# ─────────────────────────────────────────────────────────────────────────────
# TXT builder
# ─────────────────────────────────────────────────────────────────────────────

def build_txt(df: pd.DataFrame, title: str) -> str:
    """Build Sage‑compatible TXT string from a filtered pulltags DataFrame."""
    if df.empty:
        return ""

    kit_date = df["last_updated"].min().date()
    fmt_date = kit_date.strftime("%m-%d-%y")

    lines: List[str] = [
        f"I,{title},{fmt_date},{fmt_date},,\"\",,,,,,,,,\n",
        ",,,,\"\",,,,,,,,,\n",
    ]

    for _, row in df.iterrows():
        location = row["location"] or row["warehouse"] or ""
        item_code = row["item_code"]
        qty = row["quantity"]
        uom = row["uom"] or ""
        desc = (row["description"] or "").ljust(45)  # pad for readability
        cost_code = row["cost_code"] or item_code
        job = row["job_number"]
        lot = row["lot_number"]

        il = (
            f"IL,{location},{item_code},{qty},{uom},\"{desc}\",1,,,"
            f"{job},{lot},{cost_code},M,,{fmt_date}\n"
        )
        lines.append(il)

    return "".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────────────────────────────────────

def run():
    _init_session_state()

    st.title("📤 Sage Pull‑Tag Export")

    # ── 1) Job / Lot queue form ─────────────────────────────────────
    with st.form("job_lot_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([3, 3, 1])
        job = c1.text_input("Job #", key="job_buffer")
        lot = c2.text_input("Lot #", key="lot_buffer")
        added = c3.form_submit_button("Add")

        if added and job and lot:
            pair = (job.strip(), lot.strip())
            if pair not in st.session_state.job_lot_queue:   # ← skip dups
                st.session_state.job_lot_queue.append(pair)

    # ── 1b) Display & clear queue ────────────────────────────────────
    if st.session_state.job_lot_queue:
        st.write("**Queued lots:**")
        for jb, lt in st.session_state.job_lot_queue:
            st.write(f"• Job **{jb}** – Lot **{lt}**")

        if st.button("🗑️ Clear list"):
            st.session_state.job_lot_queue.clear()
    else:
        st.info("Add at least one Job/Lot pair above.")

    st.markdown("---")

    # ── 2) Optional filters ───────────────────────────────────────────────
    default_tx = ["Job Issue", "ADD", "RETURNB", "Return"]
    tx_types = st.multiselect("Transaction Types", default_tx, default=default_tx)

    col_from, col_to = st.columns(2)
    from_date = col_from.date_input("From date (inclusive)",
                                    value=date.today().replace(day=1))
    to_date = col_to.date_input("To date (inclusive)", value=date.today())

    st.markdown("---")

    # ── 3) TXT header metadata ────────────────────────────────────────────
    title = st.text_input("TXT Title")

    if st.button("🚀 Generate TXT"):
        # Validation
        if not st.session_state.job_lot_queue:
            st.warning("Please add at least one Job/Lot pair.")
            st.stop()
        if not title.strip():
            st.warning("Please enter a TXT title.")
            st.stop()
        if from_date > to_date:
            st.warning("'From' date must be on or before 'To' date.")
            st.stop()

        # Build query window (inclusive‑inclusive)
        start_dt = datetime.combine(from_date, datetime.min.time())
        end_dt_excl = datetime.combine(to_date + timedelta(days=1), datetime.min.time())

        df = query_pulltags(st.session_state.job_lot_queue, tx_types, start_dt, end_dt_excl)
        if df.empty:
            st.warning("No pulltags match the selected criteria.")
            st.stop()

        txt_blob = build_txt(df, title.strip())
        safe_name = re.sub(r"\W+", "_", title.strip()) + ".txt"

        clicked = st.download_button(
            "📥 Download TXT",
            data=txt_blob,
            file_name=safe_name,
            mime="text/plain",
        )

        st.dataframe(df)

        # Update status after successful download
        if clicked:
            mark_exported(df["id"].tolist())
            st.success("Rows marked as **exported**.")
