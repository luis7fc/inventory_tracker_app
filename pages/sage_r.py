import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from typing import List, Tuple
import re
import psycopg2                 # or whichever driver you're using

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
def get_distinct_statuses() -> list[str]:
    with get_db_cursor() as cur:
        cur.execute("SELECT DISTINCT status FROM pulltags ORDER BY status;")
        return [r[0] for r in cur.fetchall()]

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
# ── DB query ─────────────────────────────────────────────────────────

def query_pulltags(
    job_lot_pairs: List[Tuple[str, str]],
    tx_types: List[str],
    statuses: List[str],
) -> pd.DataFrame:

    if not job_lot_pairs:
        return pd.DataFrame()

    if not statuses:            # treat “no selection” as “all statuses”
        with get_db_cursor() as cur:
            cur.execute("SELECT DISTINCT status FROM pulltags;")
            statuses = [r[0] for r in cur.fetchall()]

    with get_db_cursor() as cur:
        sql = """
            SELECT id, job_number, lot_number, item_code, quantity,
                   uom, description, cost_code,
                   warehouse AS location,
                   transaction_type, status
            FROM   pulltags
            WHERE  (job_number, lot_number) IN %s
              AND  transaction_type = ANY(%s)
              AND  status           = ANY(%s)
            ORDER  BY job_number, lot_number, item_code
        """
        cur.execute(sql,
                    (tuple(job_lot_pairs), tx_types, statuses))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]

    return pd.DataFrame(rows, columns=cols)


# ── TXT builder ─────────────────────────────────────────────────────
def build_txt(header: dict, df: pd.DataFrame) -> str:
    """header = {'batch':..., 'kit_date': date, 'acct_date': date}"""

    kit = header["kit_date"].strftime("%m/%d/%Y")
    acct = header["acct_date"].strftime("%m/%d/%Y")

    out = io.StringIO()
    # I-line (Batch, Kit Date, Accounting Date)
    out.write(f"I,{header['batch']},{kit},{acct}\n")

    # IL lines
    for r in df.itertuples():
        desc = (r.description or "").replace('"', "'")       # Sage hates quotes
        out.write(
            f"IL,{r.location},{r.item_code},{r.quantity},{r.uom},"
            f"\"{desc}\",1,,,"
            f"{r.job_number},{r.lot_number},{r.cost_code},M,,{kit}\n"
        )
    return out.getvalue()

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

    # 2) Optional filters -------------------------------------------------
    default_tx = ["Job Issue", "ADD", "RETURNB", "Return"]
    tx_types   = st.multiselect("Transaction Types", default_tx, default=default_tx)

    # dynamic status list ────────────────────────────────────────────────
    all_statuses = get_distinct_statuses()                # helper defined above run()
    # If user unticks everything we’ll treat that as “show all”
    statuses = st.multiselect("Status filter",
                              all_statuses,
                              default=all_statuses)
    if not statuses:      # show-all fallback
        statuses = all_statuses

    st.markdown("---")

    # 3) Pull tags grid ---------------------------------------------------
    if st.button("🔍 Load pull-tags"):
        df = query_pulltags(st.session_state.job_lot_queue, tx_types, statuses)

        if df.empty:
            st.warning("No rows match those filters.")
            st.stop()

        # editable grid
        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            key="edit_grid",
            hide_index=True
        )

        # commit any edits
        if st.button("💾 Save changes to DB"):
            with get_db_cursor() as cur:
                for row in edited_df.itertuples():
                    cur.execute(
                        """
                        UPDATE pulltags
                        SET quantity=%s, uom=%s, description=%s,
                            cost_code=%s, warehouse=%s, transaction_type=%s, status=%s
                        WHERE id=%s
                        """,
                        (
                            row.quantity, row.uom, row.description,
                            row.cost_code, row.location, row.transaction_type, row.status,
                            row.id
                        )
                    )
            st.success("Updates saved!")

    # 4) Header + TXT export ---------------------------------------------
    st.markdown("### Export to Sage")

    col1, col2, col3 = st.columns(3)
    batch      = col1.text_input("Batch name")
    kit_date   = col2.date_input("Kit Date",  value=date.today())
    acct_date  = col3.date_input("Accounting Date", value=date.today())

    if st.button("🚀 Generate & Download TXT"):
        grid_df = pd.DataFrame(st.session_state["edit_grid"]["edited_rows"])
        if grid_df.empty:
            st.warning("Nothing to export — load & edit rows first.")
            st.stop()

        txt_data = build_txt(
            {"batch": batch.strip(), "kit_date": kit_date, "acct_date": acct_date},
            grid_df
        )

        fname = re.sub(r"\W+", "_", batch.strip() or "export") + ".txt"
        downloaded = st.download_button("📥 Download", txt_data, file_name=fname,
                                        mime="text/plain")

        if downloaded:
            mark_exported(grid_df["id"].tolist())
            st.success("Rows marked **exported**.")
