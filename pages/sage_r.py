# pages/sage_r.py
import io
import re
from datetime import date
from typing import List, Tuple

import pandas as pd
import streamlit as st

from db import get_db_cursor


# ─────────────────────────────────────────────────────────────────────────────
# Session-state bootstrap
# ─────────────────────────────────────────────────────────────────────────────
def _init_session_state() -> None:
    ss = st.session_state
    ss.setdefault("job_lot_queue", [])       # List[Tuple[job, lot]]
    ss.setdefault("job_buffer", "")
    ss.setdefault("lot_buffer", "")
    ss.setdefault("show_grid", False)
    ss.setdefault("pulltag_df", pd.DataFrame())
    ss.setdefault("edited_df", pd.DataFrame())


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────
def get_distinct_statuses() -> List[str]:
    with get_db_cursor() as cur:
        cur.execute("SELECT DISTINCT status FROM pulltags ORDER BY status;")
        return [r[0] for r in cur.fetchall()]


def query_pulltags(
    job_lot_pairs: List[Tuple[str, str]],
    tx_types: List[str],
    statuses: List[str],
) -> pd.DataFrame:
    if not job_lot_pairs:
        return pd.DataFrame()

    if not statuses:                  # fall back to "show all"
        statuses = get_distinct_statuses()

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
        cur.execute(sql, (tuple(job_lot_pairs), tx_types, statuses))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def save_changes_to_db(df: pd.DataFrame) -> None:
    with get_db_cursor() as cur:
        for r in df.itertuples():
            cur.execute(
                """
                UPDATE pulltags
                SET quantity=%s, uom=%s, description=%s,
                    cost_code=%s, warehouse=%s,
                    transaction_type=%s, status=%s
                WHERE id=%s
                """,
                (
                    r.quantity, r.uom, r.description,
                    r.cost_code, r.location,
                    r.transaction_type, r.status,
                    r.id,
                ),
            )


def mark_exported(ids: List[int]) -> None:
    if ids:
        with get_db_cursor() as cur:
            cur.execute(
                "UPDATE pulltags SET status = 'exported' WHERE id = ANY(%s)",
                (ids,),
            )


# ─────────────────────────────────────────────────────────────────────────────
# TXT builder
# ─────────────────────────────────────────────────────────────────────────────
def build_txt(header: dict, df: pd.DataFrame) -> str:
    kit  = header["kit_date"].strftime("%m/%d/%Y")
    acct = header["acct_date"].strftime("%m/%d/%Y")

    buf = io.StringIO()
    buf.write(f"I,{header['batch']},{kit},{acct}\n")
    for r in df.itertuples():
        desc = (r.description or "").replace('"', "'")
        buf.write(
            f"IL,{r.location},{r.item_code},{r.quantity},{r.uom},"
            f"\"{desc}\",1,,,"
            f"{r.job_number},{r.lot_number},{r.cost_code},M,,{kit}\n"
        )
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN UI
# ─────────────────────────────────────────────────────────────────────────────
def run() -> None:
    _init_session_state()
    ss = st.session_state

    st.title("📤 Sage Pull-Tag Export")

    # ── 1) Job / Lot queue form ──────────────────────────────────────────
    with st.form("job_lot_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([3, 3, 1])
        job = c1.text_input("Job #", value=ss.job_buffer, key="job_buffer")
        lot = c2.text_input("Lot #", value=ss.lot_buffer, key="lot_buffer")
        if c3.form_submit_button("Add"):
            if job and lot:
                pair = (job.strip(), lot.strip())
                if pair not in ss.job_lot_queue:
                    ss.job_lot_queue.append(pair)
            ss.job_buffer = ss.lot_buffer = ""

    if ss.job_lot_queue:
        st.write("**Queued lots:**")
        for jb, lt in ss.job_lot_queue:
            st.write(f"• Job **{jb}** – Lot **{lt}**")
        if st.button("🗑️ Clear list"):
            ss.job_lot_queue.clear()
            ss.pop("show_grid", None)
            ss.pop("pulltag_df", None)
            ss.pop("edited_df", None)
            st.rerun()
    else:
        st.info("Add at least one Job/Lot pair above.")

    st.markdown("---")

    # ── 2) Filters ───────────────────────────────────────────────────────
    default_tx = ["Job Issue", "ADD", "RETURNB", "Return"]
    tx_types = st.multiselect("Transaction Types", default_tx, default=default_tx)

    all_statuses = get_distinct_statuses()
    statuses = st.multiselect("Status filter", all_statuses, default=all_statuses)
    if not statuses:
        statuses = all_statuses

    # ── 3) Load pull-tags button ────────────────────────────────────────
    if st.button("🔍 Load pull-tags"):
        df = query_pulltags(ss.job_lot_queue, tx_types, statuses)
        if df.empty:
            st.warning("No rows match those filters.")
        else:
            ss.pulltag_df = df
            ss.edited_df = df.copy()
            ss.show_grid = True
            st.rerun()

    # ── 4) Editable grid ────────────────────────────────────────────────
    if ss.get("show_grid"):
        st.subheader("Review & Edit Pull-Tags")
        column_cfg = {col: {"disabled": True} for col in
                      ("id", "job_number", "lot_number", "item_code")}
        edited_df = st.data_editor(
            ss.edited_df,
            num_rows="dynamic",
            column_config=column_cfg,
            key="edit_grid"
        )
        ss.edited_df = edited_df

        if st.button("💾 Save changes to DB"):
            save_changes_to_db(edited_df)
            st.success("Changes saved to database.")

    st.markdown("---")

    # ── 5) Header + Export ──────────────────────────────────────────────
    st.subheader("Export to Sage")
    col1, col2, col3 = st.columns(3)
    batch     = col1.text_input("Batch Name")
    kit_date  = col2.date_input("Kit Date",  value=date.today())
    acct_date = col3.date_input("Accounting Date", value=date.today())

    if st.button("🚀 Generate & Download TXT"):
        if ss.edited_df.empty:
            st.warning("Load pull-tags first.")
            st.stop()
        if not batch.strip():
            st.warning("Batch name required.")
            st.stop()

        txt_payload = build_txt(
            {"batch": batch.strip(), "kit_date": kit_date, "acct_date": acct_date},
            ss.edited_df
        )
        fname = re.sub(r"\W+", "_", batch.strip()) + ".txt"
        st.download_button("📥 Download", txt_payload,
                           file_name=fname, mime="text/plain")

        mark_exported(ss.edited_df["id"].tolist())
        st.success("Rows marked **exported**.")
