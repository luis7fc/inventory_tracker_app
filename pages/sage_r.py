# pages/sage_r.py
import io
import re
from datetime import date, datetime
from typing import List, Tuple, Optional
import uuid
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
    job_lot_pairs: Optional[List[Tuple[str, str]]] = None,
    tx_types: Optional[List[str]] = None,
    statuses: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    warehouses: Optional[List[str]] = None,
) -> pd.DataFrame:
    if not any([job_lot_pairs, tx_types, statuses, warehouses, start_date, end_date]):
        raise ValueError("At least one filter must be applied to query pulltags.")

    with get_db_cursor() as cur:
        sql = """
            SELECT id, job_number, lot_number, item_code, quantity,
                   uom, description, cost_code,
                   warehouse AS location,
                   transaction_type, status, last_updated, note
            FROM pulltags
            WHERE 
              (%(job_lot_pairs)s IS NULL OR (job_number, lot_number) IN (
                SELECT * FROM UNNEST(%(job_lot_pairs)s) AS t(job_number text, lot_number text)
              ))
              AND (%(tx_types)s IS NULL OR transaction_type = ANY(%(tx_types)s))
              AND (%(statuses)s IS NULL OR status = ANY(%(statuses)s))
              AND (%(warehouses)s IS NULL OR warehouse = ANY(%(warehouses)s))
              AND (%(start_date)s IS NULL OR last_updated::date >= %(start_date)s)
              AND (%(end_date)s IS NULL OR last_updated::date <= %(end_date)s)
              AND quantity != 0

            ORDER BY job_number, lot_number, item_code
        """

        cur.execute(sql, {
            "job_lot_pairs": job_lot_pairs if job_lot_pairs else None,
            "tx_types": tx_types if tx_types else None,
            "statuses": statuses if statuses else None,
            "warehouses": warehouses if warehouses else None,
            "start_date": start_date,
            "end_date": end_date,
        })

        rows = cur.fetchall()
        cols = [desc.name for desc in cur.description]
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

def mark_exported(ids: List[str]) -> None:
    """
    Flag the given pull-tag rows as exported.
    Accepts a list of ID *strings* coming from the DataFrame.
    """
    if not ids:
        return

    # Cast every id string to a real UUID object so psycopg2 adapts them
    uuid_ids = [uuid.UUID(x) for x in ids]

    with get_db_cursor() as cur:
        # UUID objects → uuid[] automatically, so the = operator matches
        cur.execute(
            """
            UPDATE pulltags
            SET    status = 'exported'
            WHERE  id = ANY(%s::uuid[])
            """,
            (ids,),
        )

def revert_exported_pulltags(ids: List[str], note: str) -> None:
    if not ids:
        return
    uuid_ids = [uuid.UUID(x) for x in ids]
    with get_db_cursor() as cur:
        cur.execute(
            """
            UPDATE pulltags
            SET status = 'pending',
                note = %s,
                last_updated = %s
            WHERE id = ANY(%s::uuid[])
              AND status = 'exported'
            """,
            (note, datetime.utcnow(), uuid_ids),
        )
# ─────────────────────────────────────────────────────────────────────────────
# TXT builder
# ─────────────────────────────────────────────────────────────────────────────
def build_txt(header: dict, df: pd.DataFrame) -> str:
    kit  = header["kit_date"].strftime("%m-%d-%y")
    acct = header["acct_date"].strftime("%m-%d-%y")

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
    if st.button("🔄 Reset Page"):
        for key in ["job_lot_queue", "job_buffer", "lot_buffer", "show_grid", "pulltag_df", "edited_df", "revert_df"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

    tab1, tab2 = st.tabs(["📤 Export Pull-Tags", "⏪ Revert Exports"])

    with tab1:
    # ── 1) Job / Lot queue form ──────────────────────────────────────────
        with st.form("job_lot_form", clear_on_submit=True):
            c1, c2, c3 = st.columns([3, 3, 1])
            
            # NOTE: no more 'value='parameter
            job = c1.text_input("Job #", value=ss.job_buffer, key="job_buffer")
            lot = c2.text_input("Lot #", value=ss.lot_buffer, key="lot_buffer")
            if c3.form_submit_button("Add"):
                if job and lot:
                    pair = (job.strip(), lot.strip())
                    if pair not in ss.job_lot_queue:
                        ss.job_lot_queue.append(pair)
    
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
        warehouse_filter = st.text_input("Warehouse filter (comma-separated)")
        start_date = st.date_input("Start Date", value=None)
        end_date = st.date_input("End Date", value=None)
    
    
        all_statuses = get_distinct_statuses()
        statuses = st.multiselect("Status filter", all_statuses, default=all_statuses)
        if not statuses:
            statuses = all_statuses
    
        # ── 3) Load pull-tags button ────────────────────────────────────────
        if st.button("🔍 Load pull-tags"):
            try:
                warehouses = [w.strip() for w in warehouse_filter.split(",") if w.strip()] or None
                df = query_pulltags(
                    job_lot_pairs=ss.job_lot_queue or None,
                    tx_types=tx_types or None,
                    statuses=statuses or None,
                    start_date=start_date if start_date else None,
                    end_date=end_date if end_date else None,
                    warehouses=warehouses
                )
                if df.empty:
                    st.warning("No rows match those filters.")
                else:
                    ss.pulltag_df = df
                    ss.edited_df = df.copy()
                    ss.show_grid = True
                    st.rerun()
            except ValueError as e:
                st.error(str(e))
    
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

        with tab2:
                st.subheader("Revert Exported Pulltag")
                job = st.text_input("Job Number", key="revert_job")
                lot = st.text_input("Lot Number", key="revert_lot")
                is_return = st.checkbox("RETURN Transaction", key="revert_type")
                note = st.text_area("Reversion Note", key="revert_note")
        
                tx_type = "Return" if is_return else "Job Issue"
        
                if job and lot:
                    df = query_pulltags(
                        job_lot_pairs=[(job.strip(), lot.strip())],
                        tx_types=[tx_type],
                        statuses=["exported"]
                    )
                    if not df.empty:
                        ss.revert_df = df
                        st.dataframe(df)
                        if st.button("🔁 Revert Export"):
                            revert_exported_pulltags(df["id"].tolist(), note)
                            st.success("Pulltag reverted to 'pending' with note.")
                    else:
                        st.info("No matching exported pulltag found for that Job/Lot/Type.")

