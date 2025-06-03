"""
Refactored Multi‑Lot Job Kitting module  — v3 (2025‑06‑02)
=========================================================
**Key features**
---------------------------------------------------------
* Duplicate‑scan rejection with ❌ error feedback.
* `kitted_qty = 0` ➜ pull‑tag row deletion.
* Negative quantities permitted **only** on `transaction_type == "Return"` rows, with scan‑count check on `abs(kitted_qty)`.
* Notes field persisted back to `pulltags.notes` with `last_updated` stamp.
* Prefilled `kitted_qty` from `qty_req` for minimal typing.
"""

import streamlit as st
import pandas as pd
import re
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from fpdf import FPDF
from io import BytesIO
import logging
import uuid
from psycopg2 import OperationalError, IntegrityError
from db import (
    get_db_cursor,
)

# ─── Logging 
logging.basicConfig(level=logging.INFO, filename="kitting_app.log")
logger = logging.getLogger(__name__)

EDIT_ANCHOR = "scan-edit"

# ─── Exceptions 
class ScanMismatchError(Exception):
    """Qty ≠ #scans."""

class ExportedPulltagError(Exception):
    """Pull‑tag already exported."""

class DuplicateScanError(Exception):
    """Same scan‑ID used twice."""

# ─── Helpers 

def get_timezone():
    try:
        return ZoneInfo(st.secrets.get("APP_TIMEZONE", "America/Los_Angeles"))
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def validate_alphanum(v: str, field: str) -> bool:
    if not re.match(r"^[A-Za-z0-9\-]+$", v):
        st.error(f"{field} must be alphanumeric (dashes allowed).")
        return False
    return True

def render_scan_inputs():
    st.markdown("## 🧪 Item Scans Required")

    if not st.session_state.pulltag_editor_df:
        st.info("Load pulltags to begin scanning.")
        return

    # 1. Group required scan counts per item_code
    from collections import defaultdict

    item_requirements = defaultdict(int)
    item_meta = {}

    for (job, lot), df in st.session_state.pulltag_editor_df.items():
        for _, row in df.iterrows():
            if row["scan_required"]:
                ic = row["item_code"]
                item_requirements[ic] += abs(int(row["kitted_qty"]))
                item_meta[ic] = {
                    "description": row.get("description", ""),
                }

    # 2. Initialize input fields per item_code
    new_scan_map = {}
    for item_code, qty_needed in item_requirements.items():
        label = f"🔍 Scan for `{item_code}` ({item_meta[item_code]['description']}) — Need {qty_needed} unique scans"
        input_key = f"scan_input_{item_code}"
        raw = st.text_area(label, key=input_key, help="Enter one scan ID per line or comma-separated")
        scan_list = list(filter(None, re.split(r"[\s,]+", raw.strip())))
        new_scan_map[item_code] = scan_list

    # 3. Validation + populate scan_buffer
    if st.button("✅ Validate Scans"):
        st.session_state.scan_buffer.clear()
        errors = []
        for item_code, expected_qty in item_requirements.items():
            scans = new_scan_map[item_code]
            unique_scans = list(dict.fromkeys(scans))  # remove dups, preserve order
            if len(unique_scans) != expected_qty:
                errors.append(f"{item_code}: Expected {expected_qty}, got {len(unique_scans)} unique scans.")
            for sid in unique_scans:
                # Distribute scans evenly across job/lot entries
                for (job, lot), df in st.session_state.pulltag_editor_df.items():
                    if item_code in df["item_code"].values:
                        st.session_state.scan_buffer.append((job, lot, item_code, sid))
                        break  # stop after first match

        if errors:
            st.error("❌ Scan mismatch:\n" + "\n".join(errors))
        else:
            st.success("✅ All scans validated and assigned.")


def generate_finalize_summary_pdf(rows, user, ts):
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 12)
    pdf.cell(270, 10, "CRS Final Scan Summary Report", ln=True, align="C")
    pdf.set_font("Arial", size=9)
    pdf.cell(270, 6, f"Verified by: {user}   |   Date: {ts}", ln=True, align="C")
    pdf.ln(4)
    headers = ["Job", "Lot", "Item", "Description", "Scan ID", "Qty"]
    widths = [30, 25, 25, 110, 60, 15]
    for h, w in zip(headers, widths):
        pdf.cell(w, 6, h, 1)
    pdf.ln()
    pdf.set_font_size(8)
    for r in rows:
        vals = [r["job_number"], r["lot_number"], r["item_code"], r["item_description"], r.get("scan_id") or "‑", str(r["qty"])]
        for v, w in zip(vals, widths):
            pdf.cell(w, 6, str(v), 1)
        pdf.ln()
    buf = BytesIO()
    pdf.output(buf)
    buf.seek(0)
    return buf


def bootstrap_state():
    base = {
        "session_id": str(uuid.uuid4()),
        "job_lot_queue": [],
        "pulltag_editor_df": {},
        "location": "",
        "scan_buffer": [],   # list[(job, lot, item, scan_id)]
        "user": st.experimental_user.get("username", "unknown"),
        "confirm_kitting": False,
    }
    for k, v in base.items():
        st.session_state.setdefault(k, v)
# ----Load PT rows
def get_pulltag_rows(job: str, lot: str) -> list[dict]:
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT 
                id AS pulltag_id,
                warehouse,
                job_number,
                lot_number,
                item_code,
                description,
                quantity AS qty_req,
                uom,
                cost_code,
                status,
                transaction_type,
                last_updated,
                note
            FROM pulltags
            WHERE job_number = %s AND lot_number = %s
        """, (job, lot))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]

# ─── Data fetch 
def load_pulltags(job: str, lot: str) -> pd.DataFrame:
    rows = get_pulltag_rows(job, lot)
    if not rows:
        st.warning(f"No pull‑tags for {job}-{lot}")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if (df["status"] == "exported").any():
        st.warning(f"❌ {job}-{lot} was already exported. Kitting not allowed.")
        return pd.DataFrame()
    if (df["status"] == "kitted").any():
        st.warning(f"⚠️ Auto‑kitted on {pd.to_datetime(df['last_updated']).max():%Y‑%m‑%d %H:%M}")
    df = df[df["transaction_type"].isin(["Job Issue", "Return"])]
    with get_db_cursor() as cur:
        cur.execute("SELECT item_code FROM items_master WHERE scan_required")
        scan_set = {r[0] for r in cur.fetchall()}
    df["scan_required"] = df["item_code"].isin(scan_set)
    df["kitted_qty"] = df["qty_req"]
    df["note"].fillna("", inplace=True)
    df["warehouse"].fillna("MAIN", inplace=True)
    return df

# ─── Finalise 

def finalise():
    # 1️⃣ validate non‑Return negatives
    for df in st.session_state.pulltag_editor_df.values():
        bad = df[(df["transaction_type"] != "Return") & (df["kitted_qty"] < 0)]
        if not bad.empty:
            st.error("Negative qty only allowed on Return lines.")
            return
    summaries, tx, scans, inv, upd, dels, note_upd = [], [], [], [], [], [], []
    sb = st.session_state.scan_buffer

    # 2️⃣ build changes
    for (job, lot), df in st.session_state.pulltag_editor_df.items():
        buf = {(j, l, ic): [] for j, l, ic, _ in sb}
        for j, l, ic, sid in sb:
            buf[(j, l, ic)].append(sid)
        for _, r in df.iterrows():
            ic, qty, tx_type = r["item_code"], int(r["kitted_qty"]), r["transaction_type"]
            wh, loc = r["warehouse"], st.session_state.location
            if qty == 0:
                dels.append((job, lot, ic))
                continue
            sc = buf.get((job, lot, ic), [])
            if r["scan_required"] and len(set(sc)) != abs(qty):
                raise ScanMismatchError(f"{job}-{lot}-{ic}: need {abs(qty)} scans, got {len(set(sc))}.")
            qty_abs = abs(qty)
            if tx_type == "Job Issue":
                tx.append((tx_type, wh, loc, job, lot, ic, qty, st.session_state.user))
                inv.append((ic, loc, -qty_abs, wh))
            else:  # Return
                tx.append((tx_type, wh, loc, job, lot, ic, qty, st.session_state.user))
                inv.append((ic, loc, qty_abs, wh))
            for sid in sc:
                scans.append((ic, sid, job, lot, loc, tx_type, wh, st.session_state.user))
                summaries.append({"job_number": job, "lot_number": lot, "item_code": ic,
                                   "item_description": r.get("description", ""), "scan_id": sid, "qty": 1})
            if not sc:
                summaries.append({"job_number": job, "lot_number": lot, "item_code": ic,
                                   "item_description": r.get("description", ""), "scan_id": None, "qty": qty_abs})
            upd.append(("kitted" if tx_type == "Job Issue" else "returned", job, lot, ic))
            if r["note"]:
                note_upd.append((r["note"], job, lot, ic))

    # 3️⃣ commit
    try:
        with get_db_cursor() as cur, cur.connection:
            for d in tx:
                if d[0] == "Job Issue":
                    cur.execute("""INSERT INTO transactions (transaction_type, date, warehouse, from_location, job_number, lot_number, item_code, quantity, user_id) VALUES (%s,NOW(),%s,%s,%s,%s,%s,%s,%s)""", d)
                else:
                    cur.execute("""INSERT INTO transactions (transaction_type, date, warehouse, to_location, job_number, lot_number, item_code, quantity, user_id) VALUES (%s,NOW(),%s,%s,%s,%s,%s,%s,%s)""", d)
            if scans:
                cur.executemany("""INSERT INTO scan_verifications (item_code, scan_id, job_number, lot_number, scan_time, location, transaction_type, warehouse, scanned_by) VALUES (%s,%s,%s,%s,NOW(),%s,%s,%s,%s)""", scans)
            if inv:
                cur.executemany("""INSERT INTO current_inventory (item_code, location, quantity, warehouse) VALUES (%s,%s,%s,%s) ON CONFLICT (item_code, location, warehouse) DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity""", inv)
            if upd:
                cur.executemany("""UPDATE pulltags SET status=%s, last_updated=NOW() WHERE job_number=%s AND lot_number=%s AND item_code=%s""", upd)
            if note_upd:
                cur.executemany("""UPDATE pulltags SET note=%s, last_updated=NOW() WHERE job_number=%s AND lot_number=%s AND item_code=%s""", note_upd)
            if dels:
                cur.executemany("""DELETE FROM pulltags WHERE job_number=%s AND lot_number=%s AND item_code=%s""", dels)
            cur.connection.commit()
    except Exception as e:
        st.error(str(e))
        logger.exception("Finalisation failed")
        return

    pdf = generate_finalize_summary_pdf(summaries, st.session_state.user,
                                        datetime.now(get_timezone()).strftime("%Y-%m-%d %H:%M"))
    st.download_button("📄 Download Final Scan Summary", pdf, file_name="final_scan_summary.pdf", mime="application/pdf")
    st.session_state.scan_buffer.clear()

# ─── Main UI ───────────────────────────────────────────────────────────────────

def run():
    bootstrap_state()
    st.title("📦 Multi-Lot Job Kitting")

    # ─── Select Job + Lot ─────────────────────────────────────────────────────
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        job = st.text_input("Job Number", key="job_input")
    with col2:
        lot = st.text_input("Lot Number", key="lot_input")
    with col3:
        if st.button("➕ Load Pull‑Tags"):
            if not validate_alphanum(job, "Job Number") or not validate_alphanum(lot, "Lot Number"):
                return
            df = load_pulltags(job, lot)
            if not df.empty:
                st.session_state.pulltag_editor_df[(job, lot)] = df

    # ─── Staging Location ─────────────────────────────────────────────────────
    st.session_state.location = st.text_input("Staging Location", value=st.session_state.location or "")

    # ─── Scan Entry ───────────────────────────────────────────────────────────
    with st.expander("🔍 Scan Input"):
        render_scan_inputs()

        if st.session_state.scan_buffer:
            st.markdown("### 📋 Scan Buffer")
            st.table(pd.DataFrame(st.session_state.scan_buffer, columns=["Job", "Lot", "Item", "Scan ID"]))

    # ─── Pull‑Tag Editors 
    for (job, lot), df in list(st.session_state.pulltag_editor_df.items()):
        st.markdown(f"### 🛠 Editing Pull‑Tags for `{job}-{lot}`")
    
        col1, col2 = st.columns([6, 1])
        with col2:
            if st.button(f"❌ Remove `{job}-{lot}`", key=f"remove_{job}_{lot}"):
                del st.session_state.pulltag_editor_df[(job, lot)]
                continue
    
        with col1:
            edited_df = st.data_editor(
                df[["item_code", "description", "qty_req", "kitted_qty", "note"]],
                key=f"{EDIT_ANCHOR}_{job}_{lot}",
                num_rows="dynamic",
                use_container_width=True,
                column_config={
                    "kitted_qty": st.column_config.NumberColumn("Kitted Qty"),
                    "note": st.column_config.TextColumn("Notes"),
                }
            )
            # Update session copy
            for i, (_, row) in enumerate(edited_df.iterrows()):
                st.session_state.pulltag_editor_df[(job, lot)].iloc[i]["kitted_qty"] = row["kitted_qty"]
                st.session_state.pulltag_editor_df[(job, lot)].iloc[i]["note"] = row["note"]

    # ─── Finalize 

    if st.button("✅ Finalize Kitting"):
        finalise()
