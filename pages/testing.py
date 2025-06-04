import streamlit as st
import json
import pandas as pd
import re
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from fpdf import FPDF
from io import BytesIO
import logging
import uuid
from psycopg2 import OperationalError, IntegrityError
from collections import defaultdict
from db import get_db_cursor

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
def sync_editor_edits():
    for (job, lot), df in st.session_state.pulltag_editor_df.items():
        editor_key = f"{EDIT_ANCHOR}_{job}_{lot}"
        ui_df = st.session_state.get(editor_key)
        if isinstance(ui_df, pd.DataFrame):
            try:
                # Use item_code for robust alignment
                ui_df = ui_df.set_index("item_code")
                df = df.set_index("item_code")
                for item_code in df.index:
                    if item_code in ui_df.index:
                        df.at[item_code, "kitted_qty"] = ui_df.at[item_code, "kitted_qty"]
                        df.at[item_code, "note"] = ui_df.at[item_code, "note"]
                        logger.info(f"[sync_editor_edits] {job}-{lot}-{item_code} → kitted_qty={df.at[item_code, 'kitted_qty']}")
                df.reset_index(inplace=True)
                st.session_state.pulltag_editor_df[(job, lot)] = df
            except Exception as e:
                logger.error(f"[sync_editor_edits] Failed for {job}-{lot}: {e}")
        else:
            logger.warning(f"[sync_editor_edits] Skipped {editor_key} (not a DataFrame)")

def validate_scan_location(cur, scan_id, trans_type, expected_location=None, expected_item_code=None):
    cur.execute("SELECT location, item_code FROM current_scan_location WHERE scan_id = %s", (scan_id,))
    row = cur.fetchone()
    if trans_type == "Job Issue":
        if not row:
            raise Exception(f"Scan {scan_id} is not registered to any location.")
        location, item_code = row
        if location != expected_location:
            raise Exception(f"Scan {scan_id} is at {location}, not {expected_location}.")
        if expected_item_code and item_code != expected_item_code:
            raise Exception(f"Scan {scan_id} is registered to item {item_code}, not {expected_item_code}.")
    elif trans_type == "Return":
        if row:
            location, item_code = row
            raise Exception(f"Scan {scan_id} is already assigned to location {location}. Cannot return again.")
    else:
        raise ValueError(f"Unsupported transaction type: {trans_type}")

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


def compute_scan_requirements():
    logger.info("[compute_scan_requirements] START")
    for (job, lot), df in st.session_state.pulltag_editor_df.items():
        logger.info(f"[CSR] {job}-{lot} → {df[['item_code', 'kitted_qty']].to_dict()}")

    if not st.session_state.pulltag_editor_df:
        st.session_state.item_requirements = {}
        st.session_state.item_meta = {}
        return
    item_requirements = defaultdict(int)
    item_meta = {}
    errors = []
    for (job, lot), df in st.session_state.pulltag_editor_df.items():
        for _, row in df.iterrows():
            if row["scan_required"]:
                ic = row["item_code"]
                try:
                    qty = int(row["kitted_qty"])
                    item_requirements[ic] += abs(qty)
                    description = row.get("description", "")
                    if ic in item_meta and item_meta[ic]["description"] != description:
                        st.warning(f"Inconsistent description for {ic}: using '{description}'")
                    item_meta[ic] = {"description": description}
                except (ValueError, TypeError):
                    errors.append(f"Invalid kitted quantity for item {ic} in {job}-{lot}")
    if errors:
        st.error("❌ Scan requirement errors:\n" + "\n".join(errors))
        st.session_state.item_requirements = {}
        st.session_state.item_meta = {}
    else:
        st.session_state.item_requirements = item_requirements
        st.session_state.item_meta = item_meta

def render_scan_inputs():
    st.markdown("## 🧪 Item Scans Required")
    sync_editor_edits()
    compute_scan_requirements()
    logger.info(f"[render_scan_inputs] FINAL item_requirements: {st.session_state.get('item_requirements', {})}")
    if not st.session_state.pulltag_editor_df:
        st.info("Load pulltags to begin scanning.")
        return
    item_requirements = st.session_state.get("item_requirements", {})
    item_meta = st.session_state.get("item_meta", {})
    new_scan_map = {}
    for item_code, qty_needed in item_requirements.items():
        label = f"🔍 Scan for `{item_code}` ({item_meta[item_code]['description']}) — Need {qty_needed} unique scans"
        input_key = f"scan_input_{item_code}"
        raw = st.text_area(label, key=input_key, help="Enter one scan ID per line or comma-separated")
        scan_list = list(filter(None, re.split(r"[\s,]+", raw.strip())))
        new_scan_map[item_code] = scan_list
    if st.button("✅ Validate Scans"):
        st.session_state.scan_buffer.clear()
        errors = []
        for item_code, expected_qty in item_requirements.items():
            scans = new_scan_map[item_code]
            unique_scans = list(dict.fromkeys(scans))
            if len(unique_scans) != expected_qty:
                errors.append(f"{item_code}: Expected {expected_qty}, got {len(unique_scans)} unique scans.")
            for sid in unique_scans:
                for (job, lot), df in st.session_state.pulltag_editor_df.items():
                    if item_code in df["item_code"].values:
                        st.session_state.scan_buffer.append((job, lot, item_code, sid))
                        break
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
    pdf.cell(270, 6, f"Verified by: {user}   |   Date: {ts}", ln=True, align="C")
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
        "scan_buffer": [],
        "user": st.experimental_user.get("username", "unknown"),
        "confirm_kitting": False,
        "locked": False,
    }
    for k, v in base.items():
        st.session_state.setdefault(k, v)

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
    if "kitted_qty" not in df.columns:
        df["kitted_qty"] = df["qty_req"]
    df["note"].fillna("", inplace=True)
    df["warehouse"].fillna("MAIN", inplace=True)
    logger.info(f"Loaded pulltags for {job}-{lot}: {df[['item_code', 'kitted_qty', 'qty_req', 'transaction_type', 'scan_required']].to_dict()}")
    return df

def finalise():
    for df in st.session_state.pulltag_editor_df.values():
        bad = df[(df["transaction_type"] != "Return") & (df["kitted_qty"] < 0)]
        if not bad.empty:
            st.error("Negative qty only allowed on Return lines.")
            return
    summaries, tx, scans, inv, upd, dels, note_upd, qty_upd = [], [], [], [], [], [], []
    sb = st.session_state.scan_buffer
    missing_notes = []
    for (job, lot), df in st.session_state.pulltag_editor_df.items():
        for _, row in df.iterrows():
            if row["kitted_qty"] != row["qty_req"] and not row["note"]:
                missing_notes.append(f"{job}-{lot}-{row['item_code']}")
    if missing_notes:
        logger.warning(f"Blocked finalization due to missing notes: {missing_notes}")
        st.warning(f"📝 Notes required for items with changed quantity: {', '.join(missing_notes)}")
        return
    try:
        with get_db_cursor() as cur, cur.connection:
            for (job, lot), df in st.session_state.pulltag_editor_df.items():
                buf = {(j, l, ic): [] for j, l, ic, _ in sb}
                for j, l, ic, sid in sb:
                    buf[(j, l, ic)].append(sid)
                for _, r in df.iterrows():
                    ic, qty, tx_type = r["item_code"], int(r["kitted_qty"]), r["transaction_type"]
                    wh, loc = r["warehouse"], st.session_state.location
                    if qty == 0:
                        note_upd.append((r["note"], job, lot, ic))
                        qty_upd.append((qty, job, lot, ic))
                        upd.append(("adjusted", job, lot, ic))
                        continue
                    sc = buf.get((job, lot, ic), [])
                    if r["scan_required"] and len(set(sc)) != abs(qty):
                        raise ScanMismatchError(f"{job}-{lot}-{ic}: need {abs(qty)} scans, got {len(set(sc))}.")
                    qty_abs = abs(qty)
                    if tx_type == "Job Issue":
                        tx.append((tx_type, wh, loc, job, lot, ic, qty, st.session_state.user))
                        inv.append((ic, loc, -qty_abs, wh))
                    else:
                        tx.append((tx_type, wh, loc, job, lot, ic, qty, st.session_state.user))
                        inv.append((ic, loc, qty_abs, wh))
                    for sid in sc:
                        validate_scan_location(cur, sid, tx_type, expected_location=loc, expected_item_code=ic)
                        scans.append((ic, sid, job, lot, loc, tx_type, wh, st.session_state.user))
                        summaries.append({
                            "job_number": job, "lot_number": lot, "item_code": ic,
                            "item_description": r.get("description", ""), "scan_id": sid, "qty": 1
                        })
                    if not sc:
                        summaries.append({
                            "job_number": job, "lot_number": lot, "item_code": ic,
                            "item_description": r.get("description", ""), "scan_id": None, "qty": qty_abs
                        })
                    upd.append(("kitted" if tx_type == "Job Issue" else "returned", job, lot, ic))
                    if r["note"]:
                        note_upd.append((r["note"], job, lot, ic))
                        qty_upd.append((qty, job, lot, ic))
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
            if qty_upd:
                cur.executemany("""UPDATE pulltags SET quantity = %s, last_updated = NOW() WHERE job_number = %s AND lot_number = %s AND item_code = %s""", qty_upd)
            if dels:
                cur.executemany("""DELETE FROM pulltags WHERE job_number=%s AND lot_number=%s AND item_code=%s""", dels)
            cur.connection.commit()
    except Exception as e:
        st.error("❌ Finalization failed. Please check your scan counts, lot state, or try again. Error logged.")
        logger.exception("Finalisation failed")
        return
    pdf = generate_finalize_summary_pdf(summaries, st.session_state.user,
                                        datetime.now(get_timezone()).strftime("%Y-%m-%d %H:%M"))
    st.download_button("📄 Download Final Scan Summary", pdf, file_name="final_scan_summary.pdf", mime="application/pdf")
    finalized_lots = list(st.session_state.pulltag_editor_df.keys())
    logger.info(f"Finalized and archived: {finalized_lots}")
    st.session_state.scan_buffer.clear()
    st.session_state.pulltag_editor_df.clear()
    st.session_state.locked = False
    st.success("✅ Finalization complete. All pulltags archived from editor.")

def run():
    bootstrap_state()
    st.title("📦 Multi-Lot Job Kitting")
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
    loc_input = st.text_input("Staging Location", value=st.session_state.location or "")
    st.session_state.location = loc_input
    if loc_input:
        with get_db_cursor() as cur:
            cur.execute("SELECT 1 FROM locations WHERE location_code = %s", (loc_input,))
            if not cur.fetchone():
                st.warning(f"⚠️ Location '{loc_input}' not found in system. Please verify or add it first.")
    lock_btn_text = "🔓 Unlock Quantities" if st.session_state.locked else "✔️ Lock Quantities"
    with st.form("lock_quantities_form"):
        submitted = st.form_submit_button(lock_btn_text)
        if submitted:
            st.session_state.locked = not st.session_state.locked
            if not st.session_state.locked:
                st.session_state.scan_buffer.clear()
                st.info("Quantities unlocked. Scan buffer cleared.")
                logger.info(f"Unlocked quantities. pulltag_editor_df: {[(k, df[['item_code', 'kitted_qty']].to_dict()) for k, df in st.session_state.pulltag_editor_df.items()]}")
            else:
                if submitted:
                    sync_editor_edits()
                    compute_scan_requirements()
                    for (job, lot), df in st.session_state.pulltag_editor_df.items():
                        logger.info(f"[FORM SUBMIT] Applied for {job}-{lot} → {df[['item_code', 'kitted_qty']].to_dict()}")
                    

                st.success("Quantities locked. Scanning enabled.")
                logger.info(f"Locked quantities. pulltag_editor_df: {[(k, df[['item_code', 'kitted_qty']].to_dict()) for k, df in st.session_state.pulltag_editor_df.items()]}")
    session_label_default = f"{st.session_state.user} – Kit @ {datetime.now().strftime('%H:%M')}"
    session_label = st.text_input("📝 Session Label (optional)", value=session_label_default)
    compute_scan_requirements()
    if st.button("📂 Save Progress"):
        sync_editor_edits()  # Ensure latest edits are saved
        snapshot = {
            "pulltag_editor_df": {f"{k[0]}|{k[1]}": df.to_dict() for k, df in st.session_state.pulltag_editor_df.items()},
            "scan_buffer": st.session_state.scan_buffer,
            "locked": st.session_state.locked,
        }
        logger.info(f"Saving session with pulltag_editor_df: {snapshot['pulltag_editor_df'].keys()}")
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO kitting_sessions (session_id, user_id, data, label, expires_at)
                VALUES (%s, %s, %s, %s, NOW() + INTERVAL '48 hours')
                ON CONFLICT (session_id) DO UPDATE
                SET data = EXCLUDED.data, label = EXCLUDED.label, saved_at = NOW(), expires_at = EXCLUDED.expires_at
            """, (
                st.session_state.session_id,
                st.session_state.user,
                json.dumps(snapshot),
                session_label
            ))
        st.success("📂 Progress saved to database.")
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT session_id, label, saved_at
            FROM kitting_sessions
            WHERE user_id = %s AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY saved_at DESC
            LIMIT 10
        """, (st.session_state.user,))
        sessions = cur.fetchall()
    if sessions:
        session_options = {f"{label} ({saved_at[:16]})": sid for sid, label, saved_at in sessions}
        selected = st.selectbox("📂 Resume or Delete a Saved Session", options=list(session_options.keys()))
        col1, col2 = st.columns([1, 1])
        with col1:
            if selected and st.button("🔁 Load Selected Session"):
                sid = session_options[selected]
                cur.execute("SELECT data FROM kitting_sessions WHERE session_id = %s", (sid,))
                row = cur.fetchone()
                if row:
                    saved = json.loads(row[0])
                    st.session_state.pulltag_editor_df = {
                        tuple(k.split("|")): pd.DataFrame(v)
                        for k, v in saved["pulltag_editor_df"].items()
                    }
                    st.session_state.scan_buffer = saved["scan_buffer"]
                    st.session_state.locked = saved["locked"]
                    st.success(f"Session '{selected}' restored.")
                    logger.info(f"Restored session: pulltag_editor_df: {[(k, df[['item_code', 'kitted_qty']].to_dict()) for k, df in st.session_state.pulltag_editor_df.items()]}")
        with col2:
            if selected and st.button("🗑️ Delete This Session"):
                sid = session_options[selected]
                cur.execute("DELETE FROM kitting_sessions WHERE session_id = %s", (sid,))
                st.success(f"Session '{selected}' deleted.")
                st.experimental_rerun()
    if st.session_state.locked:
        with st.expander("🔍 Scan Input"):
            render_scan_inputs()
            if st.session_state.scan_buffer:
                st.markdown("### 📋 Scan Buffer")
                st.table(pd.DataFrame(st.session_state.scan_buffer, columns=["Job", "Lot", "Item", "Scan ID"]))
                if st.button("🧹 Clear Scan Buffer"):
                    st.session_state.scan_buffer.clear()
                    st.success("Scan buffer cleared.")
    for (job, lot), df in list(st.session_state.pulltag_editor_df.items()):
        st.markdown(f"### 🛠 Editing Pull‑Tags for `{job}-{lot}`")
        col1, col2 = st.columns([6, 1])
        with col1:
            form_key = f"{EDIT_ANCHOR}_form_{job}_{lot}"
            with st.form(form_key):
                editor_key = f"{EDIT_ANCHOR}_{job}_{lot}"
                sync_editor_edits()  # Sync before rendering to ensure latest changes
                edited_df = st.data_editor(
                    df[["item_code", "description", "qty_req", "kitted_qty", "note"]],
                    key=editor_key,
                    num_rows="dynamic",
                    use_container_width=True,
                    disabled=st.session_state.locked,
                    column_config={
                        "item_code": st.column_config.TextColumn("Item Code", disabled=True),
                        "description": st.column_config.TextColumn("Description", disabled=True),
                        "qty_req": st.column_config.NumberColumn("Qty Required", disabled=True),
                        "kitted_qty": st.column_config.NumberColumn("Kitted Qty"),
                        "note": st.column_config.TextColumn("Notes"),
                    }
                )
                submitted = st.form_submit_button("📂 Apply Changes")
                if submitted:
                    sync_editor_edits()
                    compute_scan_requirements()
                    st.success(f"Changes for `{job}-{lot}` saved.")
                    logger.info(f"Applied changes for {job}-{lot}: {df[['item_code', 'kitted_qty']].to_dict()}")
        with col2:
            if st.button(f"❌ Remove `{job}-{lot}`", key=f"remove_{job}_{lot}"):
                del st.session_state.pulltag_editor_df[(job, lot)]
    if not st.session_state.locked:
        st.warning("🔒 Lock quantities before finalizing.")
    else:
        if st.button("✅ Finalize Kitting"):
            finalise()
