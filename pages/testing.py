import streamlit as st
import pandas as pd
import io
import math
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from db import get_db_cursor

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
ROUNDING_UNITS = {
    "10RED": 500, "10BLK": 500, "10WHT": 500, "10GRN": 500,
    "8RED": 500, "8WHT": 500, "8BLK": 500, "8GRN": 500,
    "6RED": 500, "6WHT": 500, "6BLK": 500, "6GRN": 500,
    "4BLK": 100, "4RED": 100, "4GRN": 100, "4WHT": 100,
    "ROM143": 250, "ROM103": 250, "ROM83": 125,
    "ROM63": 125, "ROM43": 100, "184CSHLD": 500, "CAT5S": 500,
    "SF1HEM": 1,   "SF1DSA": 1,   "SF1BASE": 1,
    "QFP100": 1,  "34EMT": 10,   "NAILPLT": 1
}

# ─────────────────────────────────────────────
# HELPERS – PDF, TXT GENERATION & DISTRIBUTION
# ─────────────────────────────────────────────
def generate_pdf(activities, totals):
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    pdf.setFont("Helvetica", 10)

    # Activities per lot
    y = 750
    pdf.drawString(100, 780, "Activities Dictionary Report")
    pdf.line(100, 775, 500, 775)
    for key, mats in activities.items():
        pdf.drawString(100, y, f"{key}:")
        y -= 15
        for m, q in mats.items():
            pdf.drawString(120, y, f"- {m}: {q}")
            y -= 15
            if y < 50:
                pdf.showPage()
                pdf.setFont("Helvetica", 10)
                y = 750
        y -= 10

    # Totals (rounded)
    pdf.showPage()
    y = 750
    pdf.setFont("Helvetica", 10)
    pdf.drawString(100, 780, "Total Material Allocation (Rounded)")
    pdf.line(100, 775, 500, 775)
    for m, q in totals.items():
        pdf.drawString(120, y, f"- {m}: {q}")
        y -= 15
        if y < 50:
            pdf.showPage()
            pdf.setFont("Helvetica", 10)
            y = 750

    pdf.save()
    buffer.seek(0)
    return buffer


def generate_sage_txt(activities, item_data):
    today = datetime.today().strftime("%m-%d-%y")
    lines = [
        f"I,Test Import,{today},{today},,""",,,,,,,,",  # Sage import header
        ";line ID,location,item code,quantity,unit of measure,\"description\",conversion factor,equipment id,equipment cost code,job,lot,cost code,category,requisition number,issue date"
    ]

    for key, mats in activities.items():
        try:
            lot, job, jobno = key.split(" - ")
        except ValueError:
            continue
        for code, qty in mats.items():
            meta = item_data.get(code.upper(), {
                "description": "Unknown", "job_cost_code": "BOS", "unit_of_measure": "EA"
            })
            row = [
                "IL", "FNOSolar", code.upper(), str(qty), meta["unit_of_measure"],
                f'"{meta["description"]}"', "1", "", "", jobno, lot,
                meta["job_cost_code"], "M", "", today
            ]
            lines.append(",".join(row))

    return "\n".join(lines)


def round_and_distribute(activities):
    # sum current totals
    totals = {}
    for mats in activities.values():
        for m, q in mats.items():
            totals[m] = totals.get(m, 0) + q

    # round to spool sizes
    rounded = {}
    for m, q in totals.items():
        unit = ROUNDING_UNITS.get(m)
        if unit:
            rounded[m] = math.ceil(q / unit) * unit

    # distribute delta evenly
    for m, new_q in rounded.items():
        delta = new_q - totals.get(m, 0)
        if delta == 0:
            continue
        lots = [k for k in activities if m in activities[k]]
        inc, rem = divmod(delta, len(lots))
        for idx, key in enumerate(lots):
            activities[key][m] += inc + (1 if idx < rem else 0)

    return activities, rounded

# ─────────────────────────────────────────────
# MAIN UI – ENTRY POINT
# ─────────────────────────────────────────────
def run():
    st.subheader("📂 Upload Job Excel to Generate Outputs")
    uploaded = st.file_uploader("Upload Excel File", type="xlsx")
    if not uploaded:
        st.info("Upload a job file to begin.")
        return

    df = pd.read_excel(uploaded, engine="openpyxl")
    df.columns = df.columns.str.strip().str.lower()
    required = {"lot #", "job name", "job number"}
    if not required.issubset(df.columns):
        st.error(f"Missing columns: {required - set(df.columns)}")
        return

    # fetch opportunities & items
    with get_db_cursor() as cur:
        cur.execute("SELECT * FROM opportunities")
        opp_df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
        cur.execute("SELECT item_code, item_description, cost_code, uom FROM items_master")
        rows = cur.fetchall()
        item_data = {r[0].upper(): {"description": r[1], "job_cost_code": r[2], "unit_of_measure": r[3]} for r in rows}

    opp_df.columns = opp_df.columns.str.lower()

    # build activities
    activities = {}
    for _, row in df.iterrows():
        lot  = str(row["lot #"]).strip()
        job  = str(row["job name"]).strip().lower()
        jobno = str(row["job number"]).strip()
        key = f"{lot} - {job} - {jobno}"
        match = opp_df[opp_df["job_name"] == job]
        if match.empty:
            continue
        mats = match.iloc[0].drop(["id", "job_name", "account"]).to_dict()
        activities[key] = {k.upper(): int(v) for k, v in mats.items() if int(v) > 0}

    if not activities:
        st.warning("No matching opportunities found.")
        return

    activities, totals = round_and_distribute(activities)

    st.subheader("📋 Activities Dictionary")
    st.json(activities)
    st.subheader("🔢 Total Materials (Rounded)")
    st.json(totals)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("📄 Download PDF"):
            pdf_buf = generate_pdf(activities, totals)
            st.download_button("Download PDF", data=pdf_buf, file_name="activities_report.pdf")
    with c2:
        if st.button("📥 Download TXT"):
            txt = generate_sage_txt(activities, item_data)
            st.download_button("Download TXT", data=txt, file_name="sage_output.txt")
