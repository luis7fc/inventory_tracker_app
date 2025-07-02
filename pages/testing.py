# Tab 1 Module – Opportunity Processor
# This file exposes a `run()` function that Streamlit can import and render inside a tab.

import streamlit as st
import pandas as pd
import io
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from db import get_db_cursor
import math

__all__ = ["run"]  # for easy import *

# --- WIRE ROUNDING UNITS ----------------------------------------------------
ROUNDING_UNITS = {
    "10RED": 500, "10BLK": 500, "10WHT": 500, "10GRN": 500,
    "8RED": 500, "8WHT": 500, "8BLK": 500, "8GRN": 500,
    "6RED": 500, "6WHT": 500, "6BLK": 500, "6GRN": 500,
    "4BLK": 100, "4RED": 100, "4GRN": 100, "4WHT": 100,
    "ROM143": 250, "ROM103": 250, "ROM83": 125,
    "ROM63": 125, "ROM43": 100, "184CSHLD": 500, "CAT5S": 500,
    "SF1HEM": 1, "SF1DSA": 1, "SF1BASE": 1,
    "QFP100": 1, "34EMT": 10, "NAILPLT": 1
}

# ---------------------------------------------------------------------------
# Utilities – PDF, Sage TXT, and rounding logic
# ---------------------------------------------------------------------------

def generate_pdf(activities_dict: dict, total_materials: dict) -> io.BytesIO:
    """Return in-memory PDF summarising the activities and totals."""
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    pdf.setFont("Helvetica", 10)

    # Page 1 – activities per lot
    y = 750
    pdf.drawString(100, 780, "Activities Dictionary Report")
    pdf.line(100, 775, 500, 775)
    for key, mats in activities_dict.items():
        pdf.drawString(100, y, f"{key}:")
        y -= 15
        for m, q in mats.items():
            pdf.drawString(120, y, f"- {m}: {q}")
            y -= 15
            if y < 50:
                pdf.showPage(); pdf.setFont("Helvetica", 10); y = 750
        y -= 10

    # Page 2 – totals
    pdf.showPage(); y = 750
    pdf.setFont("Helvetica", 10)
    pdf.drawString(100, 780, "Total Material Allocation (Rounded)")
    pdf.line(100, 775, 500, 775)
    for m, q in total_materials.items():
        pdf.drawString(120, y, f"- {m}: {q}")
        y -= 15
        if y < 50:
            pdf.showPage(); pdf.setFont("Helvetica", 10); y = 750

    pdf.save(); buffer.seek(0)
    return buffer


def generate_sage_txt(activities_dict: dict, item_data: dict) -> str:
    """Return Sage-formatted TXT string for import."""
    today = datetime.today().strftime("%m-%d-%y")
    lines = [
        f"I,Test Import,{today},{today},,\"\",,,,,,,,",
        ";line ID,location,item code,quantity,unit of measure,\"description\",conversion factor,equipment id,equipment cost code,job,lot,cost code,category,requisition number,issue date",
    ]

    for key, mats in activities_dict.items():
        try:
            lot, job, jobno = key.split(" - ")
        except ValueError:
            continue  # malformed key; skip

        for code, qty in mats.items():
            meta = item_data.get(code.upper(), {
                "description": "Unknown", "job_cost_code": "BOS", "unit_of_measure": "EA"
            })
            row = [
                "IL", "FNOSolar", code.upper(), str(qty), meta["unit_of_measure"],
                f'"{meta["description"]}"', "1", "", "", jobno, lot,
                meta["job_cost_code"], "M", "", today,
            ]
            lines.append(",".join(row))
    return "\n".join(lines)


def round_and_distribute(actdict: dict) -> tuple[dict, dict]:
    """Round totals up to spool sizes & evenly distribute difference across lots."""
    # Sum totals
    total: dict[str, int] = {}
    for mats in actdict.values():
        for m, q in mats.items():
            total[m] = total.get(m, 0) + q

    # Round
    rounded: dict[str, int] = {}
    for mat, qty in total.items():
        unit = ROUNDING_UNITS.get(mat.upper())
        if unit:
            rounded[mat] = int(math.ceil(qty / unit) * unit)

    # Sprinkle delta
    for mat, new_total in rounded.items():
        diff = new_total - total.get(mat, 0)
        if diff == 0:
            continue
        targets = [k for k in actdict if mat in actdict[k]]
        if not targets:
            continue  # safety
        inc, rem = divmod(diff, len(targets))
        for i, k in enumerate(targets):
            actdict[k][mat] += inc + (1 if i < rem else 0)

    return actdict, rounded

# ---------------------------------------------------------------------------
# Public entry point – call from main app.py
# ---------------------------------------------------------------------------

def run():
    """Render the Upload / Generate tab UI."""
    st.subheader("📂 Upload Job Excel to Generate Outputs")
    uploaded = st.file_uploader("Upload Excel File", type="xlsx")

    if not uploaded:
        st.info("Upload a job file to begin.")
        return

    # -------------------------------------------------------------------
    # Load user spreadsheet
    # -------------------------------------------------------------------
    df = pd.read_excel(uploaded, engine="openpyxl")
    df.columns = df.columns.str.strip().str.lower()

    required_cols = {"lot #", "job name", "job number"}
    if not required_cols.issubset(df.columns):
        st.error(f"Missing columns: {required_cols - set(df.columns)}")
        return

    # -------------------------------------------------------------------
    # Fetch DB data
    # -------------------------------------------------------------------
    with get_db_cursor() as cur:
        cur.execute("SELECT * FROM opportunities")
        opp_df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

        cur.execute("SELECT item_code, item_description, cost_code, uom FROM items_master")
        item_rows = cur.fetchall()
        item_data = {
            r[0].upper(): {
                "description": r[1],
                "job_cost_code": r[2],
                "unit_of_measure": r[3],
            } for r in item_rows
        }

    opp_df.columns = opp_df.columns.str.lower()

    # -------------------------------------------------------------------
    # Build activities dictionary keyed by "lot - job - jobno"
    # -------------------------------------------------------------------
    activities: dict[str, dict[str, int]] = {}
    for _, row in df.iterrows():
        lot  = str(row["lot #"]).strip()
        job  = str(row["job name"]).strip().lower()
        jobno = str(row["job number"]).strip()
        key = f"{lot} - {job} - {jobno}"

        match = opp_df[opp_df["job_name"] == job]
        if match.empty:
            continue  # skip jobs not in opportunities

        mats = match.iloc[0].drop(["id", "job_name", "account"]).to_dict()
        mats = {k.upper(): int(v) for k, v in mats.items() if int(v) > 0}
        activities[key] = mats

    if not activities:
        st.warning("No matching opportunities found for the uploaded file.")
        return

    # -------------------------------------------------------------------
    # Apply rounding + sprinkling
    # -------------------------------------------------------------------
    activities, total_rounded = round_and_distribute(activities)

    # -------------------------------------------------------------------
    # UI output
    # -------------------------------------------------------------------
    st.subheader("📋 Activities Dictionary")
    st.json(activities)
    st.subheader("🔢 Total Materials (Rounded)")
    st.json(total_rounded)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("📄 Download PDF"):
            pdf_buf = generate_pdf(activities, total_rounded)
            st.download_button("Download PDF", data=pdf_buf, file_name="activities_report.pdf")

    with col2:
        if st.button("📥 Download TXT"):
            txt_str = generate_sage_txt(activities, item_data)
            st.download_button("Download TXT", data=txt_str, file_name="sage_output.txt")
