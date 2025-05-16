# pages/landing.py

import streamlit as st
import base64
from db import get_db_cursor
from datetime import date

def run():

    #-Welcome message-
    user = st.session_state.get("user","")
    st.title(f"Welcome, {user}!")

    # Fetch metrics from DB
    total, today = _fetch_scan_counts()

    # Display metrics prominently
    col1, col2 = st.columns(2)
    col1.metric(label="Total Transactions", value=total)
    col2.metric(label="Transactions Today", value=today)


def _fetch_scan_counts():
    today = date.today()
    with get_db_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM scan_verifications")
        total = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM scan_verifications WHERE DATE(scan_time) = %s",
            (today,)
        )
        today_count = cur.fetchone()[0]
    return total, today_count
