# pages/landing.py

import streamlit as st
from db import get_db_cursor
from datetime import date


def run():
    # Display company logo in sidebar (place logo.png under assets/)
    st.sidebar.image("assets/logo.png", use_container_width=True)

    st.markdown(
        f"""
    <style>
    [data-testid+"stAppViewContainer"]{{
        background-image: url("assets/logo.png");
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
    }}
    <style>
    """,
        unsafe_allow_html=True,
    )
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
