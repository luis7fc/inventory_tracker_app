# pages/landing.py

import streamlit as st
from db import get_db_cursor
from datetime import date


def run():

    st.markdown(
        """
        <style>
        /* 1) set the page‐body background */
        body {
          background: url("/assets/logo.png") no-repeat center center fixed;
          background-size: cover;
        }

        /* 2) make the outer Streamlit app container transparent */
        [data-testid="stAppViewContainer"] {
          background-color: transparent !important;
        }

        /* 3) make the inner block‐container (where widgets live) transparent */
        .css-18e3th9, /* generic “block-container” classname */
        .block-container {
          background-color: transparent !important;
        }

        /* 4) hide the built-in pages menu */
        [data-testid="stSidebarNav"] {
          display: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    # 2) Sidebar logo (optional—you can remove this now if you just want wallpaper)
    st.sidebar.image("assets/logo.png", use_container_width=True)

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
