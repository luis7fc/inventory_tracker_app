# pages/landing.py

import streamlit as st
import base64
from db import get_db_cursor
from datetime import date

def _get_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

_LOGO_BASE64 = _get_base64("assets/logo.png")

def run():

    # 1) full-screen background DIV with embedded Base64 PNG
    st.markdown(
        f"""
        <style>
          .bg-div {{
            position: fixed;
            top: 0; left: 0;
            width: 100vw; height: 100vh;
            background: url("data:image/png;base64,{_LOGO_BASE64}") 
                        no-repeat center center fixed;
            background-size: cover;
            z-index: -1;
          }}
          /* hide the built-in pages nav */
          [data-testid="stSidebarNav"] {{ display: none; }}
        </style>
        <div class="bg-div"></div>
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
