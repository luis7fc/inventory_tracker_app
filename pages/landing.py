# pages/landing.py

import streamlit as st
import base64
from db import get_db_cursor
from datetime import date

def _get_base64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

# load your logo once
_LOGO_BASE64 = _get_base64("assets/logo.png")

def run():

    st.markdown(
        f"""
        <style>
        /* ── 1) Hide built-in multipage nav ─────────────────────── */
        [data-testid="stSidebarNav"] {{ display: none !important; }}

        /* ── 2) Full-screen hero DIV with your logo (no tint) ───── */
        .bg-div {{
          position: fixed;
          top: 0; left: 0;
          width: 100vw; height: 100vh;
          background: url("data:image/png;base64,{_LOGO_BASE64}") 
                      no-repeat top center fixed !important;
          background-size: contain !important;
          z-index: -1;
        }}

        /* ── 3) Gold/orange text for title & metrics ───────────── */
        h1, h2, h3, p,
        [data-testid="stMetricValue"],
        [data-testid="stMetricLabel"] {{
          color: #F6A623 !important;
        }}
        </style>

        <!-- background DIV must come *after* CSS -->
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
