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
        /* ─── hide the built-in multipage nav ───────────────────────── */
        [data-testid="stSidebarNav"] {{ display: none !important; }}

        /* ─── translucent sidebar & transparent toolbar ───────────────── */
        [data-testid="stSidebar"] {{ background-color: rgba(0,0,0,0.2) !important; }}
        [data-testid="stToolbar"] {{ background-color: transparent !important; box-shadow: none !important; }}

        /* ─── clear out any white container backgrounds ─────────────────── */
        html, body,
        [data-testid="stAppViewContainer"],
        .block-container {{
          background-color: transparent !important;
        }}

        /* ─── your logo as a full-screen DIV (z-index 0!) ─────────────── */
        .bg-div {{
          position: fixed;
          top: 0; left: 0;
          width: 100vw; height: 100vh;
          background: url("data:image/png;base64,{_LOGO_BASE64}") 
                      no-repeat center top fixed !important;
          background-size: cover !important;
          z-index: 0 !important;
        }}

        /* ─── gold/orange text for title & metrics ───────────────────── */
        h1, h2, h3, p,
        [data-testid="stMetricValue"],
        [data-testid="stMetricLabel"] {{
          color: #F6A623 !important;
        }}
        </style>

        <!-- DIV behind everything -->
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
