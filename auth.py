# auth.py

import bcrypt
import streamlit as st
from db import get_db_cursor

# --- AUTHENTICATION ---
def verify_user_credentials(username: str, password: str):
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT password, role FROM users WHERE username = %s",
            (username,)
        )
        result = cursor.fetchone()

    if result:
        stored_hash, role = result
        if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
            return True, role
    return False, None

# --- LOGIN LOGIC ---
def login():
        st.markdown(
            f"""
            <style>
            /* ── hide Streamlit’s built-in sidebar nav ─────────────────── */
            [data-testid="stSidebarNav"] {{ display: none !important; }}

            /* ── translucent sidebar & clear toolbars ─────────────────── */
            [data-testid="stSidebar"]  {{ background: rgba(0,0,0,0.2) !important; }}
            [data-testid="stToolbar"],
            [data-testid="stHeader"]  {{ background: transparent !important; box-shadow:none !important; }}

            /* ── remove default white containers ──────────────────────── */
            html, body,
            [data-testid="stAppViewContainer"],
            .block-container {{
                background: transparent !important;
            }}

            /* ── full-viewport background div  ─────────────────────────── */
            .bg-div {{
                position: fixed; inset: 0;
                background: url("data:image/png;base64,{b64}") no-repeat center top fixed !important;
                background-size: cover !important;
                z-index: 0 !important;
            }}

            /* ── optional gold text theme  ─────────────────────────────── */
            h1, h2, h3, p, [data-testid="stMetricValue"], [data-testid="stMetricLabel"] {{
                color: #F6A629 !important;
            }}
            </style>

            <div class="bg-div"></div>
            """,
            unsafe_allow_html=True,
        )
    if "user" not in st.session_state:
        st.session_state.user = None
        st.session_state.role = None

    if not st.session_state.user:
        st.title("🔐 Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            is_valid, role = verify_user_credentials(username, password)
            if is_valid:
                st.session_state.user = username
                st.session_state.role = role
                st.success(f"Logged in as {username} ({role})")
                st.rerun()
            else:
                st.error("Invalid credentials.")
        st.stop()
    else:
        st.sidebar.success(f"Logged in as {st.session_state.user} ({st.session_state.role})")
