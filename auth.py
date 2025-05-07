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
