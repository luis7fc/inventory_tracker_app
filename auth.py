#Authorization System Set Up
import bcrypt
import streamlit as st
import psycopg2

# --- DB CONNECTION ---
def get_db_connection():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"]
    )

# --- AUTHENTICATION ---
def verify_user_credentials(username: str, password: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT password, role FROM users WHERE username = %s", (username,))
    result = cursor.fetchone()
    conn.close()

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
