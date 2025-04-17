import streamlit as st
import pandas as pd
from db import get_all_users, create_user, delete_user
import bcrypt

def run():
    if st.session_state.role != "admin":
        st.warning("Admin access required.")
        st.stop()

    st.header("👤 Registered Users")

    # Load current users
    users = get_all_users()
    users_df = pd.DataFrame(users, columns=["id", "username", "role"])
    st.dataframe(users_df)

    # --- Add New User ---
    st.subheader("➕ Add New User")
    new_username = st.text_input("New Username", key="new_username")
    new_password = st.text_input("New Password", type="password", key="new_password")
    new_role = st.selectbox("Role", ["user", "admin"], key="new_user_role")

    if st.button("Create User"):
        if not new_username or not new_password:
            st.error("Username and password are required.")
        else:
            # Check if user exists
            if any(u[1] == new_username for u in users):
                st.error("Username already exists.")
            else:
                create_user(new_username, new_password, new_role)
                st.success(f"User '{new_username}' created.")
                st.rerun()

    # --- Delete User ---
    st.subheader("🗑️ Delete User")
    delete_user_display = {f"{u[1]} ({u[2]})": u[0] for u in users}
    delete_user_label = st.selectbox("Select a user to delete", list(delete_user_display.keys()), key="delete_user_select")

    if st.button("Delete User"):
        selected_id = delete_user_display[delete_user_label]
        selected_username = delete_user_label.split(" ")[0]
        if selected_username == st.session_state.user:
            st.error("You can't delete your own account while logged in.")
        else:
            delete_user(selected_id)
            st.success(f"User '{selected_username}' deleted.")
            st.rerun()

    # --- Reset Password ---
    st.subheader("🔁 Reset User Password")
    reset_user_display = {f"{u[1]} ({u[2]})": u[0] for u in users}
    reset_user_label = st.selectbox("Select a user to reset password", list(reset_user_display.keys()), key="reset_user_select")
    new_pw_for_user = st.text_input("New Password for Selected User", type="password", key="reset_user_pw")

    if st.button("Reset Password"):
        if not new_pw_for_user:
            st.error("Password cannot be empty.")
        else:
            user_id = reset_user_display[reset_user_label]
            hashed_pw = bcrypt.hashpw(new_pw_for_user.encode(), bcrypt.gensalt()).decode()
            from db import update_user_password  # Assuming this exists
            update_user_password(user_id, hashed_pw)
            st.success(f"Password reset for user '{reset_user_label.split(' ')[0]}'.")
            st.rerun()
