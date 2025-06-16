import streamlit as st
import pandas as pd
from db import get_db_cursor

REQUIRED_COLUMNS = {"item_code", "location", "quantity"}

def run():
    st.header("📥 Upload Inventory Init CSV")
    file = st.file_uploader("Upload CSV", type="csv")

    if file:
        try:
            df = pd.read_csv(file)

            # ── Validate headers ─────────────────────────────────
            missing_cols = REQUIRED_COLUMNS - set(df.columns)
            if missing_cols:
                st.error(f"❌ CSV is missing required columns: {', '.join(missing_cols)}")
                return

            # ── Strip and uppercase where needed ────────────────
            df['item_code'] = df['item_code'].astype(str).str.strip()
            df['location'] = df['location'].astype(str).str.strip().str.upper()
            if 'warehouse' in df.columns:
                df['warehouse'] = df['warehouse'].astype(str).str.strip().str.upper()
            else:
                df['warehouse'] = ""

            # ── Connect and fetch validations ───────────────────
            with get_db_cursor() as cursor:
                cursor.execute("SELECT item_code FROM items_master")
                valid_item_codes = {row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT scan_id FROM current_scan_location")
                existing_scan_ids = {str(row[0]).strip() for row in cursor.fetchall()}

            preview_data = []
            scan_verification_preview = []

            for i, row in df.iterrows():
                item_code = row["item_code"]
                location = row["location"]
                warehouse = row["warehouse"]
                scan_id = str(row.get("scan_id", "")).strip()
                
                # ── Validation Checks ─────────────────────────────
                if not item_code or item_code == "":
                    st.error(f"❌ Row {i+2}: Blank item_code")
                    return
                if item_code not in valid_item_codes:
                    st.error(f"❌ Row {i+2}: Invalid item_code: {item_code}")
                    return
                try:
                    quantity = int(row["quantity"])
                except:
                    st.error(f"❌ Row {i+2}: Quantity is not an integer")
                    return
                if quantity < 0:
                    st.error(f"❌ Row {i+2}: Negative quantity not allowed")
                    return
                if scan_id and scan_id in existing_scan_ids:
                    st.error(f"❌ Row {i+2}: Duplicate scan_id found in system: {scan_id}")
                    return
                if not warehouse:
                    st.error(f"❌ Row {i+2}: Missing warehouse")
                    return

                # ── Prepare for preview and commit ───────────────
                preview_data.append({
                    "item_code": item_code,
                    "location": location,
                    "warehouse": warehouse,
                    "quantity": quantity,
                    "scan_id": scan_id
                })

                if scan_id:
                    scan_verification_preview.append({
                        "scan_id": scan_id,
                        "item_code": item_code,
                        "location": location,
                        "warehouse": warehouse,
                        "transaction_type": "Init",
                        "scanned_by": st.session_state.get("user", "unknown")
                    })

            # ── Preview Outputs ────────────────────────────────
            st.subheader("📋 Parsed CSV Preview")
            st.dataframe(pd.DataFrame(preview_data))

            if scan_verification_preview:
                st.subheader("🔍 Scan Verifications to Be Logged")
                st.dataframe(pd.DataFrame(scan_verification_preview))
            else:
                st.info("No valid scan_id entries found to log in scan_verifications.")

            # ── Commit to DB ───────────────────────────────────
            if st.button("✅ Commit to DB"):
                with get_db_cursor() as cursor:
                    for entry in preview_data:
                        item_code = entry['item_code']
                        location = entry['location']
                        warehouse = entry['warehouse']
                        quantity = entry['quantity']
                        scan_id = entry['scan_id']

                        cursor.execute("""
                            INSERT INTO locations (location_code, warehouse)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (location, warehouse))

                        cursor.execute("""
                            INSERT INTO inventory_init (item_code, location, quantity, scan_id)
                            VALUES (%s, %s, %s, %s)
                        """, (item_code, location, quantity, scan_id or None))

                        if scan_id:
                            cursor.execute("""
                                INSERT INTO current_scan_location (scan_id, item_code, location)
                                VALUES (%s, %s, %s)
                            """, (scan_id, item_code, location))

                            scanned_by = st.session_state.get("user", "unknown")
                            cursor.execute("""
                                INSERT INTO scan_verifications (
                                    scan_id, item_code, job_number, lot_number,
                                    scan_time, location, transaction_type,
                                    warehouse, pulltag_id, scanned_by
                                )
                                VALUES (%s, %s, NULL, NULL, NOW(), %s, 'Init', %s, NULL, %s)
                            """, (scan_id, item_code, location, warehouse, scanned_by))

                        cursor.execute("""
                            INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (item_code, location, warehouse)
                            DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity
                        """, (item_code, location, quantity, warehouse))

                        user_id = st.session_state.get("user", "unknown")
                        cursor.execute("""
                            INSERT INTO transactions (
                                transaction_type, item_code, quantity, date,
                                job_number, lot_number, po_number,
                                from_location, to_location, user_id,
                                bypassed_warning, note, warehouse
                            )
                            VALUES (
                                'Init', %s, %s, NOW(),
                                NULL, NULL, NULL,
                                NULL, %s, %s,
                                FALSE, NULL, %s
                            )
                        """, (item_code, quantity, location, user_id, warehouse))

                st.success("🎉 Inventory and scan data successfully committed to the database.")

        except Exception as e:
            st.error(f"❌ Error: {e}")
