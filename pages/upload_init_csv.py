import streamlit as st
import pandas as pd
from db import get_db_cursor

def run():
    st.header("📥 Upload Inventory Init CSV")
    file = st.file_uploader("Upload CSV", type="csv")

    if file:
        try:
            df = pd.read_csv(file)
            st.subheader("📋 Parsed CSV Preview")
            st.dataframe(df)

            preview_data = []
            scan_verification_preview = []

            for _, row in df.iterrows():
                item_code = row['item_code'].strip()
                location  = row['location'].strip().upper()
                warehouse = row.get('warehouse', '').strip().upper()
                quantity  = int(row['quantity'])
                scan_id   = row.get('scan_id')

                if not warehouse:
                    raise ValueError(f"Missing warehouse for location {location}.")

                preview_data.append({
                    "item_code": item_code,
                    "location": location,
                    "warehouse": warehouse,
                    "quantity": quantity,
                    "scan_id": scan_id
                })

                if pd.notna(scan_id) and str(scan_id).strip():
                    scan_verification_preview.append({
                        "scan_id": str(scan_id).strip(),
                        "item_code": item_code,
                        "location": location,
                        "warehouse": warehouse,
                        "transaction_type": "Init",
                        "scanned_by": st.session_state.get("user", "unknown")
                    })

            # Show scan_verifications that would be added
            if scan_verification_preview:
                st.subheader("🔍 Scan Verifications to Be Logged")
                st.dataframe(pd.DataFrame(scan_verification_preview))
            else:
                st.info("No valid scan_id entries found to log in scan_verifications.")

            if st.button("✅ Commit to DB"):
                with get_db_cursor() as cursor:
                    for entry in preview_data:
                        item_code = entry['item_code']
                        location = entry['location']
                        warehouse = entry['warehouse']
                        quantity = entry['quantity']
                        scan_id = entry.get('scan_id')

                        # Ensure location exists
                        cursor.execute(
                            """
                            INSERT INTO locations (location_code, warehouse)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (location, warehouse)
                        )

                        # Log original init
                        cursor.execute(
                            """
                            INSERT INTO inventory_init (item_code, location, quantity, scan_id)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (item_code, location, quantity, scan_id)
                        )

                        if pd.notna(scan_id) and str(scan_id).strip():
                            scan_id_clean = str(scan_id).strip()

                            # Update current_scan_location
                            cursor.execute(
                                """
                                INSERT INTO current_scan_location (scan_id, item_code, location)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (scan_id) DO UPDATE
                                SET item_code = EXCLUDED.item_code,
                                    location = EXCLUDED.location,
                                    updated_at = NOW()
                                """,
                                (scan_id_clean, item_code, location)
                            )

                            # Log to scan_verifications
                            scanned_by = st.session_state.get("user", "unknown")
                            cursor.execute(
                                """
                                INSERT INTO scan_verifications
                                  (scan_id, item_code, job_number, lot_number,
                                   scan_time, location, transaction_type, warehouse,
                                   pulltag_id, scanned_by)
                                VALUES (%s, %s, NULL, NULL, NOW(), %s, 'Init', %s, NULL, %s)
                                """,
                                (scan_id_clean, item_code, location, warehouse, scanned_by)
                            )

                        # Upsert into current_inventory
                        cursor.execute(
                            """
                            INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (item_code, location, warehouse)
                            DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity
                            """,
                            (item_code, location, quantity, warehouse)
                        )

                st.success("🎉 Inventory and scan data successfully committed to the database.")

        except Exception as e:
            st.error(f"❌ Error: {e}")
