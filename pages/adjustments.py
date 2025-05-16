import streamlit as st
from datetime import datetime
from db import get_db_cursor, insert_pulltag_line
from config import WAREHOUSES

def insert_pulltag_line(cur, job_number, lot_number, item_code, quantity, transaction_type="Job Issue", warehouse=None, status="kitted"):
    sql = """
    INSERT INTO pulltags
      (job_number, lot_number, item_code, quantity,
       description, cost_code, uom, status, transaction_type, warehouse)
    SELECT
      %s,        -- job_number
      %s,        -- lot_number
      item_code,
      %s,        -- quantity
      item_description,
      cost_code,
      uom,
      %s,        -- status
      %s,        -- transaction_type
      %s         -- warehouse
    FROM items_master
    WHERE item_code = %s
    RETURNING id
    """
    cur.execute(sql, (job_number, lot_number, quantity, status, transaction_type, warehouse, item_code))
    return cur.fetchone()[0]

def finalize_add(scans_needed, scan_inputs, job_lot_queue, from_location, to_location=None, scanned_by=None, progress_callback=None):
    total_scans = sum(qty for lots in scans_needed.values() for qty in lots.values())
    done = 0

    with get_db_cursor() as cur:
        for item_code, lots in scans_needed.items():
            total_needed = sum(lots.values())

            for (job, lot), need in lots.items():
                assign = min(need, total_needed)
                if assign == 0:
                    continue

                if from_location and not to_location:
                    trans_type = "Job Issue"
                    loc_field  = "from_location"
                    loc_value  = from_location
                elif to_location and not from_location:
                    trans_type = "Return"
                    loc_field  = "to_location"
                    loc_value  = to_location
                else:
                    trans_type = "Adjustment"
                    loc_field  = "from_location"
                    loc_value  = from_location or to_location  # fallback

                cur.execute(
                    "SELECT warehouse FROM pulltags WHERE job_number = %s AND lot_number = %s AND item_code = %s LIMIT 1",
                    (job, lot, item_code)
                )
                wh = cur.fetchone()
                warehouse = wh[0] if wh else None
                sb = scanned_by

                sql = f"""
                    INSERT INTO transactions
                        (transaction_type, date, warehouse, {loc_field},
                         job_number, lot_number, item_code, quantity, user_id)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql,
                    (trans_type, warehouse, loc_value, job, lot, item_code, assign, sb)
                )

                successful_scans = 0
                for idx in range(1, assign + 1):
                    sid = scan_inputs.get(f"scan_{item_code}_{idx}", "").strip()
                    if not sid:
                        continue

                    cur.execute(
                        "SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s AND transaction_type = 'Job Issue'", (sid,))
                    issues = cur.fetchone()[0]
                    cur.execute(
                        "SELECT COUNT(*) FROM scan_verifications WHERE scan_id = %s AND transaction_type = 'Return'", (sid,))
                    returns = cur.fetchone()[0]

                    if trans_type == "Job Issue" and issues - returns > 0:
                        continue
                    if trans_type == "Return" and issues > 0 and returns >= issues:
                        continue

                    cur.execute("""
                        INSERT INTO scan_verifications
                          (item_code, scan_id, job_number, lot_number,
                           scan_time, location, transaction_type, warehouse, scanned_by)
                        VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s)
                    """, (item_code, sid, job, lot, loc_value, trans_type, warehouse, sb))

                    # 🆕 UPDATE: Always update scan location if to_location exists
                    if to_location:
                        cur.execute("""
                            INSERT INTO current_scan_location (scan_id, item_code, location)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (scan_id) DO UPDATE SET location = EXCLUDED.location
                        """, (sid, item_code, to_location))
                    elif from_location:
                        cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                    successful_scans += 1
                    done += 1
                    if progress_callback:
                        pct = int(done / total_scans * 100)
                        progress_callback(pct)

                # 🧠 NEW DELTA LOGIC
                delta = 0
                if to_location and not from_location:
                    delta = assign
                elif from_location and not to_location:
                    delta = -assign

                cur.execute("""
                    INSERT INTO current_inventory (item_code, location, quantity, warehouse)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse) DO UPDATE
                        SET quantity = current_inventory.quantity + EXCLUDED.quantity
                """, (item_code, loc_value, delta, warehouse))

                total_needed -= assign
                if total_needed <= 0:
                    break
