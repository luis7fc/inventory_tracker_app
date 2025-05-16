import streamlit as st
from datetime import datetime
from db import get_db_cursor
from config import WAREHOUSES

# Updated insert_pulltag_line with warehouse and kitted status
def insert_pulltag_line(cur, job_number, lot_number, item_code, quantity, transaction_type="Job Issue", warehouse=None, status="kitted"):
    sql = """
    INSERT INTO pulltags
      (job_number, lot_number, item_code, quantity,
       description, cost_code, uom, status, transaction_type, warehouse)
    SELECT
      %s, %s, item_code, %s, item_description,
      cost_code, uom, %s, %s, %s
    FROM items_master
    WHERE item_code = %s
    RETURNING id
    """
    cur.execute(sql, (job_number, lot_number, quantity, status, transaction_type, warehouse, item_code))
    return cur.fetchone()[0]

# Finalize ADD/RETURNB logic
# (... finalize_add definition remains unchanged ...)

# Main App

def run():
    st.title("🛠️ Post-Kitting Adjustments")
    transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"])
    warehouse = st.selectbox("Warehouse", WAREHOUSES)
    location = st.text_input("Location", placeholder="e.g., STAGE-A")
    note = st.text_input("Transaction Note (Optional)")

    if "adjustments" not in st.session_state:
        st.session_state.adjustments = []

    with st.expander("➕ Add Adjustment Row"):
        job = st.text_input("Job Number")
        lot = st.text_input("Lot Number")
        code = st.text_input("Item Code")
        qty = st.number_input("Quantity", min_value=1, value=1, step=1)

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                with get_db_cursor() as cur:
                    cur.execute("SELECT item_description FROM items_master WHERE item_code = %s", (code.strip(),))
                    result = cur.fetchone()
                    desc = result[0] if result else "(Unknown Item)"
                st.session_state.adjustments.append({
                    "job_number": job.strip(),
                    "lot_number": lot.strip(),
                    "item_code": code.strip(),
                    "quantity": int(qty),
                    "description": desc
                })
            else:
                st.warning("Please complete all fields before adding.")

    if st.session_state.adjustments:
        st.markdown("### 📋 Adjustments Preview")
        for i, row in enumerate(st.session_state.adjustments):
            cols = st.columns([3, 3, 3, 2, 3, 1])
            cols[0].markdown(f"**Job:** {row['job_number']}")
            cols[1].markdown(f"**Lot:** {row['lot_number']}")
            cols[2].markdown(f"**Item:** {row['item_code']}")
            cols[3].markdown(f"**Qty:** {row['quantity']}")
            cols[4].markdown(f"**Desc:** {row['description']}")
            if cols[5].button("❌", key=f"remove_{i}"):
                st.session_state.adjustments.pop(i)
                st.experimental_rerun()

        if st.button("Submit Adjustments"):
            scans_needed = {}
            job_lot_queue = []
            confirmed_rows = []

            for row in st.session_state.adjustments:
                job = row["job_number"]
                lot = row["lot_number"]
                code = row["item_code"]
                qty = row["quantity"]

                with get_db_cursor() as cur:
                    cur.execute("SELECT item_code FROM items_master WHERE item_code = %s AND cost_code = item_code", (code,))
                    if not cur.fetchone():
                        st.info(f"ℹ️ Item {code} not found or not scan-tracked. Skipped.")
                        continue
                    insert_pulltag_line(cur, job, lot, code, qty, transaction_type, warehouse)

                job_lot_queue.append((job, lot))
                scans_needed.setdefault(code, {}).setdefault((job, lot), 0)
                scans_needed[code][(job, lot)] += qty
                confirmed_rows.append({"Job": job, "Lot": lot, "Item": code, "Qty": qty, "Type": transaction_type})

            if not scans_needed:
                st.warning("No valid adjustments submitted.")
                st.stop()

            st.session_state.scans_needed = scans_needed
            st.session_state.job_lot_queue = job_lot_queue
            st.session_state.confirmed_rows = confirmed_rows
            st.session_state.finalize_ready = True

# ------------------------------------------------------------------
# 4) Scan entry + finalisation  (only after “Submit Adjustments”)
# ------------------------------------------------------------------
    if st.session_state.get("finalize_ready"):
          scans_needed   = st.session_state["scans_needed"]
          job_lot_queue  = st.session_state["job_lot_queue"]
          confirmed_rows = st.session_state["confirmed_rows"]
      
          st.markdown("### 🔍 Scan Required Items")
          with st.form("scan_form"):
              scan_inputs = {}
              for item_code, lots in scans_needed.items():
                  for (job, lot), qty in lots.items():
                      st.write(f"**{item_code} — Job {job} / Lot {lot} — Total Scans: {qty}**")
                      scan_inputs[f"pallet_{item_code}_{job}_{lot}"] = st.text_input(
                          "Optional Pallet ID", key=f"pallet_{item_code}_{job}_{lot}"
                      )
                      scan_inputs[f"pallet_qty_{item_code}_{job}_{lot}"] = st.number_input(
                          "Pallet Quantity", min_value=1, value=1, step=1,
                          key=f"pallet_qty_{item_code}_{job}_{lot}"
                      )
                      for i in range(1, qty + 1):
                          scan_inputs[f"scan_{item_code}_{i}"] = st.text_input(
                              f"Scan {i} for {item_code}", key=f"scan_{item_code}_{i}"
                          )
      
              submitted = st.form_submit_button("Finalize Adjustments")
      
          if submitted:
              if not location:
                  st.error("Please enter a Location before finalizing.")
              else:
                  sb = st.session_state.get("username", "unknown")
                  progress_bar = st.progress(0)
      
                  with st.spinner("Processing adjustments..."):
                      def update_progress(pct: int):         # inner helper
                          progress_bar.progress(pct)
      
                      finalize_add(
                          scans_needed,
                          scan_inputs,
                          job_lot_queue,
                          from_location=location if transaction_type == "ADD" else None,
                          to_location=location   if transaction_type == "RETURNB" else None,
                          scanned_by=sb,
                          progress_callback=update_progress,
                          warehouse=warehouse
                      )
      
                  st.success("✅ Adjustments finalised and inventory updated.")
                  st.session_state.adjustments.clear()
                  st.session_state.finalize_ready = False

