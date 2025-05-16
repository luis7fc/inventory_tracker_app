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
