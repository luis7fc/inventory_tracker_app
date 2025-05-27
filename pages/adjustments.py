import streamlit as st
import random
from collections import Counter, defaultdict

from db import get_db_cursor
from config import WAREHOUSES

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────
IRISH_TOASTS = [
    "☘️ Sláinte! Transaction submitted successfully!",
    "🍀 Luck o’ the Irish – you did it!",
    "🦃 Cheers, let’s grab a beer – transaction success!",
    "🌈 Pot of gold secured – job well done!",
    "🪙 May your inventory always balance – success!",
]

# Every location enforces strict scan ↔ item uniqueness (no skip list)
SKIP_SCAN_CHECK_LOCATIONS: tuple[str, ...] = ()

# ──────────────────────────────────────────────────────────────────────────────
# Helper: insert pull‑tag row (always executed)
# ──────────────────────────────────────────────────────────────────────────────

def insert_pulltag_line(
    cur,
    job_number: str,
    lot_number: str,
    item_code: str,
    quantity: int,
    location: str,
    transaction_type: str = "Job Issue",
    note: str = "Qty verified by WH",
):
    """Insert a pull‑tag and derive warehouse from the location."""

    cur.execute(
        "SELECT warehouse FROM locations WHERE location_code = %s",
        (location,),
    )
    wh_row = cur.fetchone()
    if not wh_row:
        raise Exception(f"Unknown location '{location}' – cannot resolve warehouse.")
    warehouse = wh_row[0]

    cur.execute(
        """
        INSERT INTO pulltags
          (job_number, lot_number, item_code, quantity,
           description, cost_code, uom, status,
           transaction_type, note, warehouse)
        SELECT %s, %s, item_code, %s,
               item_description, cost_code, uom,
               'pending', %s, %s, %s
        FROM items_master
        WHERE item_code = %s
        RETURNING id
        """,
        (
            job_number,
            lot_number,
            quantity,
            transaction_type,
            note,
            warehouse,
            item_code,
        ),
    )
    return cur.fetchone()[0]

# ──────────────────────────────────────────────────────────────────────────────
# Finaliser for scan‑tracked items (ADD / RETURNB)
# ──────────────────────────────────────────────────────────────────────────────

def finalize_add(
    scans_needed: dict,
    scan_inputs: dict,
    *,
    from_location: str | None,
    to_location: str | None,
    scanned_by: str | None,
    warehouse: str,
    progress_callback=None,
):
    """Validate all scan IDs and update inventory / scan tables."""

    if progress_callback is None:
        progress_callback = lambda *_: None

    scan_map: defaultdict[tuple[str, str, str], list[str]] = defaultdict(list)
    errors: list[str] = []

    # Build structured map & basic validation
    for item_code, lots in scans_needed.items():
        for (job, lot), qty in lots.items():
            for n in range(1, qty + 1):
                key = f"scan_{item_code}_{job}_{lot}_{n}"
                sid = scan_inputs.get(key, "").strip()
                if not sid:
                    errors.append(
                        f"Missing scan {n} for {item_code} — Job {job} / Lot {lot}."
                    )
                else:
                    scan_map[(item_code, job, lot)].append(sid)

    dupes = [s for s, c in Counter([s for lst in scan_map.values() for s in lst]).items() if c > 1]
    if dupes:
        errors.append(f"Duplicate scan IDs entered: {', '.join(dupes)}")

    if errors:
        raise Exception("\n".join(errors))

    total_scans = sum(len(v) for v in scan_map.values())
    processed   = 0

    with get_db_cursor() as cur:
        for (item_code, job, lot), sid_list in scan_map.items():
            trans_type = "Return" if to_location and not from_location else "Job Issue"
            loc_value  = to_location if trans_type == "Return" else from_location

            # Single‑item location guard
            cur.execute(
                "SELECT multi_item_allowed FROM locations WHERE location_code = %s",
                (loc_value,),
            )
            multi_allowed = bool(cur.fetchone() and cur.fetchone())
            if not multi_allowed:
                cur.execute(
                    "SELECT DISTINCT item_code FROM current_inventory WHERE location = %s AND quantity > 0",
                    (loc_value,),
                )
                present = [r[0] for r in cur.fetchall()]
                if present and any(p != item_code for p in present):
                    raise Exception(
                        f"Location '{loc_value}' currently holds other item codes ({', '.join(present)})."
                    )

            # Per‑scan processing
            for sid in sid_list:
                # Check existing mapping
                cur.execute(
                    "SELECT item_code, location FROM current_scan_location WHERE scan_id = %s",
                    (sid,),
                )
                prev = cur.fetchone()

                if trans_type == "Return":
                    if prev and prev[0] != item_code:
                        raise Exception(
                            f"Scan '{sid}' already registered to {prev[0]} in {prev[1]}."
                        )
                    # upsert to_location
                    cur.execute(
                        """
                        INSERT INTO current_scan_location (scan_id, item_code, location, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (scan_id)
                              DO UPDATE SET item_code = EXCLUDED.item_code,
                                            location  = EXCLUDED.location,
                                            updated_at= EXCLUDED.updated_at
                        """,
                        (sid, item_code, loc_value),
                    )
                else:  # Job Issue
                    if not prev:
                        raise Exception(f"Scan '{sid}' not found in inventory.")
                    if prev[0] != item_code:
                        raise Exception(
                            f"Scan '{sid}' belongs to item {prev[0]}, not {item_code}."
                        )
                    if prev[1] != from_location:
                        raise Exception(
                            f"Scan '{sid}' located in '{prev[1]}', expected '{from_location}'."
                        )
                    # delete – scan leaves inventory
                    cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                # Transaction row (qty = 1 per scan)
                loc_col = "to_location" if trans_type == "Return" else "from_location"
                cur.execute(
                    f"""
                    INSERT INTO transactions
                          (transaction_type, date, warehouse, {loc_col},
                           job_number, lot_number, item_code, quantity, user_id)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, 1, %s)
                    """,
                    (
                        trans_type,
                        warehouse,
                        loc_value,
                        job,
                        lot,
                        item_code,
                        scanned_by or "unknown",
                    ),
                )

                # Inventory delta
                delta = 1 if trans_type == "Return" else -1
                cur.execute(
                    """
                    INSERT INTO current_inventory (item_code, location, warehouse, quantity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_code, location, warehouse)
                        DO UPDATE SET quantity = current_inventory.quantity + EXCLUDED.quantity
                    """,
                    (item_code, loc_value, warehouse, delta),
                )

                processed += 1
                progress_callback(int(processed / total_scans * 100))

# ──────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ──────────────────────────────────────────────────────────────────────────────

def run():
    st.title("🛠️ Post‑Kitting Adjustments")

    transaction_type = st.selectbox("Transaction Type", ["ADD", "RETURNB"], key="type")
    warehouse        = st.selectbox("Warehouse", WAREHOUSES, key="wh")
    location         = st.text_input("Location", placeholder="e.g., STAGE-A", key="loc")
    note             = st.text_input("Transaction Note (optional)", key="note")

    adjustments = st.session_state.setdefault("adjustments", [])

    # ── Row entry ───────────────────────────────────────────────────────────
    with st.expander("➕ Add Adjustment Row"):
        job  = st.text_input("Job Number", key="job")
        lot  = st.text_input("Lot Number", key="lot")
        code = st.text_input("Item Code", key="code")
        qty  = st.number_input("Quantity", min_value=1, value=1, key="qty")

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT item_description, scan_required FROM items_master WHERE item_code = %s",
                        (code.strip(),),
                    )
                    res = cur.fetchone()
                desc           = res[0] if res else "(Unknown)"
                scan_required  = bool(res and res[1])

                adjustments.append(
                    {
                        "job_number": job.strip(),
                        "lot_number": lot.strip(),
                        "item_code": code.strip(),
                        "quantity": int(qty),
                        "description": desc,
                        "scan_required": scan_required,
                    }
                )
            else:
                st.warning("Please complete all fields before adding.")

    # ── Preview & delete ───────────────────────────────────────────────────
    if adjustments:
        st.markdown("### 📋 Adjustments Preview")
        for i, row in enumerate(adjustments):
            cols = st.columns([3, 3, 3, 2, 3, 2, 1])
            cols[0].markdown(f"**Job:** {row['job_number']}")
            cols[1].markdown(f"**Lot:** {row['lot_number']}")
            cols[2].markdown(f"
