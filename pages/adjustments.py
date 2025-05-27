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

# Every location enforces strict scan–item uniqueness
SKIP_SCAN_CHECK_LOCATIONS: tuple[str, ...] = ()

# ──────────────────────────────────────────────────────────────────────────────
# Helper: pull‑tag insert (always called)
# ──────────────────────────────────────────────────────────────────────────────

def insert_pulltag_line(
    cur,
    job_number: str,
    lot_number: str,
    item_code: str,
    quantity: int,
    location: str,
    transaction_type: str,
    note: str,
):
    """Insert a pulltag row using items_master metadata and derive warehouse."""

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
        FROM   items_master
        WHERE  item_code = %s
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
# Finaliser for scan‑tracked items – writes transactions & inventory
# ──────────────────────────────────────────────────────────────────────────────

def finalize_scan_items(
    scans_needed: dict,
    scan_inputs: dict,
    *,
    from_location: str | None,
    to_location: str | None,
    scanned_by: str,
    warehouse: str,
    progress_callback=None,
):
    """Validate scan IDs and update DB for scan‑tracked items only."""

    if progress_callback is None:
        progress_callback = lambda *_: None

    scan_map: defaultdict[tuple[str, str, str], list[str]] = defaultdict(list)
    errors: list[str] = []

    for item_code, lots in scans_needed.items():
        for (job, lot), qty in lots.items():
            for n in range(1, qty + 1):
                key = f"scan_{item_code}_{job}_{lot}_{n}"
                sid = scan_inputs.get(key, "").strip()
                if not sid:
                    errors.append(f"Missing scan {n} for {item_code} — Job {job} / Lot {lot}.")
                else:
                    scan_map[(item_code, job, lot)].append(sid)

    # Duplicate detection
    flat_scans = [sid for bundle in scan_map.values() for sid in bundle]
    dupes = [s for s, c in Counter(flat_scans).items() if c > 1]
    if dupes:
        errors.append(f"Duplicate scan IDs entered: {', '.join(dupes)}")

    if errors:
        raise Exception("\n".join(errors))

    total = len(flat_scans)
    done  = 0

    with get_db_cursor() as cur:
        for (item_code, job, lot), sid_list in scan_map.items():
            trans_type = "Return" if to_location and not from_location else "Job Issue"
            loc_value  = to_location if trans_type == "Return" else from_location

            # Guard: single‑item locations
            cur.execute(
                "SELECT multi_item_allowed FROM locations WHERE location_code = %s",
                (loc_value,),
            )
            r = cur.fetchone()
            multi_allowed = bool(r and r[0])
            if not multi_allowed:
                cur.execute(
                    "SELECT DISTINCT item_code FROM current_inventory WHERE location = %s AND quantity > 0",
                    (loc_value,),
                )
                present = [row[0] for row in cur.fetchall()]
                if present and any(p != item_code for p in present):
                    raise Exception(f"Location '{loc_value}' currently holds other items ({', '.join(present)}).")

            for sid in sid_list:
                # Existing scan lookup
                cur.execute(
                    "SELECT item_code, location FROM current_scan_location WHERE scan_id = %s",
                    (sid,),
                )
                prev = cur.fetchone()

                if trans_type == "Return":
                    if prev and prev[0] != item_code:
                        raise Exception(f"Scan '{sid}' registered to {prev[0]} in {prev[1]}.")
                    # Upsert new location
                    cur.execute(
                        """
                        INSERT INTO current_scan_location (scan_id, item_code, location, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (scan_id) DO UPDATE
                               SET item_code = EXCLUDED.item_code,
                                   location  = EXCLUDED.location,
                                   updated_at= EXCLUDED.updated_at
                        """,
                        (sid, item_code, loc_value),
                    )
                else:  # Job Issue
                    if not prev:
                        raise Exception(f"Scan '{sid}' not found in inventory.")
                    if prev[0] != item_code:
                        raise Exception(f"Scan '{sid}' belongs to item {prev[0]}, not {item_code}.")
                    if prev[1] != from_location:
                        raise Exception(f"Scan '{sid}' located in '{prev[1]}', expected '{from_location}'.")
                    # Remove from scan table
                    cur.execute("DELETE FROM current_scan_location WHERE scan_id = %s", (sid,))

                # Insert transaction (qty = 1 per scan)
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
                        scanned_by,
                    ),
                )

                # Adjust inventory
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

                done += 1
                progress_callback(int(done / total * 100))

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

    # ── Entry form ────────────────────────────────────────────────────────
    with st.expander("➕ Add Adjustment Row"):
        col_j, col_l, col_c, col_q = st.columns([2, 2, 3, 1])
        job  = col_j.text_input("Job #", key="job")
        lot  = col_l.text_input("Lot #", key="lot")
        code = col_c.text_input("Item Code", key="code")
        qty  = col_q.number_input("Qty", min_value=1, value=1, key="qty")

        if st.button("Add to List"):
            if all([job.strip(), lot.strip(), code.strip(), qty > 0]):
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT item_description, scan_required FROM items_master WHERE item_code = %s",
                        (code.strip(),),
                    )
                    res = cur.fetchone()
                desc          = res[0] if res else "(Unknown)"
                scan_required = bool(res and res[1])

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
                st.experimental_rerun()
            else:
                st.warning("Fill all fields before adding.")

    # ── Preview table ─────────────────────────────────────────────────────
    if adjustments:
        st.markdown("### 📋 Pending Adjustments")
        for i, row in enumerate(adjustments):
            cols = st.columns([2, 2, 3, 1, 3, 1, 1])
            cols[0].markdown(f"**Job:** {row['job_number']}")
            cols[1].markdown(f"**Lot:** {row['lot_number']}")
            cols[2].markdown(f"**Item:** {row['item
