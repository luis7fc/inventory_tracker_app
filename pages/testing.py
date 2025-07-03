import streamlit as st
import math
import random
from collections import Counter, defaultdict
from contextlib import contextmanager
import psycopg2
from config import WAREHOUSES
from pages.receiving import IRISH_TOASTS

@contextmanager
def get_db_cursor():
    """Yields a fresh cursor and commits or rolls back when done."""
    conn = psycopg2.connect(
        host=st.secrets["DB_HOST"],
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        port=st.secrets.get("DB_PORT", 5432)
    )
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# Supabase migrations (run once separately):
# CREATE INDEX IF NOT EXISTS idx_inventory_wh_loc_item ON current_inventory(warehouse, location, item_code);
# ALTER TABLE current_inventory ADD CONSTRAINT unique_inventory_wh_loc_item UNIQUE(warehouse, location, item_code);
# ALTER TABLE current_inventory ADD CONSTRAINT fk_inventory_item FOREIGN KEY(item_code) REFERENCES items_master(item_code);
# ALTER TABLE current_scan_location ADD CONSTRAINT fk_scan_item FOREIGN KEY(item_code) REFERENCES items_master(item_code);

def run():
    # Preload metadata
    with get_db_cursor() as cur:
        # Items and scan requirement
        cur.execute("SELECT item_code, scan_required FROM items_master")
        rows = cur.fetchall()
        valid_items = {item for item, _ in rows}
        scan_required = {item: req for item, req in rows}
        # Locations and warehouse mapping
        cur.execute("SELECT location_code, warehouse, multi_item_allowed FROM locations")
        loc_meta = {loc: (wh, multi) for loc, wh, multi in cur.fetchall()}

    st.title("🔁 Transfer Operations")
    mode = st.radio(
        "Select Transfer Type",
        ["🏭 Warehouse Movement", "🚚 Cross-Warehouse Transfer"],
        horizontal=True,
        key="transfer_mode"
    )
    if mode == "🏭 Warehouse Movement":
        _internal_movement(valid_items, scan_required, loc_meta)
    else:
        _cross_warehouse_transfer(valid_items, scan_required, loc_meta)

def _internal_movement(valid_items, scan_required, loc_meta):
    st.header("🔀 Internal Movement")
    lines = st.session_state.get(
        "im_lines",
        [{"item_code": "", "quantity": 1, "pallet_qty": 1, "from_location": "", "to_location": "", "note": "", "scans": []}]
    )
    # Render lines
    for i, line in enumerate(lines):
        with st.expander(f"Line {i+1}", expanded=True):
            c1, c2, c3, c4, c5, c6, c7 = st.columns([2, 1, 1, 2, 2, 2, 1])
            line["item_code"] = c1.text_input("Item Code", line["item_code"], key=f"im_code_{i}")
            line["quantity"] = c2.number_input("Quantity", min_value=1, value=line["quantity"], key=f"im_qty_{i}")
            line["pallet_qty"] = c3.number_input("Pallet Qty", min_value=1, value=line["pallet_qty"], key=f"im_pallet_{i}")
            line["from_location"] = c4.text_input("From Location", line["from_location"], key=f"im_from_{i}")
            line["to_location"] = c5.text_input("To Location", line["to_location"], key=f"im_to_{i}")
            line["note"] = c6.text_input("Note", line["note"], key=f"im_note_{i}")
            if c7.button("Remove", key=f"im_rem_{i}"):
                lines.pop(i)
                st.session_state["im_lines"] = lines
                st.rerun()
            exp = math.ceil(line["quantity"] / line["pallet_qty"])
            scans = []
            if scan_required.get(line["item_code"], False):
                for j in range(exp):
                    scans.append(st.text_input(f"Scan {j+1}/{exp}",
                                              value=(line["scans"][j] if j < len(line["scans"]) else ""),
                                              key=f"im_scan_{i}_{j}"))
            line["scans"] = scans
    if st.button("Add Line"):
        lines.append({"item_code": "", "quantity": 1, "pallet_qty": 1, "from_location": "", "to_location": "", "note": "", "scans": []})
        st.session_state["im_lines"] = lines
        st.rerun()
    warehouse = st.selectbox("Warehouse", WAREHOUSES, key="im_wh")
    if not st.button("Submit Internal Movement"):
        return

    errors = []
    warns = []
    all_scans = []
    totals = defaultdict(int)
    for ln in lines:
        totals[(ln["item_code"], ln["from_location"])] += ln["quantity"]
    # Validate stock
    with get_db_cursor() as cur:
        for (item, loc), qty in totals.items():
            if item not in valid_items:
                errors.append(f"Unknown item '{item}'")
                continue
            cur.execute(
                "SELECT COALESCE(quantity, 0) FROM current_inventory WHERE warehouse=%s AND location=%s AND item_code=%s FOR UPDATE",
                (warehouse, loc, item)
            )
            avail = cur.fetchone()
            if avail is None or avail[0] < qty:
                warns.append(f"{item} at {loc}: have {avail[0] if avail else 0}, need {qty}")
    # Line-level checks
    for idx, ln in enumerate(lines):
        item, qty = ln["item_code"], ln["quantity"]
        fl, tl = ln["from_location"], ln["to_location"]
        if not item or qty <= 0 or not fl or not tl:
            errors.append(f"Line {idx+1}: missing data")
        if fl == tl:
            errors.append(f"Line {idx+1}: from/to same")
        if fl not in loc_meta:
            errors.append(f"Line {idx+1}: bad src loc {fl}")
        elif loc_meta[fl][0] != warehouse:
            errors.append(f"Line {idx+1}: src {fl} in {loc_meta[fl][0]}, expected {warehouse}")
        if tl not in loc_meta:
            errors.append(f"Line {idx+1}: bad dest loc {tl}")
        else:
            wh, multi = loc_meta[tl]
            if wh != warehouse:
                errors.append(f"Line {idx+1}: dest {tl} in {wh}, expected {warehouse}")
            else:
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(SUM(quantity), 0) FROM current_inventory WHERE warehouse=%s AND location=%s AND item_code!=%s",
                        (warehouse, tl, item)
                    )
                    other_qty = cur.fetchone()[0]
                    cur.execute(
                        "SELECT COALESCE(SUM(quantity), 0) FROM current_inventory WHERE warehouse=%s AND location=%s AND item_code=%s",
                        (warehouse, tl, item)
                    )
                    same_qty = cur.fetchone()[0]
                if other_qty > 0 and not multi:
                    errors.append(f"Line {idx+1}: dest {tl} multi not allowed")
                if same_qty > 0:
                    errors.append(f"Line {idx+1}: dest {tl} already has {item}")
        exp = math.ceil(qty / ln["pallet_qty"])
        scans = [s.strip() for s in ln["scans"]]
        if scan_required.get(item, False) and (len(scans) != exp or any(not s for s in scans)):
            errors.append(f"Line {idx+1}: expect {exp} scans")
        all_scans.extend(scans)
        for s in scans:
            with get_db_cursor() as cur:
                cur.execute("SELECT location, item_code FROM current_scan_location WHERE scan_id=%s", (s,))
                r = cur.fetchone()
            if r:
                scan_loc, scan_item = r
                if scan_loc != fl:
                    warns.append(f"Line {idx+1}: scan {s} at {scan_loc}, expected {fl}")
                if scan_item != item:
                    warns.append(f"Line {idx+1}: scan {s} tagged to {scan_item}, expected {item}")
            elif scan_required.get(item, False):
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT location, item_code FROM scan_verifications WHERE scan_id=%s ORDER BY scan_time DESC LIMIT 1",
                        (s,)
                    )
                    last_seen = cur.fetchone()
                if last_seen:
                    last_loc, last_item = last_seen
                    warns.append(f"Line {idx+1}: scan {s} not live, last at {last_loc} (item {last_item})")
                else:
                    warns.append(f"Line {idx+1}: scan {s} not recognized")
    dup = [s for s, c in Counter(all_scans).items() if c > 1]
    if dup:
        errors.append(f"Duplicate scans {dup}")
    if errors:
        st.error("\n".join(errors))
        return
    if warns:
        st.warning("Issues:\n" + "\n".join(warns))
        if not st.button("Bypass"):
            return
    # Commit
    try:
        with get_db_cursor() as cur:
            for idx, ln in enumerate(lines):
                item, qty = ln["item_code"], ln["quantity"]
                fl, tl = ln["from_location"], ln["to_location"]
                note = (ln["note"] + ";" + ";".join(warns)).strip(";") if warns else ln["note"]
                for s in ln["scans"]:
                    cur.execute("DELETE FROM current_scan_location WHERE scan_id=%s AND location=%s", (s, fl))
                cur.execute(
                    "INSERT INTO transactions(transaction_type, item_code, quantity, date, from_location, to_location, user_id, bypassed_warning, note, warehouse) "
                    "VALUES(%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)",
                    ("Internal Movement", item, qty, fl, tl, st.session_state.user, bool(warns), note, warehouse)
                )
                cur.execute(
                    "UPDATE current_inventory SET quantity=quantity-%s WHERE warehouse=%s AND location=%s AND item_code=%s",
                    (qty, warehouse, fl, item)
                )
                cur.execute(
                    "INSERT INTO current_inventory(warehouse, location, item_code, quantity) VALUES(%s, %s, %s, %s) "
                    "ON CONFLICT(warehouse, location, item_code) DO UPDATE SET quantity=current_inventory.quantity+EXCLUDED.quantity",
                    (warehouse, tl, item, qty)
                )
                for s in ln["scans"]:
                    cur.execute(
                        "INSERT INTO scan_verifications(item_code, scan_time, scan_id, location, transaction_type, warehouse, scanned_by) "
                        "VALUES(%s, NOW(), %s, %s, %s, %s, %s)",
                        (item, s, tl, "Internal Movement", warehouse, st.session_state.user)
                    )
                    cur.execute(
                        "INSERT INTO current_scan_location(scan_id, item_code, location, updated_at) VALUES(%s, %s, %s, NOW()) "
                        "ON CONFLICT(scan_id) DO UPDATE SET item_code=EXCLUDED.item_code, location=EXCLUDED.location, updated_at=EXCLUDED.updated_at",
                        (s, item, tl)
                    )
                st.progress(int((idx + 1) / len(lines) * 100))
        st.success(random.choice(IRISH_TOASTS))
        if st.button("Continue"):
            st.session_state.pop("im_lines", None)
            st.rerun()
    except Exception as e:
        st.error(f"Internal movement failed: {e}")
        with get_db_cursor() as cur:
            cur.execute(
                "INSERT INTO transactions(transaction_type, item_code, quantity, date, from_location, to_location, user_id, bypassed_warning, note, warehouse) "
                "VALUES(%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)",
                ("Error Log", item, qty, fl, tl, st.session_state.user, bool(warns), f"Failed: {e}", warehouse)
            )

def _cross_warehouse_transfer(valid_items, scan_required, loc_meta):
    st.header("🚚 Transfers Between Warehouses")
    lines = st.session_state.get(
        "transfer_lines",
        [{"item_code": "", "quantity": 1, "pallet_qty": 1, "from_location": "", "from_warehouse": "", "to_location": "", "to_warehouse": "", "note": "", "scans": []}]
    )
    # Render
    for i, line in enumerate(lines):
        with st.expander(f"Line {i+1}", expanded=True):
            c1, c2, c3 = st.columns([2, 1, 1])
            line["item_code"] = c1.text_input("Item Code", line["item_code"], key=f"tr_code_{i}")
            line["quantity"] = c2.number_input("Quantity", min_value=1, value=line["quantity"], key=f"tr_qty_{i}")
            line["pallet_qty"] = c3.number_input("Pallet Qty", min_value=1, value=line["pallet_qty"], key=f"tr_pallet_{i}")
            c4, c5 = st.columns(2)
            line["from_location"] = c4.text_input("From Loc", line["from_location"], key=f"tr_fromloc_{i}")
            line["from_warehouse"] = c5.selectbox("From WH", WAREHOUSES, key=f"tr_fromwh_{i}")
            c6, c7 = st.columns(2)
            line["to_location"] = c6.text_input("To Loc", line["to_location"], key=f"tr_toloc_{i}")
            line["to_warehouse"] = c7.selectbox("To WH", WAREHOUSES, key=f"tr_towh_{i}")
            line["note"] = st.text_input("Note", line["note"], key=f"tr_note_{i}")
            if st.button("Remove", key=f"tr_rem_{i}"):
                lines.pop(i)
                st.session_state["transfer_lines"] = lines
                st.rerun()
            exp = math.ceil(line["quantity"] / line["pallet_qty"])
            scans = []
            if scan_required.get(line["item_code"], False):
                for j in range(exp):
                    scans.append(st.text_input(f"Scan {j+1}/{exp}", line["scans"][j] if j < len(line["scans"]) else "", key=f"tr_scan_{i}_{j}"))
            line["scans"] = scans
    if st.button("Add Transfer Line"):
        lines.append({"item_code": "", "quantity": 1, "pallet_qty": 1, "from_location": "", "from_warehouse": "", "to_location": "", "to_warehouse": "", "note": "", "scans": []})
        st.session_state["transfer_lines"] = lines
        st.rerun()
    if not st.button("Submit Transfer"):
        return
    errors = []
    warns = []
    all_scans = []
    totals = defaultdict(int)
    for ln in lines:
        totals[(ln["item_code"], ln["from_warehouse"], ln["from_location"])] += ln["quantity"]
    with get_db_cursor() as cur:
        for (item, wh, loc), qty in totals.items():
            if item not in valid_items:
                errors.append(f"Unknown item {item}")
                continue
            cur.execute(
                "SELECT COALESCE(quantity, 0) FROM current_inventory WHERE warehouse=%s AND location=%s AND item_code=%s FOR UPDATE",
                (wh, loc, item)
            )
            avail = cur.fetchone()
            if avail is None or avail[0] < qty:
                warns.append(f"{item} at {loc}/{wh}: have {avail[0] if avail else 0}, need {qty}")
    for i, ln in enumerate(lines):
        item, qty = ln["item_code"], ln["quantity"]
        fl, tl = ln["from_location"], ln["to_location"]
        fwh, twh = ln["from_warehouse"], ln["to_warehouse"]
        if not item or qty <= 0 or not fl or not tl or not fwh or not twh:
            errors.append(f"Line {i+1}: missing data")
        if fl == tl and fwh == twh:
            errors.append(f"Line {i+1}: from/to same")
        if fl not in loc_meta or loc_meta[fl][0] != fwh:
            errors.append(f"Line {i+1}: bad src {fl}@{fwh}")
        if tl not in loc_meta or loc_meta[tl][0] != twh:
            errors.append(f"Line {i+1}: bad dest {tl}@{twh}")
        else:
            wh, multi = loc_meta[tl]
            with get_db_cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(quantity), 0) FROM current_inventory WHERE warehouse=%s AND location=%s AND item_code!=%s",
                    (twh, tl, item)
                )
                other_qty = cur.fetchone()[0]
                cur.execute(
                    "SELECT COALESCE(SUM(quantity), 0) FROM current_inventory WHERE warehouse=%s AND location=%s AND item_code=%s",
                    (twh, tl, item)
                )
                same_qty = cur.fetchone()[0]
            if other_qty > 0 and not multi:
                errors.append(f"Line {i+1}: dest {tl} multi not allowed")
            if same_qty > 0:
                errors.append(f"Line {i+1}: dest {tl} already has {item}")
        exp = math.ceil(qty / ln["pallet_qty"])
        scans = [s.strip() for s in ln["scans"]]
        if scan_required.get(item, False) and (len(scans) != exp or any(not s for s in scans)):
            errors.append(f"Line {i+1}: expect {exp} scans")
        all_scans.extend(scans)
        for s in scans:
            with get_db_cursor() as cur:
                cur.execute("SELECT location, item_code FROM current_scan_location WHERE scan_id=%s", (s,))
                r = cur.fetchone()
            if r:
                scan_loc, scan_item = r
                if scan_loc != fl or loc_meta.get(scan_loc, [None])[0] != fwh:
                    warns.append(f"Line {i+1}: scan {s} at {scan_loc} ({loc_meta.get(scan_loc, [None])[0]}), expected {fl} ({fwh})")
                if scan_item != item:
                    warns.append(f"Line {i+1}: scan {s} tagged to {scan_item}, expected {item}")
            elif scan_required.get(item, False):
                with get_db_cursor() as cur:
                    cur.execute(
                        "SELECT location, item_code FROM scan_verifications WHERE scan_id=%s ORDER BY scan_time DESC LIMIT 1",
                        (s,)
                    )
                    last_seen = cur.fetchone()
                if last_seen:
                    last_loc, last_item = last_seen
                    warns.append(f"Line {i+1}: scan {s} not live, last at {last_loc} (item {last_item})")
                else:
                    warns.append(f"Line {i+1}: scan {s} not recognized")
    dup = [s for s, c in Counter(all_scans).items() if c > 1]
    if dup:
        errors.append(f"Duplicate scans {dup}")
    if errors:
        st.error("\n".join(errors))
        return
    if warns:
        st.warning("Issues:\n" + "\n".join(warns))
        if not st.button("Bypass"):
            return
    try:
        with get_db_cursor() as cur:
            for i, ln in enumerate(lines):
                item, qty = ln["item_code"], ln["quantity"]
                fl, tl = ln["from_location"], ln["to_location"]
                fwh, twh = ln["from_warehouse"], ln["to_warehouse"]
                note = (ln["note"] + ";" + ";".join(warns)).strip(";") if warns else ln["note"]
                for s in ln["scans"]:
                    cur.execute("DELETE FROM current_scan_location WHERE scan_id=%s AND location=%s", (s, fl))
                cur.execute(
                    "INSERT INTO transactions(transaction_type, item_code, quantity, date, from_location, to_location, user_id, bypassed_warning, note, warehouse) "
                    "VALUES(%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)",
                    ("Transfer", item, qty, fl, tl, st.session_state.user, bool(warns), note, twh)
                )
                cur.execute(
                    "UPDATE current_inventory SET quantity=quantity-%s WHERE warehouse=%s AND location=%s AND item_code=%s",
                    (qty, fwh, fl, item)
                )
                cur.execute(
                    "INSERT INTO current_inventory(warehouse, location, item_code, quantity) VALUES(%s, %s, %s, %s) "
                    "ON CONFLICT(warehouse, location, item_code) DO UPDATE SET quantity=current_inventory.quantity+EXCLUDED.quantity",
                    (twh, tl, item, qty)
                )
                for s in ln["scans"]:
                    cur.execute(
                        "INSERT INTO scan_verifications(item_code, scan_time, scan_id, location, transaction_type, warehouse, scanned_by) "
                        "VALUES(%s, NOW(), %s, %s, %s, %s, %s)",
                        (item, s, tl, "Transfer", twh, st.session_state.user)
                    )
                    cur.execute(
                        "INSERT INTO current_scan_location(scan_id, item_code, location, updated_at) VALUES(%s, %s, %s, NOW()) "
                        "ON CONFLICT(scan_id) DO UPDATE SET item_code=EXCLUDED.item_code, location=EXCLUDED.location, updated_at=EXCLUDED.updated_at",
                        (s, item, tl)
                    )
                st.progress(int((i + 1) / len(lines) * 100))
        st.success(random.choice(IRISH_TOASTS))
        if st.button("Done"):
            st.session_state.pop("transfer_lines", None)
            st.rerun()
    except Exception as e:
        st.error(f"Transfer failed: {e}")
        with get_db_cursor() as cur:
            cur.execute(
                "INSERT INTO transactions(transaction_type, item_code, quantity, date, from_location, to_location, user_id, bypassed_warning, note, warehouse) "
                "VALUES(%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)",
                ("Error Log", item, qty, fl, tl, st.session_state.user, bool(warns), f"Failed: {e}", twh)
            )
