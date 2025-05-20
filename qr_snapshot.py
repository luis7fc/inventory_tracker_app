from __future__ import annotations

import io
import time
from typing import Tuple

import pandas as pd
import qrcode
from supabase import create_client
import streamlit as st

# ── Supabase client init ------------------------------------------------------
# Values come from st.secrets
SUPA_URL: str = st.secrets["SUPABASE_URL"]
SUPA_KEY: str = st.secrets["SUPABASE_SERVICE_KEY"]  # ← updated secret key name
SUPABASE = create_client(SUPA_URL, SUPA_KEY)
BUCKET = "kitting-snapshots"  # ← updated bucket name

# ── Helpers -------------------------------------------------------------------

def _make_html(df: pd.DataFrame) -> bytes:
    """Render a minimal HTML table so the kit bundle can be opened in a browser."""
    head = "<html><body><h2>Kit Snapshot</h2>"
    body = df.to_html(index=False, border=1)
    tail = "</body></html>"
    return (head + body + tail).encode()


def _upload_and_sign(path: str, data: bytes, mime: str, *, expires: int = 60 * 60 * 24) -> str:
    """Upload *data* to Supabase Storage (upsert) and return a signed URL."""

    # supabase‑py ≥2.3.0 expects file_options for metadata / upsert
    SUPABASE.storage.from_(BUCKET).upload(
        path,
        data,
        file_options={"contentType": mime, "upsert": True},
    )

    signed = SUPABASE.storage.from_(BUCKET).create_signed_url(path, expires)
    return signed["signedURL"] if isinstance(signed, dict) else signed


# ── Public --------------------------------------------------------------------

def generate_qr_snapshot_from_df(df: pd.DataFrame, user_email: str) -> Tuple[str, bytes]:
    """Return (signed_url, png_bytes) representing the provided *df*."""

    if df.empty:
        raise ValueError("DataFrame is empty – nothing to snapshot.")

    # Ensure required columns exist
    for col in ("job_number", "lot_number"):
        if col not in df.columns:
            df[col] = ""

    ts = int(time.time())
    html_path = f"bundle/{user_email}/{ts}.html"
    signed_url = _upload_and_sign(html_path, _make_html(df), "text/html")

    # Generate QR
    qr_img = qrcode.make(signed_url)
    buf = io.BytesIO()
    qr_img.save(buf, format="PNG")

    return signed_url, buf.getvalue()
