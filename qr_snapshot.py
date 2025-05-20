from __future__ import annotations
import io, os, textwrap, time
from typing import List

import pandas as pd
import qrcode
from supabase import create_client, Client
from db import get_db_cursor

# ──────────────────────────────────────────────
# Supabase client & constants
# ──────────────────────────────────────────────
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SERVICE_ROLE  = os.environ["SUPABASE_SERVICE_KEY"]          # server-side secret
SUPABASE: Client = create_client(SUPABASE_URL, SERVICE_ROLE)

BUCKET  = "kitting-snapshots"
EXPIRES = 60 * 60 * 24 * 30          # 30 days

# ──────────────────────────────────────────────
# HTML template helper
# ──────────────────────────────────────────────
def _make_html(df: pd.DataFrame) -> bytes:
    html = textwrap.dedent(f"""
    <!doctype html><html>
      <head>
        <meta charset="utf-8"/>
        <title>Kitted Materials</title>
        <style>
          body {{ font-family: Arial, sans-serif; margin: 1rem; }}
          table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
          th, td {{ border: 1px solid #ccc; padding: .35rem .5rem; }}
          th {{ background:#f0f0f0; }}
        </style>
      </head>
      <body>
        <h2>Kitted Materials – generated {time.strftime('%Y-%m-%d %H:%M:%S')}</h2>
        {df.to_html(index=False)}
      </body>
    </html>
    """)
    return html.encode("utf-8")

# ──────────────────────────────────────────────
# Storage helper
# ──────────────────────────────────────────────
def _upload_and_sign(path: str, data: bytes, mime: str, expires: int = EXPIRES) -> str:
    """Upload bytes to Supabase Storage (upsert) and return a signed URL."""
    SUPABASE.storage.from_(BUCKET).upload(path, data, content_type=mime, upsert=True)
    return SUPABASE.storage.from_(BUCKET).create_signed_url(path, expires)["signedURL"]

# ──────────────────────────────────────────────
# Public API – main generator
# ──────────────────────────────────────────────
def generate_qr_snapshot_from_df(df: pd.DataFrame, created_by: str) -> tuple[str, bytes]:
    """
    Build one static HTML snapshot from *df*, upload it, generate a QR PNG,
    save audit rows, and return (signed_url, png_bytes).
    """
    if df.empty:
        raise ValueError("DataFrame is empty – nothing to snapshot.")

    # meta
    job_numbers: List[str] = df["job_number"].unique().tolist()
    lot_numbers: List[str] = df["lot_number"].unique().tolist()

    # 1️⃣  HTML → Storage
    ts        = int(time.time())
    html_path = f"bundle/batch-{ts}.html"
    signed_url = _upload_and_sign(html_path, _make_html(df), "text/html")

    # 2️⃣  QR PNG (in-memory)
    qr_img   = qrcode.make(signed_url)
    buf      = io.BytesIO(); qr_img.save(buf, format="PNG")
    png_bytes = buf.getvalue()

    #    upload PNG (optional, handy for re-print UI)
    png_path       = html_path.replace(".html", ".png")
    png_signed_url = _upload_and_sign(png_path, png_bytes, "image/png")

    # 3️⃣  Persist audit rows
    pulltag_ids = df["id"].astype(str).tolist()      # ensure uuid→str
    with get_db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO kitting_batches
              (job_numbers, lot_numbers, snapshot_url, qr_png_url, created_by)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
            """,
            (job_numbers, lot_numbers, signed_url, png_signed_url, created_by)
        )
        batch_id = cur.fetchone()[0]

        cur.executemany(
            "INSERT INTO batch_pulltags (batch_id, pulltag_id) VALUES (%s, %s)",
            [(batch_id, pid) for pid in pulltag_ids]
        )

    return signed_url, png_bytes
