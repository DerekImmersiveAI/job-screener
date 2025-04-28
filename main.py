#!/usr/bin/env python3
"""
job-screener - main.py
────────────────────────────────────────────────────────────────────────────
  1. Downloads the most-recent BrightData CSV (or any HTTP/S URL).
  2. Cleans + scores each job row.
  3. Upserts results to Airtable in chunks.
────────────────────────────────────────────────────────────────────────────
Required ENV
────────────
BRIGHTDATA_URL         – HTTPS link to latest CSV (fallback: CSV_URL)
AIRTABLE_API_KEY       – “Bearer …” key
AIRTABLE_BASE_ID       – e.g. appXXXXXXXXXXXXXX
AIRTABLE_TABLE_NAME    – target table
OPTIONAL
CHUNK_SIZE   – rows per Airtable batch (default 10)
LOG_LEVEL    – DEBUG / INFO / WARNING (default INFO)
"""

import csv
import io
import os
import sys
import time
import math
import json
import glob
import gzip
import logging as log
from datetime import datetime, timezone
from typing import Dict, List, Any, Generator, Optional

import requests
import pandas as pd
from dotenv import load_dotenv

# ─────────────────────────────  ENV & LOG  ────────────────────────────────
load_dotenv()  # loads .env for local runs

def require_env(name: str, *fallbacks: str) -> str:
    """Return the first non-empty env value among name + fallbacks or exit."""
    for key in (name, *fallbacks):
        val = os.getenv(key)
        if val:
            if key != name:
                log.warning("⚠️  using %s because %s is missing", key, name)
            return val
    log.error(
        "❌  Required environment variable %s (or %s) not found. "
        "Set it in Render → Environment.", name, ", ".join(fallbacks)
    )
    sys.exit(1)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
log.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

BRIGHT_URL   = require_env("BRIGHTDATA_URL", "CSV_URL")
AIR_KEY      = require_env("AIRTABLE_API_KEY")
AIR_BASE     = require_env("AIRTABLE_BASE_ID")
AIR_TABLE    = require_env("AIRTABLE_TABLE_NAME")
CHUNK_SIZE   = int(os.getenv("CHUNK_SIZE", "10"))

AIR_ENDPOINT = f"https://api.airtable.com/v0/{AIR_BASE}/{AIR_TABLE}"
HEADERS      = {"Authorization": f"Bearer {AIR_KEY}", "Content-Type": "application/json"}

# ──────────────────────  HELPERS  ─────────────────────────────────────────

def download_csv(url: str) -> pd.DataFrame:
    log.info("📥 downloading CSV: %s", url)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    buf = io.StringIO(r.text)
    df  = pd.read_csv(buf)
    log.info("✅  downloaded %s rows", len(df))
    return df


# date parsing helper (assumes ISO or common formats)
def parse_date(val: Any) -> Optional[datetime]:
    if pd.isna(val):
        return None
    try:
        return pd.to_datetime(val, utc=True)
    except Exception:
        return None


# ─────────────  SCORING (recency ▸ job poster ▸ salary)  ──────────────────
MAX_AGE_DAYS = 14             # recency threshold for full points
RECENCY_WT   = 0.60
POSTER_WT    = 0.25
SALARY_WT    = 0.15

def score_row(row: pd.Series) -> float:
    # 1. recency
    post_date = parse_date(row.get("job_posted_date"))
    if post_date:
        age_days = (datetime.now(timezone.utc) - post_date).days
        recency_score = max(0, 1 - age_days / MAX_AGE_DAYS)
    else:
        recency_score = 0.0

    # 2. job poster present
    has_poster = bool(row.get("job_poster_name") or row.get("job_poster"))
    poster_score = 1.0 if has_poster else 0.0

    # 3. salary ≥ 140k
    sal = row.get("salary")
    try:
        high_end = float(str(sal).split("-")[-1].replace("$", "").replace(",", ""))
    except Exception:
        high_end = 0
    salary_score = 1.0 if high_end >= 140_000 else 0.0

    total = (
        RECENCY_WT * recency_score +
        POSTER_WT  * poster_score +
        SALARY_WT  * salary_score
    )
    return round(total * 10, 2)   # scale 0-10


# ───────────────────────  AIRTABLE  ───────────────────────────────────────
def chunk(records: List[Dict[str, Any]], n: int) -> Generator[List, None, None]:
    for i in range(0, len(records), n):
        yield records[i : i + n]

def airtable_upsert(records: List[Dict[str, Any]]) -> None:
    for batch in chunk(records, CHUNK_SIZE):
        payload = {"records": batch}
        tries = 0
        while tries < 3:
            resp = requests.post(AIR_ENDPOINT, headers=HEADERS, json=payload)
            if resp.ok:
                log.debug("🆙 airtable batch ok (%d rows)", len(batch))
                break
            tries += 1
            log.warning("⚠️ airtable error %s – retry %d/3", resp.text, tries)
            time.sleep(2)
        else:
            log.error("❌ failed to upsert batch after 3 retries")
            log.error(resp.text)


# ───────────────────────────  MAIN  ────────────────────────────────────────
def main() -> None:
    log.info("🚀 main.py booted – starting pipeline …")

    df = download_csv(BRIGHT_URL)

    # keep/rename only the columns we care about (adjust as needed)
    COL_MAP = {
        "job_title": "title",
        "company_name": "company",
        "job_posted_date": "posted",
        "job_poster": "poster",
        "salary": "salary",
        "job_location": "location",
        "url": "source_url",
    }
    df = df[list(COL_MAP.keys())].rename(columns=COL_MAP)

    df["score"] = df.apply(score_row, axis=1)

    # build Airtable payload
    at_records = [
        {"fields": row.dropna().to_dict()} for _, row in df.iterrows()
    ]
    log.info("📊 prepared %d records for Airtable", len(at_records))

    airtable_upsert(at_records)

    log.info("🎉 pipeline completed")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:           # catches anything we didn’t foresee
        log.exception("💥 Unhandled exception: %s", exc)
        sys.exit(1)
