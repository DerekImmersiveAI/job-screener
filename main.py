#!/usr/bin/env python3
"""
main.py – cron-entry for Render.
Scrapes / loads raw jobs, ranks them, pushes to S3 + Airtable.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import boto3          # make sure boto3 is in requirements.txt
import pandas as pd
import requests
from airtable import Airtable   # pip install airtable-python-wrapper

# --------------------------------------------------------------------------------------
# 1. configure logging
# --------------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cron")

# --------------------------------------------------------------------------------------
# 2. required environment variables
# --------------------------------------------------------------------------------------
REQUIRED_ENV_VARS = [
    "AIRTABLE_BASE",
    "AIRTABLE_TABLE_NAME",
    "AIRTABLE_TOKEN",
    "S3_BUCKET",
]

missing = [k for k in REQUIRED_ENV_VARS if not os.getenv(k)]
if missing:
    log.error("Required env var%s %s missing",
              "" if len(missing) == 1 else "s",
              ", ".join(missing))
    sys.exit(1)

AIRTABLE_BASE        = os.environ["AIRTABLE_BASE"]
AIRTABLE_TABLE_NAME  = os.environ["AIRTABLE_TABLE_NAME"]
AIRTABLE_TOKEN       = os.environ["AIRTABLE_TOKEN"]
S3_BUCKET            = os.environ["S3_BUCKET"]

# optional – region / prefix
AWS_REGION           = os.getenv("AWS_REGION", "us-east-1")
S3_KEY_PREFIX        = os.getenv("S3_KEY_PREFIX", "ranked-jobs")

# --------------------------------------------------------------------------------------
# 4. helper: upload DataFrame to S3
# --------------------------------------------------------------------------------------
def upload_df_to_s3(df: pd.DataFrame, bucket: str, key: str) -> str:
    """Write df to CSV in-memory and upload to S3 — returns the s3:// URL."""
    import io

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    url = f"s3://{bucket}/{key}"
    log.info("✅  Uploaded %d rows to %s", len(df), url)
    return url


# --------------------------------------------------------------------------------------
# 5. helper: upsert to Airtable
# --------------------------------------------------------------------------------------
def upsert_to_airtable(df: pd.DataFrame) -> None:
    """Upsert DataFrame rows into Airtable (simple wrapper)."""
    airtable = Airtable(AIRTABLE_BASE, AIRTABLE_TABLE_NAME, AIRTABLE_TOKEN)

    for _, row in df.iterrows():
        # Assumes there's a unique 'id' column; adjust as needed.
        record_id = row["id"]
        fields = row.dropna().to_dict()

        # Try update first, otherwise create :
        existing = airtable.match("id", record_id)
        if existing:
            airtable.update(existing["id"], fields)
        else:
            airtable.insert(fields)

    log.info("✅  Upserted %d rows into Airtable table '%s'", len(df), AIRTABLE_TABLE_NAME)


# --------------------------------------------------------------------------------------
# 6. your scraping / data-loading logic
# --------------------------------------------------------------------------------------
def load_raw_jobs() -> pd.DataFrame:
    """
    RETURN a DataFrame of raw jobs you want to rank.
    Replace this stub with the real scraping / DB load work.
    """
    # --- TODO: replace with real source -------------------------------------
    sample = [
        {"id": "1", "title": "Data Scientist",   "salary": 145000},
        {"id": "2", "title": "ML Engineer",      "salary": 160000},
        {"id": "3", "title": "BI Analyst",       "salary": 105000},
    ]
    # ------------------------------------------------------------------------
    df = pd.DataFrame(sample)
    log.info("Loaded %d raw jobs", len(df))
    return df


# --------------------------------------------------------------------------------------
# 7. main driver
# --------------------------------------------------------------------------------------
def main() -> None:
    log.info("🚀  Cron started")

    # 1. load / scrape
    raw_df = load_raw_jobs()
    if raw_df.empty:
        log.warning("No jobs found – nothing to do")
        return

    # 2. rank
    ranked_df = rank_jobs(raw_df.copy())   # your logic in ranker.py
    log.info("Top job after ranking: %s", ranked_df.iloc[0].to_dict())

    # 3. push to S3
    date_tag = datetime.utcnow().strftime("%Y-%m-%d")
    s3_key = f"{S3_KEY_PREFIX}/{date_tag}/ranked_jobs.csv"
    upload_df_to_s3(ranked_df, S3_BUCKET, s3_key)

    # 4. upsert into Airtable
    upsert_to_airtable(ranked_df)

    log.info("🏁  Cron finished successfully")


# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.exception("❌  Cron crashed: %s", exc)
        # non-zero exit so Render marks the cron run as failed
        sys.exit(1)
