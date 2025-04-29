#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py – cron-job entry-point
"""

import os
import sys
import logging
from datetime import datetime
from typing import List

import boto3
import pandas as pd

# ──────────────────────────────────────────────────────────────
# 1. logging setup
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# 2. required ENV-VARS
# ──────────────────────────────────────────────────────────────
REQUIRED_VARS: List[str] = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "S3_BUCKET",
    # airtable keys are only required if you want to push there
    # they'll be validated later if PUSH_TO_AIRTABLE is true
]

for var in REQUIRED_VARS:
    if not os.getenv(var):
        logger.error("Required env var %s missing", var)
        sys.exit(1)

# Optional flag – default False
PUSH_TO_AIRTABLE = os.getenv("PUSH_TO_AIRTABLE", "false").lower() == "true"

if PUSH_TO_AIRTABLE:
    for var in ("AIRTABLE_API_KEY", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_NAME"):
        if not os.getenv(var):
            logger.error(
                "PUSH_TO_AIRTABLE is true, but env var %s is missing", var
            )
            sys.exit(1)

# ──────────────────────────────────────────────────────────────
# 3. import your ranking function
#    (ranker.py must live in the same folder or be on PYTHONPATH)
# ──────────────────────────────────────────────────────────────
# ──────────────────────────────────────────────────────────────
# 4. helpers – S3 and Airtable
# ──────────────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    region_name=os.environ["AWS_REGION"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)


def read_df_from_s3(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def write_df_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    csv_bytes = df.to_csv(index=False).encode()
    s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes)
    logger.info("✔️  Wrote %d bytes to s3://%s/%s", len(csv_bytes), bucket, key)


def push_to_airtable(df: pd.DataFrame) -> None:
    """Stream rows to Airtable (create-or-update on 'job_id')."""
    from pyairtable import Table

    table = Table(
        os.environ["AIRTABLE_API_KEY"],
        os.environ["AIRTABLE_BASE_ID"],
        os.environ["AIRTABLE_TABLE_NAME"],
    )

    for _, row in df.iterrows():
        record = row.to_dict()
        # upsert using 'job_id' (or whatever primary key your table uses)
        job_id = record["job_id"]
        matches = table.all(formula=f"{{job_id}} = '{job_id}'")
        if matches:
            table.update(matches[0]["id"], record)
        else:
            table.create(record)
    logger.info("✔️  Pushed %s rows to Airtable", len(df))


# ──────────────────────────────────────────────────────────────
# 5. main pipeline
# ──────────────────────────────────────────────────────────────
RAW_KEY = "raw/jobs.csv"
RANKED_KEY = "ranked/jobs.csv"
BUCKET = os.environ["S3_BUCKET"]


def main() -> None:
    logger.info("🚀  Cron started")

    # ---------------------------------------------------------------------
    # step 1 – load raw jobs
    # ---------------------------------------------------------------------
    raw_df = read_df_from_s3(BUCKET, RAW_KEY)
    logger.info("Loaded %d raw jobs", len(raw_df))

    if raw_df.empty:
        logger.warning("No rows found in %s – exiting early", RAW_KEY)
        return

    # ---------------------------------------------------------------------
    # step 2 – rank
    # ---------------------------------------------------------------------
    logger.info("Ranking jobs …")
    ranked_df = rank_jobs(raw_df.copy())  # <-- your logic in ranker.py
    logger.info("Ranked %d jobs", len(ranked_df))

    # ---------------------------------------------------------------------
    # step 3 – write outputs
    # ---------------------------------------------------------------------
    write_df_to_s3(ranked_df, BUCKET, RANKED_KEY)

    if PUSH_TO_AIRTABLE:
        push_to_airtable(ranked_df)

    logger.info("✅  Pipeline finished successfully")


# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Cron crashed")
        raise
