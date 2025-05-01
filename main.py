#!/usr/bin/env python3
import os
import csv
import logging
from pathlib import Path
from datetime import datetime, timezone

import boto3             # pip install boto3
import pandas as pd
from pyairtable import Api
from pyairtable.formulas import match

# -------------------------------
# CONFIG (env vars or hard-code)
# -------------------------------
AWS_REGION         = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET          = os.getenv("S3_BUCKET",  "brightdata-job-screener")
S3_PREFIX          = os.getenv("S3_PREFIX",  "")           # e.g. "exports/"
AIRTABLE_TOKEN     = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID   = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Jobs")

CATEGORIES = {
    "ai expertise", "machine learning", "data science",
    "data analytics", "visualization", "data governance",
    "engineering"
}
# lower-cased for case-insensitive matching
# -------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------- S3 HELPERS ---------- #
def latest_csv_from_s3(bucket: str, prefix: str = "") -> str:
    """Return S3 URI (s3://bucket/key) of the latest .csv file under prefix."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    newest = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".csv"):
                continue
            if (newest is None) or (obj["LastModified"] > newest["LastModified"]):
                newest = obj

    if newest is None:
        raise RuntimeError(f"No CSV files found in s3://{bucket}/{prefix}")

    key = newest["Key"]
    logging.info("📄 Latest CSV detected: s3://%s/%s (modified %s)",
                 bucket, key, newest["LastModified"])
    return f"s3://{bucket}/{key}"


def download_s3_uri(s3_uri: str, dest: Path) -> Path:
    """Download the given s3:// URI to dest and return dest."""
    if not s3_uri.startswith("s3://"):
        raise ValueError("download_s3_uri expects an s3:// URI")

    bucket, key = s3_uri[5:].split("/", 1)
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.download_file(bucket, key, str(dest))
    logging.info("⬇️  Downloaded %s to %s", s3_uri, dest)
    return dest

# ---------- FILTER / RANK ---------- #
def relevant(row: pd.Series) -> bool:
    """Return True if the job row belongs to a desired category."""
    title = str(row.get("job_title", "")).lower()
    category = str(row.get("job_category", "")).lower()
    combined = f"{title} {category}"
    return any(term in combined for term in CATEGORIES)

def rank(df: pd.DataFrame) -> pd.DataFrame:
    """Simple relevancy rank: # of category keywords appearing in title+category."""
    def score_row(row):
        text = f"{row.get('job_title','')} {row.get('job_category','')}".lower()
        return sum(term in text for term in CATEGORIES)

    df["score"] = df.apply(score_row, axis=1)
    return df.sort_values("score", ascending=False)

# ---------- MAIN ---------- #
def main():
    logging.info("🚀 Starting job screener…")

    # 1. Find latest CSV and download
    latest_uri = latest_csv_from_s3(S3_BUCKET, S3_PREFIX)
    tmp_csv = download_s3_uri(latest_uri, Path("/tmp/jobs.csv"))

    # 2. Load
    df = pd.read_csv(tmp_csv)
    logging.info("📥 Loaded %d rows from CSV", len(df))

    # 3. Filter
    df = df[df.apply(relevant, axis=1)]
    logging.info("🔍 After category filter: %d rows remain", len(df))

    if df.empty:
        logging.warning("No matching rows – exiting.")
        return

    df = rank(df)

    # 4. Push to Airtable
    table = Api(AIRTABLE_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

    for _, row in df.iterrows():
        record = {
            "Job Title": row.get("job_title"),
            "Company": row.get("company_name"),
            "Location": row.get("job_location"),
            "Posted": row.get("job_posted_date"),
            "Source": row.get("apply_link"),
            "Relevancy Score": int(row["score"]),
        }
        table.create(record)
        logging.info("✅ Airtable: added %s", record["Job Title"])

if __name__ == "__main__":
    main()
