#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────────────────────────
#  main.py – download newest BrightData CSV from S3, score & upsert to Airtable
# ──────────────────────────────────────────────────────────────────────────────

import os
import sys
import csv
import json
import time
import math
import boto3
import logging
import tempfile
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

# ───────────────────────── logging ──────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("job-screener")

# ─────────────────────── configuration ──────────────────────
AIRTABLE_BASE    = os.environ["AIRTABLE_BASE"]
AIRTABLE_TABLE   = os.environ["AIRTABLE_TABLE"]
AIRTABLE_TOKEN   = os.environ["AIRTABLE_TOKEN"]

OPENAI_API_KEY   = os.environ["OPENAI_API_KEY"]

# Optional overrides
BRIGHTDATA_URL   = os.getenv("BRIGHTDATA_URL") or os.getenv("CSV_URL")

# S3 settings
S3_BUCKET   = os.getenv("S3_BUCKET")        # *required* if BRIGHTDATA_URL not set
S3_PREFIX   = os.getenv("S3_PREFIX", "")    # optional “folder/”
AWS_REGION  = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# ──────────────────── helpers / utilities ───────────────────

def newest_s3_object(bucket: str, prefix: str = "") -> str:
    """Return the S3 URI (s3://bucket/key) of the newest *.csv object."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    most_recent = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            if (most_recent is None) or (obj["LastModified"] > most_recent["LastModified"]):
                most_recent = obj

    if not most_recent:
        raise RuntimeError(f"No CSV files found in s3://{bucket}/{prefix}")

    return f"s3://{bucket}/{most_recent['Key']}"

def download_s3_file(s3_uri: str, dest_path: Path) -> None:
    """Stream-download an S3 object to dest_path."""
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with dest_path.open("wb") as fh:
        s3.download_fileobj(bucket, key, fh)
    log.info("📥 downloaded %s → %s", key, dest_path)

def download_http_file(url: str, dest_path: Path) -> None:
    """Stream-download a large file over HTTP to dest_path."""
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with dest_path.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
    log.info("📥 downloaded %s → %s", url, dest_path)

def get_latest_csv(local_dir: Path) -> Path:
    """Return Path to latest CSV after downloading it (if needed)."""
    local_dir.mkdir(parents=True, exist_ok=True)
    dest = local_dir / "brightdata_latest.csv"

    if BRIGHTDATA_URL:                # explicit URL wins
        download_http_file(BRIGHTDATA_URL, dest)
    else:                             # auto-discover newest S3 object
        if not S3_BUCKET:
            raise RuntimeError("S3_BUCKET env var missing and no BRIGHTDATA_URL override provided.")
        s3_uri = newest_s3_object(S3_BUCKET, S3_PREFIX)
        log.info("🔍 newest S3 object: %s", s3_uri)
        download_s3_file(s3_uri, dest)

    return dest

# ───────────────────── GPT scoring stub ─────────────────────
def gpt_score(row: Dict[str, Any]) -> int:
    """
    Your existing advanced scoring logic goes here;
    returning an int 0-10.  (Stubbed to 3 for brevity.)
    """
    return 3

# ───────────────────── airtable uploader ────────────────────
import backoff, requests

AIRTABLE_ENDPOINT = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
AIRTABLE_HEADERS  = {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}

@backoff.on_exception(backoff.expo, requests.HTTPError, max_tries=5)
def airtable_batch_upsert(rows: List[Dict[str, Any]]) -> None:
    payload = {"records": [{"fields": r} for r in rows]}
    resp = requests.post(AIRTABLE_ENDPOINT, headers=AIRTABLE_HEADERS, json=payload, timeout=30)
    if resp.status_code >= 300:
        log.error("Airtable error: %s", resp.text)
        resp.raise_for_status()
    log.info("🆙 airtable batch ok (%s rows)", len(rows))

# ─────────────────────────  main  ───────────────────────────
def main():
    t0 = time.time()
    workdir = Path("/tmp/work")
    csv_path = get_latest_csv(workdir)

    # ── read & process ──
    batch, batch_size = [], 10
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            row["gpt_score"] = gpt_score(row)
            batch.append(row)

            if len(batch) == batch_size:
                airtable_batch_upsert(batch)
                batch.clear()

        if batch:
            airtable_batch_upsert(batch)

    elapsed = time.time() - t0
    log.info("🎉 pipeline completed in %.1fs", elapsed)

# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except KeyError as ke:
        env = str(ke).strip("'")
        log.error("Required environment variable %s not found. Set it in Render → Environment.", env)
        sys.exit(1)
    except Exception as exc:
        log.exception("Unhandled exception: %s", exc)
        sys.exit(1)
