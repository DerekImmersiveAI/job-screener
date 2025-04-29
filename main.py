#!/usr/bin/env python3
# ── main.py  –  BrightData → S3 → Airtable pipeline with ranking ────────────
import os, csv, time, sys, logging
from pathlib import Path
from typing import List, Dict, Any

import boto3, requests, backoff

# ────────── logging ──────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")

# ────────── env (fail–fast) ──────────
def env(name: str, *, optional=False, default=None):
    val = os.getenv(name, default)
    if val is None and not optional:
        log.error("Required env var %s missing", name)
        sys.exit(1)
    return val

AIRTABLE_BASE   = env("AIRTABLE_BASE")
AIRTABLE_TABLE  = env("AIRTABLE_TABLE")
AIRTABLE_TOKEN  = env("AIRTABLE_TOKEN")
OPENAI_API_KEY  = env("OPENAI_API_KEY")          # used by your ranker

BRIGHTDATA_URL  = os.getenv("BRIGHTDATA_URL") or os.getenv("CSV_URL")

# S3 details (only used when no URL override is given)
S3_BUCKET = env("S3_BUCKET", optional=bool(BRIGHTDATA_URL))
S3_PREFIX = os.getenv("S3_PREFIX", "")           # optional “folder/”
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# ────────── Airtable tiny client ──────────
AT_ENDPOINT = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
AT_HEADERS  = {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}

@backoff.on_exception(backoff.expo, requests.RequestException, max_tries=5)
def airtable_upsert(rows: List[Dict[str, Any]]) -> None:
    if not rows: return
    payload = {"records": [{"fields": r} for r in rows]}
    r = requests.post(AT_ENDPOINT, json=payload, headers=AT_HEADERS, timeout=30)
    if r.status_code >= 300:
        log.error("Airtable error %s: %s", r.status_code, r.text[:200])
        r.raise_for_status()
    log.info("🆙  sent %s rows to Airtable", len(rows))

# ────────── S3 helpers ──────────
def latest_s3_object(bucket: str, prefix: str) -> str:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    newest = None
    paginator = s3.get_paginator("list_objects_v2")
    for p in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in p.get("Contents", []):
            if not obj["Key"].endswith(".csv"):
                continue
            if newest is None or obj["LastModified"] > newest["LastModified"]:
                newest = obj
    if newest is None:
        raise RuntimeError(f"No *.csv objects under s3://{bucket}/{prefix or ''}")
    return newest["Key"]

def download_s3(key: str, dst: Path):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("wb") as fh:
        s3.download_fileobj(S3_BUCKET, key, fh)
    log.info("📥  downloaded s3://%s/%s → %s", S3_BUCKET, key, dst)

def download_http(url: str, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with dst.open("wb") as fh:
            for chunk in r.iter_content(8192):
                fh.write(chunk)
    log.info("📥  downloaded %s → %s", url, dst)

# ────────── ranking (your existing algorithm) ──────────
from ranker import rank_job           # <-- you already have this module

# ────────── pipeline ──────────
def main():
    t0 = time.time()
    tmp = Path("/tmp/work")
    csv_path = tmp / "brightdata_latest.csv"

    # 1️⃣  fetch the latest CSV
    if BRIGHTDATA_URL:
        download_http(BRIGHTDATA_URL, csv_path)
    else:
        key = latest_s3_object(S3_BUCKET, S3_PREFIX)
        download_s3(key, csv_path)

    # 2️⃣  process & push in small batches
    batch, BATCH_SZ = [], 10
    with csv_path.open(newline="", encoding="utf-8") as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            row["gpt_score"] = rank_job(row)
            batch.append(row)
            if len(batch) >= BATCH_SZ:
                airtable_upsert(batch)
                batch.clear()
        airtable_upsert(batch)   # leftovers

    log.info("✅  finished %s rows in %.1fs", rdr.line_num, time.time() - t0)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.exception("💥  pipeline crashed: %s", exc)
        sys.exit(1)
