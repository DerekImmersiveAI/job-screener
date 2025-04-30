#!/usr/bin/env python3
# ────────────────────────────────────────────────────────────────────────────────
# main.py – download newest jobs CSV from S3, score each job with GPT-4o,
#           push scored jobs to Airtable (NaN-safe).
# ────────────────────────────────────────────────────────────────────────────────
import os
import time
import json
import logging
from datetime import datetime
from math import isnan

import boto3
import numpy as np
import pandas as pd
from pyairtable import Table
from openai import OpenAI

# ─── Environment / configuration ───────────────────────────────────────────────
AIRTABLE_TOKEN       = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID     = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME  = os.getenv("AIRTABLE_TABLE_NAME")

AWS_ACCESS_KEY       = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY       = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET           = os.getenv("AWS_BUCKET_NAME")
AWS_REGION           = os.getenv("AWS_REGION", "us-east-1")

S3_PREFIX            = os.getenv("S3_PREFIX", "")           # e.g. "incoming/"
FILE_EXT             = ".csv"

assert all([AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME,
            AWS_ACCESS_KEY,  AWS_SECRET_KEY,  AWS_BUCKET]), \
       "🔑 One or more required environment variables are missing!"

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)

# ─── External clients ──────────────────────────────────────────────────────────
client = OpenAI()                     # OpenAI
table  = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)  # Airtable

s3 = boto3.client(
    "s3",
    region_name           = AWS_REGION,
    aws_access_key_id     = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
)

# ─── Helpers ───────────────────────────────────────────────────────────────────
def _safe(val):
    """
    Convert values that JSON (and Airtable) reject ­– NaN, numpy scalars, pd.NA –
    into None or plain Python types.
    """
    if val is None:
        return None

    # float('nan'), numpy.nan
    if isinstance(val, float) and isnan(val):
        return None

    # pandas NA / numpy scalar → None / native python scalar
    if val is pd.NA:
        return None
    if isinstance(val, np.generic):
        return val.item()

    # list / tuple: recursively sanitise
    if isinstance(val, (list, tuple)):
        return [_safe(v) for v in val]

    return val


def fetch_latest_from_s3() -> str | None:
    """Download the newest *.csv file from S3 and return the local filename."""
    try:
        resp     = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=S3_PREFIX)
        objects  = resp.get("Contents", [])
        csv_objs = [o for o in objects if o["Key"].endswith(FILE_EXT)]
        if not csv_objs:
            logging.error("S3: no %s files found in %s/%s", FILE_EXT, AWS_BUCKET, S3_PREFIX)
            return None

        latest = max(csv_objs, key=lambda o: o["LastModified"])
        key    = latest["Key"]
        local  = os.path.basename(key)
        logging.info("📥 Downloading s3://%s/%s", AWS_BUCKET, key)
        s3.download_file(AWS_BUCKET, key, local)
        return local

    except Exception as exc:
        logging.error("S3 download error: %s", exc)
        return None


def score_job(job: dict) -> tuple[int, str]:
    """Ask GPT-4o-mini to rate the job and return (score, reason)."""
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
• Role relevance to "Data Science"
• Seniority (prefer senior roles)
• Remote work option
• Salary (prefer $140k+)

Job Title: {job.get('job_title')}
Company: {job.get('company_name')}
Summary: {job.get('job_summary')}
Location: {job.get('job_location')}
Salary: {job.get('base_salary')}
Description: {job.get('job_description')}

Respond in this format:
Score: X/10
Reason: [short reason]
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        content    = resp.choices[0].message.content.strip()
        score_line = next((ln for ln in content.splitlines() if "Score" in ln), "Score: 0/10")
        score      = int(score_line.split(":")[1].split("/")[0].strip())
        return score, content

    except Exception as exc:
        logging.error("OpenAI error: %s", exc)
        return 0, f"Score: 0/10\nReason: OpenAI error: {exc}"


def push_to_airtable(job: dict, score: int, reason: str) -> None:
    """Send one record to Airtable, with NaN/None sanitisation."""
    try:
        fields = {
            "job_title"         : _safe(job.get("job_title")),
            "company_name"      : _safe(job.get("company_name")),
            "job_location"      : _safe(job.get("job_location")),
            "job_summary"       : _safe(job.get("job_summary")),
            "job_function"      : _safe(job.get("job_function")),
            "job_industries"    : _safe(job.get("job_industries")),
            "job_base_pay_range": _safe(job.get("job_base_pay_range")),
            "url"               : _safe(job.get("url")),
            "job_posted_time"   : _safe(job.get("job_posted_time")),
            "job_num_applicants": _safe(job.get("job_num_applicants")),
            "Score"             : score,
            "Reason"            : reason,
        }

        poster = job.get("job_poster")
        if isinstance(poster, str) and 0 < len(poster) <= 255:
            fields["job_poster"] = poster.strip()

        table.create(fields)
        logging.info("✅ Airtable: added %s at %s", job.get("job_title"), job.get("company_name"))

    except Exception as exc:
        logging.error("❌ Airtable error: %s", exc)


# ─── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    logging.info("🚀 Starting job screener...")
    path = fetch_latest_from_s3()
    if not path:
        logging.error("🚨 Job screener failed: no file retrieved from S3.")
        return

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        logging.error("CSV read error: %s", exc)
        return

    logging.info("📊 Loaded %d rows from CSV", len(df))

    for job in df.to_dict("records"):
        score, reason = score_job(job)
        logging.info("🧠 GPT score: %d/10", score)
        push_to_airtable(job, score, reason)
        time.sleep(1)          # stay within Airtable rate limits


if __name__ == "__main__":
    main()
