#!/usr/bin/env python3
# ────────────────────────────────────────────────────────────────────────────────
# main.py – download the newest jobs file from S3, score each job with GPT-4,
#            then push the results to Airtable.
# ────────────────────────────────────────────────────────────────────────────────
import os
import time
import json
import logging
from datetime import datetime

import boto3
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
AWS_REGION           = os.getenv("AWS_REGION", "us-east-1")          # default

S3_PREFIX            = os.getenv("S3_PREFIX", "")  # e.g. "incoming/" (can be "")
FILE_EXT             = ".csv"                      # we store CSV files in S3

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
    region_name      = AWS_REGION,
    aws_access_key_id= AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
)

# ─── Helpers ───────────────────────────────────────────────────────────────────
def fetch_latest_from_s3() -> str | None:
    """
    Download the newest *.csv file from S3 and return the local filename,
    or None if nothing was found / downloaded.
    """
    try:
        resp      = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=S3_PREFIX)
        objects   = resp.get("Contents", [])
        csv_objs  = [o for o in objects if o["Key"].endswith(FILE_EXT)]
        if not csv_objs:
            logging.error("S3: no %s files found in bucket %s/%s",
                          FILE_EXT, AWS_BUCKET, S3_PREFIX)
            return None

        latest = max(csv_objs, key=lambda o: o["LastModified"])
        key    = latest["Key"]
        local  = os.path.basename(key)      # e.g. jobs_2025-04-29.csv
        logging.info("📥 Downloading s3://%s/%s", AWS_BUCKET, key)
        s3.download_file(AWS_BUCKET, key, local)
        return local

    except Exception as exc:
        logging.error("S3 download error: %s", exc)
        return None


def score_job(job: dict) -> tuple[int, str]:
    """
    Ask GPT-4 to rate the job.  Returns (score, full-text-reason).
    """
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
            model="gpt-4o-mini",        # or "gpt-4o" / "gpt-4-turbo" if you prefer
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
    """
    Send a single record to Airtable.
    """
    try:
        fields = {
            "job_title"        : job.get("job_title"),
            "company_name"     : job.get("company_name"),
            "job_location"     : job.get("job_location"),
            "job_summary"      : job.get("job_summary"),
            "job_function"     : job.get("job_function"),
            "job_industries"   : job.get("job_industries"),
            "job_base_pay_range": job.get("job_base_pay_range"),
            "url"              : job.get("url"),
            "job_posted_time"  : job.get("job_posted_time"),
            "job_num_applicants": job.get("job_num_applicants"),
            "Score"            : score,
            "Reason"           : reason,
        }

        # Airtable text fields have a 255-char limit
        poster = job.get("job_poster")
        if isinstance(poster, str) and 0 < len(poster) <= 255:
            fields["job_poster"] = poster

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

    # Read CSV to DataFrame
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        logging.error("CSV read error: %s", exc)
        return

    logging.info("📊 Loaded %d rows from CSV", len(df))

    # Iterate rows → GPT score → Airtable
    for job in df.to_dict("records"):
        score, reason = score_job(job)
        logging.info("🧠 GPT score: %d/10", score)
        push_to_airtable(job, score, reason)
        time.sleep(1)          # be nice to Airtable API limits


if __name__ == "__main__":
    main()
