#!/usr/bin/env python3
# ────────────────────────────────────────────────────────────────────────────────
# main.py – download the newest jobs file from S3, keep only rows
#           (a) ≤ 7 days old  AND  (b) GPT says match one of our categories,
#           ask GPT to score each kept row on relevance, then push to Airtable.
# ────────────────────────────────────────────────────────────────────────────────
import os
import time
import json
import logging
from datetime import datetime, timezone, timedelta

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
AWS_REGION           = os.getenv("AWS_REGION", "us-east-1")

S3_PREFIX            = os.getenv("S3_PREFIX", "")      # e.g. "incoming/"
FILE_EXT             = ".csv"

assert all([AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME,
            AWS_ACCESS_KEY,  AWS_SECRET_KEY,  AWS_BUCKET]), \
       "🔑 One or more required environment variables are missing!"

MAX_AGE_DAYS = 7

ALLOWED_CATEGORIES = [
    "AI Expertise",
    "Machine Learning",
    "Data Science",
    "Data Analytics",
    "Visualization",
    "Data Governance",
    "Engineering",
]

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)

# ─── External clients ──────────────────────────────────────────────────────────
client = OpenAI()
table  = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

s3 = boto3.client(
    "s3",
    region_name           = AWS_REGION,
    aws_access_key_id     = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
)

# ─── Helpers ───────────────────────────────────────────────────────────────────
def fetch_latest_from_s3() -> str | None:
    """Download the newest *.csv file from S3 and return the local filename."""
    try:
        resp   = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=S3_PREFIX)
        objs   = [o for o in resp.get("Contents", []) if o["Key"].endswith(FILE_EXT)]
        if not objs:
            logging.error("S3: no %s files found in bucket %s/%s", FILE_EXT, AWS_BUCKET, S3_PREFIX)
            return None

        latest = max(objs, key=lambda o: o["LastModified"])
        key    = latest["Key"]
        local  = os.path.basename(key)
        logging.info("📥 Downloading s3://%s/%s", AWS_BUCKET, key)
        s3.download_file(AWS_BUCKET, key, local)
        return local
    except Exception as exc:
        logging.error("S3 download error: %s", exc)
        return None


def is_recent(posted_str: str) -> bool:
    """True if posted date (ISO-ish string) ≤ MAX_AGE_DAYS."""
    try:
        posted = pd.to_datetime(posted_str, utc=True)
    except Exception:
        return False
    if posted is pd.NaT:
        return False
    return (datetime.now(timezone.utc) - posted) <= timedelta(days=MAX_AGE_DAYS)


def gpt_in_scope(job: dict) -> bool:
    """
    Ask GPT with a cheap yes/no question: does this job belong to one of
    our allowed categories?  Returns True/False.
    """
    prompt = f"""Reply with exactly "yes" or "no".
Does this job clearly belong to ANY of these categories?
{", ".join(ALLOWED_CATEGORIES)}.

Title: {job.get('job_title')}
Summary: {job.get('job_summary')}
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        answer = resp.choices[0].message.content.strip().lower()
        return answer.startswith("y")
    except Exception as exc:
        logging.error("GPT filter error (keeping row just in case): %s", exc)
        return True   # fail-open so we don't accidentally drop good rows


def score_job(job: dict) -> tuple[int, str]:
    """
    Ask GPT to rate relevance (1-10) to our categories and give a short reason.
    """
    prompt = f"""
You are ranking jobs for a candidate interested in the following categories:
{", ".join(ALLOWED_CATEGORIES)}.

Give ONE line exactly in this format:
Score: X/10 — Reason

Higher score = stronger match. 0 = not related at all.
Job title: {job.get('job_title')}
Company: {job.get('company_name')}
Description: {job.get('job_summary') or job.get('job_description')}
"""
    try:
        resp     = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        content  = resp.choices[0].message.content.strip()
        score    = int(content.split("Score:")[1].split("/")[0].strip())
        return score, content
    except Exception as exc:
        logging.error("OpenAI score error: %s", exc)
        return 0, f"Score: 0/10 — OpenAI error: {exc}"


def sanitize(val):
    """Convert NaNs/None to empty string so Airtable JSON is valid."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return val


def push_to_airtable(job: dict, score: int, reason: str) -> None:
    """Create a record in Airtable."""
    try:
        fields = {
            "job_title"         : sanitize(job.get("job_title")),
            "company_name"      : sanitize(job.get("company_name")),
            "job_location"      : sanitize(job.get("job_location")),
            "job_summary"       : sanitize(job.get("job_summary")),
            "job_function"      : sanitize(job.get("job_function")),
            "job_industries"    : sanitize(job.get("job_industries")),
            "job_base_pay_range": sanitize(job.get("job_base_pay_range")),
            "url"               : sanitize(job.get("url")),
            "job_posted_time"   : sanitize(job.get("job_posted_time")),
            "job_num_applicants": sanitize(job.get("job_num_applicants")),
            "Score"             : score,
            "Reason"            : reason,
        }
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
        return

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        logging.error("CSV read error: %s", exc)
        return

    # drop rows that are completely empty to avoid NaN spam
    df = df.dropna(how="all")
    logging.info("🗃️  Loaded %d rows from CSV", len(df))

    # pass 1 – age + GPT category filter
    keep_mask = []
    for row in df.to_dict("records"):
        if is_recent(row.get("job_posted_time", "")) and gpt_in_scope(row):
            keep_mask.append(True)
        else:
            keep_mask.append(False)

    df = df[keep_mask]
    logging.info("📊 After filtering, %d rows remain", len(df))
    if df.empty:
        return

    # iterate kept rows → score → Airtable
    for job in df.to_dict("records"):
        score, reason = score_job(job)
        logging.info("🧠 GPT score: %d/10", score)
        push_to_airtable(job, score, reason)
        time.sleep(1)     # Airtable rate-limit guard


if __name__ == "__main__":
    main()
