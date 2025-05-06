#!/usr/bin/env python3
# ────────────────────────────────────────────────────────────────────────────────
# main.py – download latest Bright Data CSV from S3, score each job with GPT-4,
#           push in-scope jobs to Airtable AND tag with the correct account owner
# ────────────────────────────────────────────────────────────────────────────────
import os, time, logging
from datetime import datetime
import boto3, pandas as pd
from pyairtable import Table
from openai import OpenAI

# ─── Owner lookup ──────────────────────────────────────────────────────────────
# Normalise company names to lower-case, no punctuation/spaces you care about.
ACCOUNT_OWNER = {
    # ── Henry Hartmann ───────────────────────────────────────────────────────
    "directv":                "Henry Hartmann",
    "the walt disney company": "Henry Hartmann",
    "espn":                    "Henry Hartmann",
    "siriusxm":                "Henry Hartmann",
    "electronic arts":         "Henry Hartmann",
    "nbcuniversal":            "Henry Hartmann",
    "consumer cellular":       "Henry Hartmann",
    "us cellular":             "Henry Hartmann",
    "rockstar games":          "Henry Hartmann",
    "t-mobile":                "Henry Hartmann",
    "time warner":             "Henry Hartmann",
    "horizon media":           "Henry Hartmann",

    # ── Chris Vaughan ────────────────────────────────────────────────────────
    "cox automotive":          "Chris Vaughan",
    "macy’s":                  "Chris Vaughan",
    "macy's":                  "Chris Vaughan",   # simple alias example
    "estee lauder":            "Chris Vaughan",
    "altice usa":              "Chris Vaughan",
    "national hockey league":  "Chris Vaughan",
    "bse global":              "Chris Vaughan",
    "netflix":                 "Chris Vaughan",
    "charter/spectrum":        "Chris Vaughan",
    "spotify":                 "Chris Vaughan",
    "major league baseball":   "Chris Vaughan",

    # …KEEP ADDING FOR EVERY AE…

    # Example for “Unassigned” bucket – simply omit from dict; script will leave blank
}

def assign_owner(company: str | None) -> str | None:
    """
    Return the account-manager name for this company or None if unknown.
    """
    if not company or not isinstance(company, str):
        return None
    key = company.lower().strip()
    return ACCOUNT_OWNER.get(key)


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
client = OpenAI()
table  = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
s3     = boto3.client(
            "s3",
            region_name          = AWS_REGION,
            aws_access_key_id    = AWS_ACCESS_KEY,
            aws_secret_access_key= AWS_SECRET_KEY,
        )

# ─── Helpers ───────────────────────────────────────────────────────────────────
def fetch_latest_from_s3() -> str | None:
    """
    Download newest *.csv from S3 and return the local filename (or None).
    """
    try:
        resp     = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=S3_PREFIX)
        objects  = resp.get("Contents", [])
        csv_objs = [o for o in objects if o["Key"].endswith(FILE_EXT)]
        if not csv_objs:
            logging.error("S3: no %s files found in bucket %s/%s",
                          FILE_EXT, AWS_BUCKET, S3_PREFIX)
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


def is_allowed(row: dict) -> bool:
    """
    Quick filter – only keep rows whose function/title mention our allowed themes.
    """
    ALLOWED = {
        "machine learning", "data science", "data analytics", "analytics",
        "visualization", "data governance", "engineering", "product management",
    }

    raw = row.get("job_function") or row.get("job_title") or ""
    text = str(raw).lower()                 # <— always a string now
    return any(k in text for k in ALLOWED)


def score_job(job: dict) -> tuple[int, str]:
    """
    Ask GPT-4 to rate the job. Returns (score, full-text reason).
    """
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1-10 based on:
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

Respond in this exact format:
Score: X/10
Reason: [short reason]
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",            # cheaper/faster; switch if you like
            messages=[{"role":"user","content":prompt}],
            temperature=0.2,
        )
        content    = resp.choices[0].message.content.strip()
        score_line = next((ln for ln in content.splitlines() if "Score" in ln), "Score: 0/10")
        score      = int(score_line.split(":")[1].split("/")[0].strip())
        return score, content

    except Exception as exc:
        logging.error("OpenAI error: %s", exc)
        return 0, f"Score: 0/10\nReason: OpenAI error: {exc}"


def sanitize(value):
    """Convert NaNs / None → None, truncate long strings for Airtable limits."""
    if pd.isna(value):
        return None
    if isinstance(value, float) and value != value:  # NaN check
        return None
    if isinstance(value, str) and len(value) > 10000:
        return value[:10000]
    return value


def push_to_airtable(job: dict, score: int, reason: str) -> None:
    """
    Send a single record to Airtable (adds blank Account Manager when unknown).
    """
    try:
        company = sanitize(job.get("company_name"))
        fields  = {
            "job_title"        : sanitize(job.get("job_title")),
            "company_name"     : company,
            "job_location"     : sanitize(job.get("job_location")),
            "job_summary"      : sanitize(job.get("job_summary")),
            "job_function"     : sanitize(job.get("job_function")),
            "job_industries"   : sanitize(job.get("job_industries")),
            "job_base_pay_range": sanitize(job.get("job_base_pay_range")),
            "url"              : sanitize(job.get("url")),
            "job_posted_time"  : sanitize(job.get("job_posted_time")),
            "job_num_applicants": sanitize(job.get("job_num_applicants")),
            "Score"            : score,
            "Reason"           : reason,
            "Account Manager"  : assign_owner(company),   # ← new field
        }

        poster = sanitize(job.get("job_poster"))
        if poster is not None:
            fields["job_poster"] = poster

        table.create(fields)
        logging.info("✅ Airtable: added %s @ %s [owner: %s]",
                     fields["job_title"], company, fields["Account Manager"] or "—")

    except Exception as exc:
        logging.error("❌ Airtable error: %s", exc)


# ─── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    logging.info("🚀 Starting job screener...")
    path = fetch_latest_from_s3()
    if not path:
        logging.error("🚨 No file retrieved from S3.")
        return

    try:
        df = pd.read_csv(path, keep_default_na=True)
    except Exception as exc:
        logging.error("CSV read error: %s", exc)
        return

    # basic cleaning
    df = df.dropna(how="all")
    df = df.dropna(subset=["job_title", "company_name"])

    logging.info("📊 Loaded %d rows", len(df))

    for job in df.to_dict("records"):
        if not is_allowed(job):
            logging.info("🛈 Skipped (out-of-scope): %s – %s",
                         job.get("job_title"), job.get("company_name"))
            continue

        score, reason = score_job(job)
        logging.info("🧠 GPT scored %d/10", score)
        push_to_airtable(job, score, reason)
        time.sleep(1)   # Airtable rate-limit kindness


if __name__ == "__main__":
    main()
