#!/usr/bin/env python3
# ============================================================================
# main.py – robust date parsing + progress logging + GPT category filter
# ============================================================================

import os, time, logging
from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
from pyairtable import Table
from openai import OpenAI

# ─── Configuration (unchanged) ────────────────────────────────────────────────
AIRTABLE_TOKEN       = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID     = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME  = os.getenv("AIRTABLE_TABLE_NAME")

AWS_ACCESS_KEY       = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY       = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET           = os.getenv("AWS_BUCKET_NAME")
AWS_REGION           = os.getenv("AWS_REGION", "us-east-1")

S3_PREFIX            = os.getenv("S3_PREFIX", "")
FILE_EXT             = ".csv"

MAX_AGE_DAYS = 7
ALLOWED_CATEGORIES = [
    "AI Expertise", "Machine Learning", "Data Science", "Data Analytics",
    "Visualization", "Data Governance", "Engineering",
]

assert all([AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME,
            AWS_ACCESS_KEY,  AWS_SECRET_KEY,  AWS_BUCKET]), \
       "🔑 Missing required environment variables"

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── External clients ────────────────────────────────────────────────────────
client = OpenAI()
table  = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
s3     = boto3.client(
           "s3", region_name=AWS_REGION,
           aws_access_key_id=AWS_ACCESS_KEY,
           aws_secret_access_key=AWS_SECRET_KEY,
         )

# ─── Helper: robust date parse ───────────────────────────────────────────────
def parse_date(val: str):
    """
    Try ISO / RFC / epoch-seconds.  Return pd.Timestamp(UTC) or NaT.
    """
    if pd.isna(val) or not str(val).strip():
        return pd.NaT
    txt = str(val).strip()
    # epoch?
    if txt.isdigit():
        try:
            return pd.to_datetime(int(txt), unit="s", utc=True)
        except Exception:
            pass
    # fall back to pandas flexible parser
    return pd.to_datetime(txt, utc=True, errors="coerce")

def is_recent(val) -> bool:
    ts = parse_date(val)
    if ts is pd.NaT:
        return False
    return (datetime.now(timezone.utc) - ts) <= timedelta(days=MAX_AGE_DAYS)

# ─── GPT helpers (unchanged logic) ────────────────────────────────────────────
def gpt_in_scope(job: dict) -> bool:
    prompt = f"""Reply only "yes" or "no". Does this job fit ANY of these?
{", ".join(ALLOWED_CATEGORIES)}.

Title: {job.get('job_title')}
Summary: {job.get('job_summary')}
"""
    try:
        ans = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        ).choices[0].message.content.strip().lower()
        return ans.startswith("y")
    except Exception as e:
        logging.error("GPT filter error (keeping row): %s", e)
        return True

def score_job(job: dict) -> tuple[int, str]:
    prompt = f"""Rate 1-10 on relevance to: {", ".join(ALLOWED_CATEGORIES)}.
One line: "Score: X/10 — Reason".
Job title: {job.get('job_title')}
Company  : {job.get('company_name')}
Summary  : {job.get('job_summary') or job.get('job_description')}
"""
    try:
        txt = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
             ).choices[0].message.content.strip()
        score = int(txt.split("Score:")[1].split("/")[0].strip())
        return score, txt
    except Exception as e:
        logging.error("GPT score error: %s", e)
        return 0, f"Score: 0/10 — {e}"

def sanitize(v):
    return "" if (v is None or (isinstance(v, float) and pd.isna(v))) else v

# ─── Airtable push ────────────────────────────────────────────────────────────
def push_to_airtable(job, score, reason):
    try:
        table.create({
            **{k: sanitize(job.get(k)) for k in [
                "job_title","company_name","job_location","job_summary",
                "job_function","job_industries","job_base_pay_range","url",
                "job_posted_time","job_num_applicants"
            ]},
            "Score": score, "Reason": reason,
        })
        logging.info("✅ Airtable: added %s", job.get("job_title"))
    except Exception as e:
        logging.error("❌ Airtable error: %s", e)

# ─── S3 download (unchanged) ──────────────────────────────────────────────────
def latest_key():
    objs = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=S3_PREFIX).get("Contents", [])
    objs = [o for o in objs if o["Key"].endswith(FILE_EXT)]
    return max(objs, key=lambda o: o["LastModified"])["Key"] if objs else None

def download_csv() -> str | None:
    key = latest_key()
    if not key:
        logging.error("S3: no CSV found")
        return None
    local = os.path.basename(key)
    logging.info("📥 Downloading s3://%s/%s", AWS_BUCKET, key)
    s3.download_file(AWS_BUCKET, key, local)
    return local

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    logging.info("🚀 Starting job screener…")
    path = download_csv()
    if not path: return

    try:
        df = pd.read_csv(path).dropna(how="all")
    except Exception as e:
        logging.error("CSV read error: %s", e); return
    logging.info("🗃️  Loaded %d rows from CSV", len(df))

    # Age filter
    recent_mask = df["job_posted_time"].apply(is_recent) \
                   if "job_posted_time" in df.columns else pd.Series(True, index=df.index)
    df_recent = df[ recent_mask ]
    logging.info("⏳ After age filter: %d rows remain", len(df_recent))

    if df_recent.empty:
        logging.warning("All rows filtered out by age check – verify date format!")
        return

    # GPT category filter
    keep = []
    for row in df_recent.to_dict("records"):
        keep.append(gpt_in_scope(row))
        time.sleep(0.1)   # tiny pause to avoid burst QPS
    df_keep = df_recent[keep]
    logging.info("🏷️  After category filter: %d rows remain", len(df_keep))

    if df_keep.empty:
        logging.warning("All rows dropped by GPT category check.")
        return

    # Score & push
    for row in df_keep.to_dict("records"):
        s, r = score_job(row)
        logging.info("🧠 GPT score: %d/10", s)
        push_to_airtable(row, s, r)
        time.sleep(1)        # Airtable rate-limit guard

if __name__ == "__main__":
    main()
