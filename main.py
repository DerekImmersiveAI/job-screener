#!/usr/bin/env python3
# main.py – S3 ➜ CSV ➜ filter (<7 days old + category keywords) ➜ GPT score ➜ Airtable
# ────────────────────────────────────────────────────────────────────────────────
import os, time, logging, re
from datetime import datetime, timezone, timedelta

import boto3, pandas as pd
from pyairtable import Table
from openai import OpenAI

# ─── Category buckets ──────────────────────────────────────────────────────────
ALLOWED_CATEGORIES: dict[str, list[str]] = {
    "AI Expertise": [
        "rag", "prompt engineer", "llm", "generative ai", "nlp",
        "contextual understanding", "conversational analytics"
    ],
    "Machine Learning": [
        "gan", "reinforcement", "rnn", "cnn", "transformer", "hyperparameter",
        "model tuning"
    ],
    "Data Science": [
        "data scientist", "feature engineering", "regression",
        "clustering", "unsupervised", "semi-supervised"
    ],
    "Data Analytics": [
        "statistical analysis", "exploratory data analysis", "eda",
        "predictive modelling", "a/b testing", "hypothesis testing",
        "segmentation", "rfm"
    ],
    "Visualization": ["tableau", "power bi", "looker", "domo", "data viz"],
    "Data Governance": ["data governance", "data security", "data privacy"],
    "Engineering": [
        "machine learning engineer", "data engineer", "site reliability",
        "sre", "devops", "full stack", "backend engineer", "frontend engineer",
        "software engineer", "cloud engineer", "api engineer", "systems engineer",
        "blockchain engineer", "etl", "big data"
    ],
}
KEYWORD_PATTERN = re.compile(
    r"|".join(re.escape(k) for kws in ALLOWED_CATEGORIES.values() for k in kws),
    flags=re.I,
)

# ─── Age limit ─────────────────────────────────────────────────────────────────
MAX_AGE_DAYS = 7

# ─── Env / config ──────────────────────────────────────────────────────────────
AIRTABLE_TOKEN       = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID     = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME  = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY       = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY       = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET           = os.getenv("AWS_BUCKET_NAME")
AWS_REGION           = os.getenv("AWS_REGION", "us-east-1")
S3_PREFIX            = os.getenv("S3_PREFIX", "")
FILE_EXT             = ".csv"

assert all([AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME,
            AWS_ACCESS_KEY,  AWS_SECRET_KEY,  AWS_BUCKET]), \
       "🔑 Missing required environment variables!"

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
    region_name           = AWS_REGION,
    aws_access_key_id     = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
)

# ─── Helpers ───────────────────────────────────────────────────────────────────
def fetch_latest_from_s3() -> str | None:
    try:
        resp     = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=S3_PREFIX)
        csv_objs = [o for o in resp.get("Contents", []) if o["Key"].endswith(FILE_EXT)]
        if not csv_objs:
            logging.error("S3: no %s files found in bucket %s/%s", FILE_EXT, AWS_BUCKET, S3_PREFIX)
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


def job_in_scope(row: pd.Series) -> bool:
    """True if the job matches one of our categories *and* is recent enough."""
    # 1 — age filter
    posted = pd.to_datetime(row.get("job_posted_time"), utc=True, errors="coerce")
    if posted is pd.NaT or (datetime.now(timezone.utc) - posted) > timedelta(days=MAX_AGE_DAYS):
        return False
    # 2 — keyword filter
    haystack = " ".join(
        str(row.get(field, "")).lower()
        for field in ("job_title", "job_summary", "job_description")
    )
    return bool(KEYWORD_PATTERN.search(haystack))


def score_job(job: dict) -> tuple[int, str]:
    prompt = f"""
You are an AI job screener.  Rate this job from 1-10 based on:

• **Relevance to any of the following buckets** → {", ".join(ALLOWED_CATEGORIES.keys())}
• Job posted date (more recent ≫ better; everything is ≤ {MAX_AGE_DAYS} days)
• Seniority (prefer senior / lead / principal)
• Remote option (prefer fully-remote or hybrid)
• Salary (prefer ≳ $140 k)

Job Title: {job.get('job_title')}
Company: {job.get('company_name')}
Summary: {job.get('job_summary')}
Location: {job.get('job_location')}
Salary: {job.get('base_salary')}
Posted:  {job.get('job_posted_time')}
Description: {job.get('job_description')}

Respond **exactly** in this format:
Score: X/10
Reason: <one short sentence>
"""
    try:
        resp       = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        content    = resp.choices[0].message.content.strip()
        score_line = next((l for l in content.splitlines() if "Score" in l), "Score: 0/10")
        score      = int(score_line.split(":")[1].split("/")[0].strip())
        return score, content
    except Exception as exc:
        logging.error("OpenAI error: %s", exc)
        return 0, f"Score: 0/10\nReason: OpenAI error: {exc}"


def sanitize(v):
    if pd.isna(v) or (isinstance(v, float) and v != v):
        return None
    if isinstance(v, str) and len(v) > 10_000:
        return v[:10_000]
    return v


def push_to_airtable(job: dict, score: int, reason: str) -> None:
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
        poster = sanitize(job.get("job_poster"))
        if poster:
            fields["job_poster"] = poster
        table.create(fields)
        logging.info("✅ Airtable: added %s at %s", fields['job_title'], fields['company_name'])
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

    # clean + filter
    df = df.dropna(how="all")
    df = df.dropna(subset=["job_title", "company_name"])
    df = df[df.apply(job_in_scope, axis=1)]
    logging.info("📊 After filtering, %d rows remain", len(df))

    for job in df.to_dict("records"):
        score, reason = score_job(job)
        logging.info("🧠 GPT score: %d/10", score)
        push_to_airtable(job, score, reason)
        time.sleep(1)        # Airtable rate-limit buffer


if __name__ == "__main__":
    main()
