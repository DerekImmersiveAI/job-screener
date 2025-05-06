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

# ─── Account-owner lookup ──────────────────────────────────────────────────────
# We normalise both the dictionary keys **and** the incoming company names:
#   • all lower-case
#   • no accents / fancy apostrophes
#   • minimal punctuation / spaces trimmed
# Feel free to add more aliases later – just keep them lower-case.

OWNER_LOOKUP: dict[str, str] = {
    # ── Henry Hartmann – Entertainment, Media, Gaming & Telecom ─────────────
    "directv": "Henry Hartmann",
    "the walt disney company": "Henry Hartmann", "disney company": "Henry Hartmann",
    "espn": "Henry Hartmann",
    "siriusxm": "Henry Hartmann", "sirius xm": "Henry Hartmann",
    "electronic arts": "Henry Hartmann", "ea": "Henry Hartmann",
    "nbcuniversal": "Henry Hartmann", "nbc universal": "Henry Hartmann",
    "consumer cellular": "Henry Hartmann",
    "us cellular": "Henry Hartmann",
    "rockstar games": "Henry Hartmann",
    "t-mobile": "Henry Hartmann", "t mobile": "Henry Hartmann",
    "time warner": "Henry Hartmann",
    "horizon media": "Henry Hartmann",
    "zynga": "Henry Hartmann",
    "marketing management analytics": "Henry Hartmann", "mma": "Henry Hartmann",
    "ogilvy": "Henry Hartmann",
    "rush street interactive": "Henry Hartmann",
    "e w scripps": "Henry Hartmann", "scripps": "Henry Hartmann",
    "madison square garden": "Henry Hartmann", "msg entertainment": "Henry Hartmann",
    "quad": "Henry Hartmann",
    "nielsen": "Henry Hartmann",
    "1-800-flowers": "Henry Hartmann", "1 800 flowers": "Henry Hartmann",

    # ── Chris Vaughan – Entertainment, Media, Gaming & Telecom ──────────────
    "cox automotive": "Chris Vaughan",
    "macy’s": "Chris Vaughan", "macys": "Chris Vaughan", "macy's": "Chris Vaughan",
    "estee lauder": "Chris Vaughan",
    "altice usa": "Chris Vaughan", "altice": "Chris Vaughan",
    "national hockey league": "Chris Vaughan", "nhl": "Chris Vaughan",
    "bse global": "Chris Vaughan",
    "netflix": "Chris Vaughan",
    "charter": "Chris Vaughan", "spectrum": "Chris Vaughan",
    "spotify": "Chris Vaughan",
    "major league baseball": "Chris Vaughan", "mlb": "Chris Vaughan",

    # ── Steve Lukaszewski – Healthcare & Life Sciences ──────────────────────
    "cvs health": "Steve Lukaszewski",
    "blue cross blue shield": "Steve Lukaszewski", "bcbs": "Steve Lukaszewski",
    "astellas": "Steve Lukaszewski",
    "abbott": "Steve Lukaszewski", "abbvie": "Steve Lukaszewski",
    "piedmont": "Steve Lukaszewski",
    "pfizer": "Steve Lukaszewski",
    "astrazeneca": "Steve Lukaszewski",
    "ecolab": "Steve Lukaszewski",
    "amgen": "Steve Lukaszewski", "horizon therapeutics": "Steve Lukaszewski",
    "shore capital": "Steve Lukaszewski",
    "unc healthcare": "Steve Lukaszewski", "unc health": "Steve Lukaszewski",
    "ssm": "Steve Lukaszewski",
    "wellstar": "Steve Lukaszewski",
    "lundbeck": "Steve Lukaszewski",
    "generac": "Steve Lukaszewski",
    "univar": "Steve Lukaszewski",
    "takeda": "Steve Lukaszewski", "shire": "Steve Lukaszewski",
    "zurich insurance": "Steve Lukaszewski",
    "bon secours": "Steve Lukaszewski",
    "roak capital": "Steve Lukaszewski",

    # ── Doug Leininger – Healthcare, Life Sciences, Insurance ───────────────
    "walgreens": "Doug Leininger", "walgreens boots": "Doug Leininger",
    "hcsc": "Doug Leininger", "blue cross il": "Doug Leininger",
    "elevance": "Doug Leininger", "anthem": "Doug Leininger",
    "ge healthcare": "Doug Leininger",
    "medical mutual": "Doug Leininger",
    "ohio health": "Doug Leininger",
    "trihealth": "Doug Leininger",
    "uc health": "Doug Leininger",
    "christ hospital": "Doug Leininger",
    "cincinnati children": "Doug Leininger",
    "erie insurance": "Doug Leininger",
    "medline": "Doug Leininger",
    "great american insurance": "Doug Leininger", "gaig": "Doug Leininger",
    "nationwide": "Doug Leininger",
    "medpace": "Doug Leininger",
    "caring communities": "Doug Leininger",
    "farmers insurance": "Doug Leininger",
    "sift md": "Doug Leininger",
    "optum": "Doug Leininger",
    "uw health": "Doug Leininger",

    # ── Scott Patterson – Financial Services & Insurance ────────────────────
    "keybank": "Scott Patterson",
    "new york life": "Scott Patterson", "nyl": "Scott Patterson",
    "allstate": "Scott Patterson",
    "m&t bank": "Scott Patterson", "mt bank": "Scott Patterson",
    "bmo": "Scott Patterson",
    "vanguard": "Scott Patterson",
    "guardian life": "Scott Patterson",
    "hanover": "Scott Patterson",
    "erie insurance": "Scott Patterson",
    "massmutual": "Scott Patterson",
    "fhlb": "Scott Patterson",
    "cincinnati financial": "Scott Patterson",
    "first united bank": "Scott Patterson",
    "first financial": "Scott Patterson",
    "varsity healthcare": "Scott Patterson",
    "associate bank": "Scott Patterson",
    "farm credit": "Scott Patterson",
    "delta community credit": "Scott Patterson",
    "percheron": "Scott Patterson",

    # ── Palmer Karsh – Retail, CPG, Consulting, Real Estate ─────────────────
    "abercrombie": "Palmer Karsh",
    "pilot ": "Palmer Karsh",
    "southern glazer": "Palmer Karsh",
    "kroger": "Palmer Karsh", "84.51": "Palmer Karsh",
    "michelin": "Palmer Karsh",
    "energizer": "Palmer Karsh",
    "kohl": "Palmer Karsh",
    "procter & gamble": "Palmer Karsh", "p&g": "Palmer Karsh",
    "wendy": "Palmer Karsh", "arby": "Palmer Karsh",
    "bob evans": "Palmer Karsh",
    "drt ": "Palmer Karsh",
    "black book": "Palmer Karsh",
    "worthington": "Palmer Karsh",
    "avalonbay": "Palmer Karsh",
    "lennar": "Palmer Karsh",
    "thrivent": "Palmer Karsh",
    "fifth third": "Palmer Karsh",
    "paycor": "Palmer Karsh",
    "penn foster": "Palmer Karsh",

    # ── Paul Ferri – Technology, Law, Manufacturing, MSP ────────────────────
    "disney ": "Paul Ferri",   # MSP side
    "ulta": "Paul Ferri",
    "belkin": "Paul Ferri",
    "intel": "Paul Ferri",
    "match group": "Paul Ferri",
    "samsung": "Paul Ferri",
    "tile ": "Paul Ferri",
    "turing labs": "Paul Ferri",
    "grubhub": "Paul Ferri",
    "logitech": "Paul Ferri",
    "getty images": "Paul Ferri",
    "uptake": "Paul Ferri",
    "sonos": "Paul Ferri",
    "rakuten": "Paul Ferri",
    "id.me": "Paul Ferri",
    "intuit": "Paul Ferri",
    "meta": "Paul Ferri", "facebook": "Paul Ferri",
    "fetch rewards": "Paul Ferri",
    "apple": "Paul Ferri",
    "vivid seats": "Paul Ferri",
}

# ---------------------------------------------------------------------------
def assign_owner(company: str | None) -> str | None:
    """
    Return the Account Executive’s name for a company, or None when unknown.
    """
    if not company:
        return None
    c = company.casefold()
    for alias, owner in OWNER_LOOKUP.items():
        if alias in c:
            return owner
    return None



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
    True if this job belongs to one of our target functions.
    Handles NaNs / non-string values safely.
    """
    ALLOWED = {
        "machine learning", "data science", "data analytics", "analytics",
        "visualization", "data governance", "engineering", "product management",
    }

    raw = row.get("job_function") or row.get("job_title") or ""
    text = str(raw).lower()                 # <— always a string now
    return any(k in text for k in ALLOWED)


# ─── Target disciplines & helpers ──────────────────────────────────────────────
CATEGORIES = {
    "machine learning",
    "data science",
    "data analytics",
    "visualization",
    "data governance",
    "engineering",
    "product management",
}

def is_allowed(row: dict) -> bool:
    """
    Return True if the job *mentions* one of our target disciplines
    in either job_function or job_title (case-insensitive).
    """
    text = " ".join(
        str(row.get(col, "")).lower()
        for col in ("job_function", "job_title")
    )
    return any(cat in text for cat in CATEGORIES)

# ─── Smarter scoring: recency ➊  +  relevance ➋ ───────────────────────────────
def score_job(job: dict) -> tuple[int, str]:
    """
    100-point score = ➊ recency (0-50) + ➋ relevance (0-50)

    • Recency → 50 if posted today, 0 if ≥30 days old (linear in-between)
    • Relevance → proportion of target keywords present in title / function
    """
    # ➊ RECENCY
    raw_time = str(job.get("job_posted_time", "")).split("T")[0]  # cope w/ ISO+
    try:
        days = (datetime.utcnow() - datetime.fromisoformat(raw_time)).days
    except Exception:
        days = 30                                                      # unknown ⇒ worst
    recency_score = max(0, 50 - int((days / 30) * 50))                 # clamp 0-50

    # ➋ RELEVANCE
    text = " ".join(
        str(job.get(col, "")).lower()
        for col in ("job_function", "job_title")
    )
    hits      = [cat for cat in CATEGORIES if cat in text]
    relevance_score = int(50 * len(hits) / len(CATEGORIES))            # 0-50

    # ─ combined
    total = recency_score + relevance_score                            # 0-100
    reason = (
        f"Recency: {recency_score}/50 (posted {days} d ago)  |  "
        f"Relevance: {relevance_score}/50 (keywords: {', '.join(hits) or 'none'})"
    )
    return total, reason

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
