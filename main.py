#!/usr/bin/env python3
# ────────────────────────────────────────────────────────────────────────────────
# main.py – download latest Bright Data CSV from S3, score each job with GPT-4,
#           push in-scope jobs to Airtable, and tag with the correct account owner.
#           Only jobs posted in the last **7 days** are uploaded to Airtable.
# ────────────────────────────────────────────────────────────────────────────────
import os, time, logging
from datetime import datetime, timedelta, timezone
import boto3, pandas as pd
from pyairtable import Table
from openai import OpenAI

# ─── Account-owner lookup (complete Top10/Next10 list) ─────────────────────────
# Keep the readable list of (account, owner) tuples below; the dictionary used
# at runtime is generated from it so you only have one place to edit.

OWNER_PAIRS: list[tuple[str, str]] = [
    # — Chris Vaughan —
    ("DIRECTV", "Chris Vaughan"),
    ("The Walt Disney Company/ESPN", "Chris Vaughan"),
    ("SiriusXM", "Chris Vaughan"),
    ("Electronic Arts (EA)", "Chris Vaughan"),
    ("NBCUniversal", "Chris Vaughan"),
    ("Consumer Cellular, Inc.", "Chris Vaughan"),
    ("US Cellular", "Chris Vaughan"),
    ("Rockstar Games", "Chris Vaughan"),
    ("T-Mobile", "Chris Vaughan"),
    ("Time Warner", "Chris Vaughan"),
    ("Horizon Media", "Chris Vaughan"),
    ("Zynga", "Chris Vaughan"),
    ("MMA (Marketing Management Analytics)", "Chris Vaughan"),
    ("Ogilvy", "Chris Vaughan"),
    ("Rush Street Interactive", "Chris Vaughan"),
    ("The E.W. Scripps Company", "Chris Vaughan"),
    ("Madison Square Garden Entertainment", "Chris Vaughan"),
    ("Quad", "Chris Vaughan"),
    ("Nielsen", "Chris Vaughan"),
    ("1-800-FLOWERS.COM, Inc.", "Chris Vaughan"),
    ("Altice USA", "Chris Vaughan"),
    ("FINRA", "Chris Vaughan"),
    ("Cox Enterprise", "Chris Vaughan"),
    ("Northwell Health", "Chris Vaughan"),
    ("Charter", "Chris Vaughan"),
    ("Spectrum", "Chris Vaughan"),
    ("Warner Bros", "Chris Vaughan"),
    ("Spotify", "Chris Vaughan"),
    ("BSE", "Chris Vaughan"),
    ("MSG", "Chris Vaughan"),
    ("NBC", "Chris Vaughan"),
    ("MSC Industrial Supply", "Chris Vaughan"),

    # — Steve Lukaszewski —
    ("CVS Health", "Steve Lukaszewski"),
    ("Blue Cross Blue Shield Association", "Steve Lukaszewski"),
    ("Astellas", "Steve Lukaszewski"),
    ("Abbott Labs and AbbVie", "Steve Lukaszewski"),
    ("Piedmont", "Steve Lukaszewski"),
    ("Pfizer", "Steve Lukaszewski"),
    ("AstraZeneca", "Steve Lukaszewski"),
    ("Ecolab", "Steve Lukaszewski"),
    ("Amgen (Horizon Therapeutics)", "Steve Lukaszewski"),
    ("Shore Capital", "Steve Lukaszewski"),
    ("UNC Healthcare", "Steve Lukaszewski"),
    ("SSM", "Steve Lukaszewski"),
    ("WellStar", "Steve Lukaszewski"),
    ("H. Lundbeck", "Steve Lukaszewski"),
    ("Generac", "Steve Lukaszewski"),
    ("Univar", "Steve Lukaszewski"),
    ("Takeda/Shire Pharmaceuticals", "Steve Lukaszewski"),
    ("Zurich Insurance", "Steve Lukaszewski"),
    ("Bon Secours Mercy Health", "Steve Lukaszewski"),
    ("Roak Capital", "Steve Lukaszewski"),

    # — Doug Leininger —
    ("Walgreens", "Doug Leininger"),
    ("Health Care Services Corporation", "Doug Leininger"),
    ("Elevance Health", "Doug Leininger"),
    ("GE Healthcare Technologies", "Doug Leininger"),
    ("Medical Mutual of Ohio", "Doug Leininger"),
    ("Ohio Health", "Doug Leininger"),
    ("TriHealth", "Doug Leininger"),
    ("UC Health", "Doug Leininger"),
    ("The Christ Hospital Health Network", "Doug Leininger"),
    ("Cincinnati Children's Hospital", "Doug Leininger"),
    ("Erie Insurance", "Doug Leininger"),
    ("Medline Industries", "Doug Leininger"),
    ("Great American Insurance Group", "Doug Leininger"),
    ("Nationwide Insurance Group", "Doug Leininger"),
    ("Medpace", "Doug Leininger"),
    ("Caring Communities", "Doug Leininger"),
    ("Farmers Insurance", "Doug Leininger"),
    ("Sift MD", "Doug Leininger"),
    ("Optum", "Doug Leininger"),
    ("UW Health", "Doug Leininger"),

    # — Scott Patterson —
    ("KeyBank", "Scott Patterson"),
    ("NYL Insurance", "Scott Patterson"),
    ("CVS Health (A&BC)", "Scott Patterson"),
    ("FHLB Chicago & Cincy", "Scott Patterson"),
    ("Metlife", "Scott Patterson"),
    ("Allstate", "Scott Patterson"),
    ("M&T Bank", "Scott Patterson"),
    ("Old National", "Scott Patterson"),
    ("Vanguard", "Scott Patterson"),
    ("Guardian Life", "Scott Patterson"),
    ("The Hanover Insurance", "Scott Patterson"),
    ("MassMutual", "Scott Patterson"),
    ("First United Bank", "Scott Patterson"),
    ("First Financial", "Scott Patterson"),
    ("BMO", "Scott Patterson"),
    ("Associate Bank", "Scott Patterson"),
    ("Farm Credit Mid-America", "Scott Patterson"),
    ("Regions Bank", "Scott Patterson"),
    ("Percheron Capital", "Scott Patterson"),
    ("Varsity Healthcare Partners", "Scott Patterson"),

    # — Palmer Karsh —
    ("Abercrombie & Fitch", "Palmer Karsh"),
    ("Pilot", "Palmer Karsh"),
    ("Southern Glazers", "Palmer Karsh"),
    ("Kroger/84.51", "Palmer Karsh"),
    ("Michelin", "Palmer Karsh"),
    ("Energizer", "Palmer Karsh"),
    ("Kohl’s", "Palmer Karsh"),
    ("Proctor & Gamble", "Palmer Karsh"),
    ("Wendy’s Arby Group", "Palmer Karsh"),
    ("Bob Evans Farm", "Palmer Karsh"),
    ("DRT", "Palmer Karsh"),
    ("Black Book", "Palmer Karsh"),
    ("Worthington", "Palmer Karsh"),
    ("AvalonBay", "Palmer Karsh"),
    ("Lennar", "Palmer Karsh"),
    ("Thrivent", "Palmer Karsh"),
    ("Fifth Third Bank", "Palmer Karsh"),
    ("Paycor", "Palmer Karsh"),
    ("GAIG", "Palmer Karsh"),
    ("Penn Foster", "Palmer Karsh"),

    # — Paul Ferri —
    ("Chamberlain", "Paul Ferri"),
    ("Charter Manufacturing", "Paul Ferri"),
    ("GE Aviation/Aerospace", "Paul Ferri"),
    ("ESPN/Disney", "Paul Ferri"),
    ("Republic National Distribution Company", "Paul Ferri"),
    ("Colonial Pipeline", "Paul Ferri"),
    ("Orrick", "Paul Ferri"),
    ("5/3 Bank", "Paul Ferri"),
    ("The Match Group", "Paul Ferri"),
    ("Priceline", "Paul Ferri"),
    ("Thompson Hine", "Paul Ferri"),
    ("Data Society", "Paul Ferri"),
    ("Archer Daniels Midland", "Paul Ferri"),
    ("Siemens", "Paul Ferri"),
    ("Turing Labs", "Paul Ferri"),
    ("Schaeffler", "Paul Ferri"),
    ("Celanese Corporation", "Paul Ferri"),
    ("McDermott Will Emery", "Paul Ferri"),
    ("DLA Piper", "Paul Ferri"),
    ("Sidley", "Paul Ferri"),

    # — Jeff Wohlgamuth —
    ("Dominos", "Jeff Wohlgamuth"),
    ("Rocket Companies", "Jeff Wohlgamuth"),
    ("May Mobility", "Jeff Wohlgamuth"),
    ("Whisker", "Jeff Wohlgamuth"),
    ("Jackson", "Jeff Wohlgamuth"),
    ("StockX", "Jeff Wohlgamuth"),
    ("Keller Williams", "Jeff Wohlgamuth"),
    ("Masco", "Jeff Wohlgamuth"),
    ("Trinity Health", "Jeff Wohlgamuth"),
    ("KLA", "Jeff Wohlgamuth"),
    ("Ford Credit", "Jeff Wohlgamuth"),
    ("GM Financial", "Jeff Wohlgamuth"),
    ("Michigan Medicine", "Jeff Wohlgamuth"),
    ("Flagstar Bank", "Jeff Wohlgamuth"),
    ("Consumers Energy", "Jeff Wohlgamuth"),
    ("Auto Owners Insurance", "Jeff Wohlgamuth"),
    ("Ford", "Jeff Wohlgamuth"),
    ("Ally Financial", "Jeff Wohlgamuth"),
    ("Dart", "Jeff Wohlgamuth"),
    ("Xpanse", "Jeff Wohlgamuth"),

    # — Ethan Frey —
    ("Nexxen", "Ethan Frey"),
    ("Elastic", "Ethan Frey"),
    ("Adobe", "Ethan Frey"),
    ("Open Text", "Ethan Frey"),
    ("Western and Southern", "Ethan Frey"),
    ("Carebridge", "Ethan Frey"),
    ("Charter Up", "Ethan Frey"),
    ("Cradlewise", "Ethan Frey"),
    ("Fandom", "Ethan Frey"),
    ("Tailwind", "Ethan Frey"),
    ("Bluestar", "Ethan Frey"),
    ("Medpace", "Ethan Frey"),
    ("Stifel", "Ethan Frey"),
    ("Divisions Maintenance Group", "Ethan Frey"),
    ("Fischer Homes", "Ethan Frey"),
    ("Speedway", "Ethan Frey"),
    ("Newell Brands", "Ethan Frey"),
    ("Honeywell", "Ethan Frey"),
    ("Home Depot", "Ethan Frey"),
    ("Inspire Brands", "Ethan Frey"),

    # — Jason Kratsa —
    ("Apple Leisure Group", "Jason Kratsa"),
    ("Dick’s Sporting Goods", "Jason Kratsa"),
    ("Cook Myosite", "Jason Kratsa"),
    ("Five Below", "Jason Kratsa"),
    ("Crayola", "Jason Kratsa"),
    ("Psyma", "Jason Kratsa"),
    ("Wawa Inc.", "Jason Kratsa"),
    ("Principia Consulting", "Jason Kratsa"),
    ("MSA – The Safety Company", "Jason Kratsa"),
    ("PPG Industries", "Jason Kratsa"),
    ("Intercontinental Exchange", "Jason Kratsa"),
    ("Freeman Company", "Jason Kratsa"),
    ("PvH Corp", "Jason Kratsa"),
    ("Microsoft", "Jason Kratsa"),
    ("Amazon", "Jason Kratsa"),
    ("OneMain Financial", "Jason Kratsa"),
    ("The Auto Club Group", "Jason Kratsa"),
    ("Credit One Bank", "Jason Kratsa"),
    ("American Credit Acceptance", "Jason Kratsa"),
    ("PenFed", "Jason Kratsa"),
]

# → Fast lookup dict  (keys lower-cased for tolerant matching)
OWNER_LOOKUP = {acct.lower(): owner for acct, owner in OWNER_PAIRS}




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
