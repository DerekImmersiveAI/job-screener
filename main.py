#!/usr/bin/env python3
# ────────────────────────────────────────────────────────────────────────────────
# main.py – download latest Bright Data CSV from S3, score each job with GPT‑4,
#           push in‑scope jobs to Airtable, and tag with the correct account owner.
#           Only jobs posted in the last **7 days** are uploaded to Airtable.
# ────────────────────────────────────────────────────────────────────────────────
import os, time, logging
from datetime import datetime, timedelta, timezone
import boto3, pandas as pd
from pyairtable import Table
from openai import OpenAI

# ─── Account‑owner lookup (full list from your Top10/Next10 spreadsheet) ───────
# Maintain the raw tuples in OWNER_PAIRS for clarity; the dict below is derived
# automatically (keys are lower‑cased).  To add / change a mapping, just append
# a new (account, owner) pair to OWNER_PAIRS.

OWNER_PAIRS: list[tuple[str, str]] = [
    ("DIRECTV", "Chris Vaughan"),
    ("SiriusXM", "Chris Vaughan"),
    ("Netflix", "Chris Vaughan"),
    ("AT&T", "Chris Vaughan"),
    ("Consumer Cellular, Inc.", "Chris Vaughan"),
    ("US Cellular", "Chris Vaughan"),
    ("FOX Corp", "Chris Vaughan"),
    ("T-Mobile", "Chris Vaughan"),
    ("Horizon Media", "Chris Vaughan"),
    ("FedEx Services", "Chris Vaughan"),
    ("Madison Square Garden Entertainment", "Chris Vaughan"),
    ("Quad", "Chris Vaughan"),
    ("Altice USA", "Chris Vaughan"),
    ("FINRA", "Chris Vaughan"),
    ("Cox Enterprise", "Chris Vaughan"),
    ("Northwell Health", "Chris Vaughan"),
    ("Warner Bros", "Chris Vaughan"),
    ("Spotify", "Chris Vaughan"),
    ("MSC Industrial Supply", "Chris Vaughan"),
    ("Walgreens", "Doug Leininger"),
    ("Discount Tire", "Doug Leininger"),
    ("Elevance Health", "Doug Leininger"),
    ("GE Healthcare Technologies", "Doug Leininger"),
    ("Conagra Brands", "Doug Leininger"),
    ("Ohio Health", "Doug Leininger"),
    ("TriHealth", "Doug Leininger"),
    ("UC Health", "Doug Leininger"),
    ("Nielsen", "Doug Leininger"),
    ("Epsilon/Catalina", "Doug Leininger"),
    ("DTN", "Doug Leininger"),
    ("Medline Industries", "Doug Leininger"),
    ("Medspace", "Doug Leininger"),
    ("Wendys", "Doug Leininger"),
    ("Optum", "Doug Leininger"),
    ("Abercrombie & Fitch", "Palmer Karsh"),
    ("Pilot", "Palmer Karsh"),
    ("Southern Glazers", "Palmer Karsh"),
    ("Kroger/84.51", "Palmer Karsh"),
    ("Michelin", "Palmer Karsh"),
    ("Energizer", "Palmer Karsh"),
    ("Kohl’s", "Palmer Karsh"),
    ("Proctor & Gamble", "Palmer Karsh"),
    ("Bob Evans Farm", "Palmer Karsh"),
    ("DRT", "Palmer Karsh"),
    ("Worthington", "Palmer Karsh"),
    ("AvalonBay", "Palmer Karsh"),
    ("Lennar", "Palmer Karsh"),
    ("Thrivent", "Palmer Karsh"),
    ("Fifth Third Bank", "Palmer Karsh"),
    ("Paycor", "Palmer Karsh"),
    ("Penn Foster", "Palmer Karsh"),
    ("Accenture", "Paul Ferri"),
    ("Conccentrix", "Paul Ferri"),
    ("ESPN/Disney", "Paul Ferri"),
    ("Republic National Distribution Company", "Paul Ferri"),
    ("Colonial Pipeline", "Paul Ferri"),
    ("Orrick", "Paul Ferri"),
    ("5/3 Bank", "Paul Ferri"),
    ("Catalina", "Paul Ferri"),
    ("Lexis Nexis", "Paul Ferri"),
    ("PWC", "Paul Ferri"),
    ("Mckinsey & Compnay", "Paul Ferri"),
    ("Deloitte", "Paul Ferri"),
    ("Bain and Company", "Paul Ferri"),
    ("KeyBank", "Scott Patterson"),
    ("CVS Health (A&BC)", "Scott Patterson"),
    ("FHLB Chicago & Cincy", "Scott Patterson"),
    ("Guardian Life", "Scott Patterson"),
    ("Metlife", "Scott Patterson"),
    ("Allstate", "Scott Patterson"),
    ("Charles Schwab", "Scott Patterson"),
    ("Old National", "Scott Patterson"),
    ("Vanguard", "Scott Patterson"),
    ("New Yrok Life", "Scott Patterson"),
    ("The Hanover Insurance", "Scott Patterson"),
    ("MassMutual", "Scott Patterson"),
    ("First United Bank", "Scott Patterson"),
    ("First Financial", "Scott Patterson"),
    ("Equifax", "Scott Patterson"),
    ("Northern Trust", "Scott Patterson"),
    ("Regional Management Corp", "Scott Patterson"),
    ("Grainger", "Scott Patterson"),
    ("Blue Cross Blue Shield Association", "Steve Lukaszewski"),
    ("Astellas", "Steve Lukaszewski"),
    ("Abbott Nutrition", "Steve Lukaszewski"),
    ("Abbvie", "Steve Lukaszewski"),
    ("Piedmont", "Steve Lukaszewski"),
    ("Pfizer", "Steve Lukaszewski"),
    ("AstraZeneca", "Steve Lukaszewski"),
    ("Ecolab", "Steve Lukaszewski"),
    ("Amgen (Horizon Therapeutics)", "Steve Lukaszewski"),
    ("Shore Capital", "Steve Lukaszewski"),
    ("UNC Healthcare", "Steve Lukaszewski"),
    ("SSM Health", "Steve Lukaszewski"),
    ("WellStar", "Steve Lukaszewski"),
    ("Generac", "Steve Lukaszewski"),
    ("Univar", "Steve Lukaszewski"),
    ("Takeda", "Steve Lukaszewski"),
    ("Zurich", "Steve Lukaszewski"),
    ("Bon Secours Mercy Health", "Steve Lukaszewski"),
    ("Roark Capital", "Steve Lukaszewski"),
    ("Dominos", "Jeff Wohlgamuth"),
    ("Rocket Companies", "Jeff Wohlgamuth"),
    ("May Mobility", "Jeff Wohlgamuth"),
    ("Whisker", "Jeff Wohlgamuth"),
    ("StockX", "Jeff Wohlgamuth"),
    ("Keller Williams", "Jeff Wohlgamuth"),
    ("Masco", "Jeff Wohlgamuth"),
    ("Ford Credit", "Jeff Wohlgamuth"),
    ("GM Financial", "Jeff Wohlgamuth"),
    ("Flagstar Bank", "Jeff Wohlgamuth"),
    ("Consumers Energy", "Jeff Wohlgamuth"),
    ("Auto Owners Insurance", "Jeff Wohlgamuth"),
    ("Ford", "Jeff Wohlgamuth"),
    ("Ally Financial", "Jeff Wohlgamuth"),
    ("Dart", "Jeff Wohlgamuth"),
    ("Xpanse", "Jeff Wohlgamuth"),
    ("Nexxen", "Ethan Frey"),
    ("Adobe", "Ethan Frey"),
    ("Carebridge", "Ethan Frey"),
    ("Charter Up", "Ethan Frey"),
    ("Cradlewise", "Ethan Frey"),
    ("Fandom", "Ethan Frey"),
    ("Tailwind", "Ethan Frey"),
    ("Medpace", "Ethan Frey"),
    ("Stifel", "Ethan Frey"),
    ("Divisions Maintenance Group", "Ethan Frey"),
    ("Fischer Homes", "Ethan Frey"),
    ("Newell Brands", "Ethan Frey"),
    ("Honeywell", "Ethan Frey"),
    ("Inspire Brands", "Ethan Frey"),
    ("Apple Leisure Group", "Jason Kratsa"),
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
    ("OneMain Financial", "Jason Kratsa"),
    ("The Auto Club Group", "Jason Kratsa"),
    ("Credit One Bank", "Jason Kratsa"),
    ("American Credit Acceptance", "Jason Kratsa"),
    ("PenFed", "Jason Kratsa"),
]

# Derive fast lookup dict (lower‑case keys)
OWNER_LOOKUP = {account.lower(): owner for account, owner in OWNER_PAIRS}

# ---------------------------------------------------------------------------
# Assign an owner to a company name (case‑insensitive, alias‑tolerant)
# ---------------------------------------------------------------------------
def assign_owner(company: str | None) -> str | None:
    """Return the Account Executive’s name for a company, or None when unknown."""
    if not company:
        return None

    cname = company.casefold()  # lower‑case, accent‑insensitive-ish

    # Exact key lookup first (fast path)
    if cname in OWNER_LOOKUP:
        return OWNER_LOOKUP[cname]

    # Fallback: check if any alias substring matches
    for alias, owner in OWNER_LOOKUP.items():
        if alias in cname:
            return owner

    return None  # no match




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

DIRECTOR_KEYWORDS = [
    "director", "sr director", "senior director", "executive director",
    "vp", "vice president", "svp", "evp", "chief", "head of", "cto", "cio", "ceo"
]

def is_allowed(row: dict) -> bool:
    """
    Return True if the job is in our target disciplines AND is director level or higher.
    """
    # Check for discipline match
    discipline_text = " ".join(
        str(row.get(col, "")).lower() for col in ("job_function", "job_title")
    )
    if not any(cat in discipline_text for cat in CATEGORIES):
        return False

    # Check for seniority keywords
    title = str(row.get("job_title", "")).lower()
    return any(keyword in title for keyword in DIRECTOR_KEYWORDS)

# (the rest of your script remains unchanged below...)

# ─── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    logging.info("\U0001F680 Starting job screener...")
    path = fetch_latest_from_s3()
    if not path:
        logging.error("\uD83D\uDEA8 No file retrieved from S3.")
        return

    try:
        df = pd.read_csv(path, keep_default_na=True)
    except Exception as exc:
        logging.error("CSV read error: %s", exc)
        return

    # basic cleaning
    df = df.dropna(how="all")
    df = df.dropna(subset=["job_title", "company_name"])

    logging.info("\U0001F4CA Loaded %d rows", len(df))

    for job in df.to_dict("records"):
        if not is_allowed(job):
            logging.info("\U0001F6C8 Skipped (not director+ or out-of-scope): %s – %s",
                         job.get("job_title"), job.get("company_name"))
        time.sleep(1)   # Airtable rate-limit kindness


if __name__ == "__main__":
    main()
