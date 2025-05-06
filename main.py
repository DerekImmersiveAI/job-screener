#!/usr/bin/env python3
"""
Bright Data ➜ Airtable job-screener
--------------------------------------------------
 • filters to analytics / data / ML roles
 • looks up account owner
 • pushes to Airtable
"""

import csv, os, logging, pathlib, datetime as dt
from pyairtable import Table

# ──────────────────────────────────────────────────────────────────────────────
#            1.  CONFIG
# ──────────────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN      = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID    = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
SOURCE_FOLDER       = pathlib.Path("/mnt/data")      # where BrightData CSV lives

log = logging.getLogger("job-screener")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ------------------------------------------------------------------
#  Account-manager lookup  (all keys lower-case)
# ------------------------------------------------------------------
OWNER_LOOKUP = {
    # ── Henry Hartmann ───────────────────────────────────────────
    "directv": "Henry Hartmann",
    "walt disney": "Henry Hartmann",
    "espn": "Henry Hartmann",
    "siriusxm": "Henry Hartmann",
    "electronic arts": "Henry Hartmann",
    "ea ": "Henry Hartmann",
    "nbcuniversal": "Henry Hartmann",
    "consumer cellular": "Henry Hartmann",
    "us cellular": "Henry Hartmann",
    "rockstar games": "Henry Hartmann",
    "t-mobile": "Henry Hartmann",
    "time warner": "Henry Hartmann",
    "horizon media": "Henry Hartmann",
    "zynga": "Henry Hartmann",
    "marketing management analytics": "Henry Hartmann",
    "mma ": "Henry Hartmann",
    "ogilvy": "Henry Hartmann",
    "rush street interactive": "Henry Hartmann",
    "scripps": "Henry Hartmann",
    "madison square garden": "Henry Hartmann",
    "quad ": "Henry Hartmann",
    "nielsen": "Henry Hartmann",
    "1-800-flowers": "Henry Hartmann",

    # ── Chris Vaughan ────────────────────────────────────────────
    "cox automotive": "Chris Vaughan",
    "macy": "Chris Vaughan",
    "estee lauder": "Chris Vaughan",
    "altice": "Chris Vaughan",
    "national hockey league": "Chris Vaughan",
    "nhl": "Chris Vaughan",
    "bse global": "Chris Vaughan",
    "netflix": "Chris Vaughan",
    "charter": "Chris Vaughan",
    "spectrum": "Chris Vaughan",
    "spotify": "Chris Vaughan",
    "major league baseball": "Chris Vaughan",
    "mlb": "Chris Vaughan",

    # ── Steve Lukaszewski ────────────────────────────────────────
    "cvs health": "Steve Lukaszewski",
    "blue cross": "Steve Lukaszewski",
    "astellas": "Steve Lukaszewski",
    "abbott": "Steve Lukaszewski",
    "abbvie": "Steve Lukaszewski",
    "piedmont": "Steve Lukaszewski",
    "pfizer": "Steve Lukaszewski",
    "astrazeneca": "Steve Lukaszewski",
    "ecolab": "Steve Lukaszewski",
    "amgen": "Steve Lukaszewski",
    "horizon therapeutics": "Steve Lukaszewski",
    "shore capital": "Steve Lukaszewski",
    "unc healthcare": "Steve Lukaszewski",
    "ssm ": "Steve Lukaszewski",
    "wellstar": "Steve Lukaszewski",
    "lundbeck": "Steve Lukaszewski",
    "generac": "Steve Lukaszewski",
    "univar": "Steve Lukaszewski",
    "takeda": "Steve Lukaszewski",
    "shire": "Steve Lukaszewski",
    "zurich": "Steve Lukaszewski",
    "bon secours": "Steve Lukaszewski",
    "roak capital": "Steve Lukaszewski",

    # ── Doug Leininger ───────────────────────────────────────────
    "walgreens": "Doug Leininger",
    "walgreens boots": "Doug Leininger",
    "hcsc": "Doug Leininger",
    "elevance": "Doug Leininger",
    "anthem": "Doug Leininger",
    "ge healthcare": "Doug Leininger",
    "medical mutual": "Doug Leininger",
    "ohio health": "Doug Leininger",
    "trihealth": "Doug Leininger",
    "uc health": "Doug Leininger",
    "christ hospital": "Doug Leininger",
    "cincinnati children": "Doug Leininger",
    "erie insurance": "Doug Leininger",
    "medline": "Doug Leininger",
    "great american insurance": "Doug Leininger",
    "nationwide": "Doug Leininger",
    "medpace": "Doug Leininger",
    "caring communities": "Doug Leininger",
    "farmers insurance": "Doug Leininger",
    "sift md": "Doug Leininger",
    "optum": "Doug Leininger",
    "uw health": "Doug Leininger",

    # ── Scott Patterson ──────────────────────────────────────────
    "keybank": "Scott Patterson",
    "new york life": "Scott Patterson",
    "nyl": "Scott Patterson",
    "allstate": "Scott Patterson",
    "m&t bank": "Scott Patterson",
    "bmo": "Scott Patterson",
    "vanguard": "Scott Patterson",
    "guardian life": "Scott Patterson",
    "hanover": "Scott Patterson",
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

    # ── Palmer Karsh ────────────────────────────────────────────
    "abercrombie": "Palmer Karsh",
    "pilot ": "Palmer Karsh",
    "southern glazer": "Palmer Karsh",
    "kroger": "Palmer Karsh",
    "84.51": "Palmer Karsh",
    "michelin": "Palmer Karsh",
    "energizer": "Palmer Karsh",
    "kohl": "Palmer Karsh",
    "procter & gamble": "Palmer Karsh",
    "p&g": "Palmer Karsh",
    "wendy": "Palmer Karsh",
    "arby": "Palmer Karsh",
    "bob evans": "Palmer Karsh",
    "drt ": "Palmer Karsh",
    "black book": "Palmer Karsh",
    "worthington": "Palmer Karsh",
    "avalonbay": "Palmer Karsh",
    "lennar": "Palmer Karsh",
    "thrivent": "Palmer Karsh",
    "fifth third": "Palmer Karsh",
    "paycor": "Palmer Karsh",
    "gaig": "Palmer Karsh",
    "penn foster": "Palmer Karsh",

    # ── Paul Ferri ───────────────────────────────────────────────
    "disney": "Paul Ferri",
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
    "meta": "Paul Ferri",
    "facebook": "Paul Ferri",
    "fetch rewards": "Paul Ferri",
    "apple": "Paul Ferri",
    "vivid seats": "Paul Ferri",
}

# ------------------------------------------------------------------
def get_owner(company: str) -> str:
    """Return AE name or ''."""
    c = (company or "").lower()
    for key, owner in OWNER_LOOKUP.items():
        if key in c:
            return owner
    return ""


# ──────────────────────────────────────────────────────────────────────────────
#            2.  BUSINESS RULES
# ──────────────────────────────────────────────────────────────────────────────
def is_allowed(row: dict) -> bool:
    """
    True if this job belongs to one of our target functions.
    Handles NaNs / non-string values safely.
    """
    ALLOWED = {
        "machine learning", "data science", "data analytics", "analytics",
        "visualization", "data governance", "engineering", "product management",
    }
    raw  = row.get("job_function") or row.get("job_title") or ""
    text = str(raw).lower()
    return any(k in text for k in ALLOWED)


# ──────────────────────────────────────────────────────────────────────────────
#            3.  MAIN
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    # ── locate newest BrightData CSV
    csv_path = max(SOURCE_FOLDER.glob("bd_*.csv"), key=lambda p: p.stat().st_mtime)
    log.info("📥 Processing %s", csv_path.name)

    airtable = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        added = 0
        for row in reader:
            if not is_allowed(row):
                continue

            company = (row.get("company_name") or row.get("company") or "").strip()
            owner   = get_owner(company)

            record = {
                "Job Title"   : row.get("job_title"),
                "Company"     : company,
                "Location"    : row.get("job_location"),
                "Posted"      : row.get("job_posted_date"),
                "Link"        : row.get("apply_link") or row.get("url"),
                "Owner"       : owner,
            }

            airtable.create(record)
            log.info("✅ Airtable: added %s @ %s [owner: %s]",
                     record["Job Title"], company, owner or "-")
            added += 1

    log.info("🎉 Done – %s new records", added)


if __name__ == "__main__":
    log.info("🚀 Starting job screener…")
    main()
