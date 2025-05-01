#!/usr/bin/env python3
# ─────────────────────────────────────────────────────────────────────────────
#  Bright-Data Job Screener  ▸  pushes selected jobs to Airtable
#  • Category filter:   AI Expertise, Machine Learning, Data Science, etc.
#  • Relevance score:   #matched-keywords in title + description
#  • Age filter:        *DISABLED*  (no “< 7 days” check)
# ─────────────────────────────────────────────────────────────────────────────

import csv
import logging
import os
import re
from pathlib import Path
from typing import List

import pandas as pd
from pyairtable import Table
from pyairtable.formulas import match

# ── config ───────────────────────────────────────────────────────────────────
CSV_URI              = os.getenv("CSV_URI")              # e.g. s3://bucket/file.csv
AIRTABLE_TOKEN       = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID     = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME  = os.getenv("AIRTABLE_TABLE_NAME")
MAX_ROWS             = int(os.getenv("MAX_ROWS", 100))   # safety-valve

CATEGORIES: List[str] = [
    "AI Expertise",
    "Machine Learning",
    "Data Science",
    "Data Analytics",
    "Visualization",
    "Data Governance",
    "Engineering",
]

CATEGORY_REGEX = re.compile("|".join(
    [re.escape(c) for c in CATEGORIES if c.strip()]), flags=re.I)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ── helpers ──────────────────────────────────────────────────────────────────
def download_csv(uri: str, dest: Path) -> Path:
    """Very thin wrapper – works for local paths or pre-mounted cloud URIs."""
    if uri.startswith(("http://", "https://", "s3://")):
        import boto3, botocore  # only used when really needed
        if uri.startswith("s3://"):
            s3 = boto3.client("s3")
            bucket, key = uri[5:].split("/", 1)
            s3.download_file(bucket, key, str(dest))
        else:  # https://
            import requests
            r = requests.get(uri, timeout=30)
            r.raise_for_status()
            dest.write_bytes(r.content)
    else:
        dest = Path(uri).expanduser().resolve()
    return dest


def relevant(row) -> int:
    """Relevance score = matched keywords in title + description."""
    text = f"{row.get('job_title','')} {row.get('job_description','')}".lower()
    return sum(k.lower() in text for k in CATEGORIES)


def push_to_airtable(table: Table, row: pd.Series):
    """Insert or update by external ID (job_posting_id if present)."""
    record = {
        "Job Title": row.get("job_title"),
        "Company": row.get("company_name"),
        "Location": row.get("job_location"),
        "Posted": row.get("job_posted_time"),
        "Relevance": row.get("relevance"),
        "Apply Link": row.get("apply_link") or row.get("url"),
    }
    ext_id = str(row.get("job_posting_id") or row.get("url"))
    # upsert
    existing = table.first(formula=match({"External ID": ext_id}))
    record["External ID"] = ext_id
    if existing:
        table.update(existing["id"], record)
    else:
        table.create(record)


# ── main ─────────────────────────────────────────────────────────────────────
def main():
    logging.info("🚀 Starting job screener…")
    tmp_csv = download_csv(CSV_URI, Path("/tmp/jobs.csv"))
    df = pd.read_csv(tmp_csv).fillna("")

    logging.info("📊 Loaded %d rows from CSV", len(df))

    # 1️⃣ CATEGORY FILTER ------------------------------------------------------
    # candidate text to search
    text = (
        df["job_title"].astype(str)
        + " " +
        df.get("job_description", "").astype(str)
    )
    mask = text.str.contains(CATEGORY_REGEX, na=False)
    df = df[mask]
    logging.info("🔍 After category filter: %d rows remain", len(df))

    if df.empty:
        logging.warning("⚠️  No rows matched the category list – exiting.")
        return

    # 2️⃣ RELEVANCE SCORE ------------------------------------------------------
    df["relevance"] = df.apply(relevant, axis=1)
    df = df[df["relevance"] > 0]

    if df.empty:
        logging.warning("⚠️  All rows scored 0 relevance – exiting.")
        return

    df = df.sort_values(
        ["relevance", "job_posted_time"],
        ascending=[False, False]
    ).head(MAX_ROWS)

    # 3️⃣ PUSH TO AIRTABLE -----------------------------------------------------
    table = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    pushed = 0
    for _, row in df.iterrows():
        try:
            push_to_airtable(table, row)
            pushed += 1
        except Exception as exc:
            logging.error("Failed to push row (%s): %s", row.get("job_title"), exc)

    logging.info("✅ Finished – %d records synced to Airtable", pushed)


if __name__ == "__main__":
    main()
