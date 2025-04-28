#!/usr/bin/env python3
"""
main.py – BrightData → CSV → skill filter / scoring → Airtable

Environment variables expected
──────────────────────────────
BRIGHTDATA_URL              HTTPS URL to CSV (or directory listing that ends in *.csv)
AIRTABLE_API_KEY
AIRTABLE_BASE_ID
AIRTABLE_TABLE_NAME         e.g. "Jobs"
CHUNK_SIZE                  (optional) batch  size for Airtable uploads – default 10
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sys
import time
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── Logging ────────────────────────────────────────────────────────────────── #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)

log = logging.getLogger("job-screener")

# ─── Constants / Skill Taxonomy ─────────────────────────────────────────────── #
NOW = datetime.now(tz=timezone.utc)

CATEGORY_SKILL_MAP: Dict[str, List[str]] = {
    "AI Expertise": [
        "RAG",
        "GAN",
        "NLP Capabilities",
        "Multi-language Support",
        "Integration with Communication Platforms",
        "Customizability and Training",
        "Conversational Analytics",
        "Contextual Understanding and Memory",
        "User Authentication and Security",
        "Voice Interaction Capabilities",
        "Custom Connectors",
    ],
    "Machine Learning": [
        "Supervised",
        "Unsupervised",
        "Semi-Supervised",
        "Clustering",
        "Reinforcement",
        "Regression",
        "RNN",
        "CNN",
        "Transformer",
        "Feature Engineering",
        "Model Evaluation",
        "Hyperparameter Optimization",
        "Anomaly Detection",
    ],
    "Data Science": [
        "Statistical Analysis",
        "Data Wrangling",
        "Data Cleaning",
        "Exploratory Data Analysis",
        "Data Visualization",
        "Predictive Modeling",
        "Hypothesis Testing",
        "A/B Testing",
    ],
    "Data Analytics": [
        "Data Front End",
        "Advanced Analytics",
        "Data Source Development",
        "Data Visualization",
        "Data Integration",
        "ETL",
        "Big Data",
    ],
    "Visualization": ["Domo", "Tableau", "Power BI", "Looker"],
    "Data Governance": ["Data Governance", "Data Security", "Privacy"],
    "Engineering": [
        "Blockchain Engineer",
        "API Engineer",
        "Machine Learning Engineer",
        "Game Engineer",
        "Database Engineer",
        "Site Reliability Engineer",
        "Systems Engineer",
        "Embedded Systems Engineer",
        "Mobile Engineer",
        "Front End Engineer",
        "Back End Engineer",
        "DevOps Engineer",
        "Software Developer Engineer in Test",
        "Quality Assurance Engineer",
        "UI/UX Engineer",
        "Architect",
        "Full Stack Developer",
        "Cloud Engineering",
        "Data Engineering",
        "Software Engineering",
        "Network Engineering",
    ],
    "Product Management": [
        "Project Management",
        "Program Management",
        "Product Management",
        "Project Coordination",
        "Business Analysis",
    ],
}

ALL_SKILLS: set[str] = {s.lower() for arr in CATEGORY_SKILL_MAP.values() for s in arr}

RECENCY_MAX_DAYS = 30  # full score if posted today, linearly decays to 0 at 30d
JOB_POSTER_BONUS = 2   # additive points if listing has poster
SALARY_THRESHOLD = 140_000  # USD
SALARY_BONUS = 1       # additive points if salary above threshold

# ─── Utils ──────────────────────────────────────────────────────────────────── #
def backoff_session(
    total=5,
    backoff_factor=0.5,
    statuses=(429, 500, 502, 503, 504),
) -> requests.Session:
    retry = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=statuses,
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess = requests.Session()
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


session = backoff_session()

# ─── Step 1: Download latest CSV ───────────────────────────────────────────── #
def latest_csv_url(base_url: str) -> str:
    """
    If BRIGHTDATA_URL already points to a .csv – return it.
    Otherwise, assume it's a directory or JSON listing with CSV filenames and
    return the one with the latest timestamp in its name.
    """
    if base_url.lower().endswith(".csv"):
        return base_url

    log.info("🔍 Fetching listing from %s …", base_url)
    resp = session.get(base_url, timeout=30)
    resp.raise_for_status()
    listing = resp.text
    # naive parse: look for *.csv substrings
    csvs = [part for part in listing.split('"') if part.endswith(".csv")]
    if not csvs:
        raise RuntimeError("No CSV found in listing")
    newest = sorted(csvs)[-1]
    log.info("📄  Using latest file: %s", newest)
    return newest if newest.startswith("http") else base_url.rstrip("/") + "/" + newest


def stream_csv(url: str):
    """Yields dict rows without loading entire file."""
    log.info("📥  Streaming CSV from %s", url)
    with session.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        r.raw.decode_content = True
        text_wrapper = io.TextIOWrapper(r.raw, encoding="utf-8", newline="")
        reader = csv.DictReader(text_wrapper)
        for row in reader:
            yield row


# ─── Step 2: Scoring & filtering ──────────────────────────────────────────── #
def days_old(ts: str | None) -> float:
    """Assumes ts is ISO-like; returns days difference."""
    if not ts:
        return RECENCY_MAX_DAYS + 1
    with suppress(Exception):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return (NOW - dt).total_seconds() / 86400
    return RECENCY_MAX_DAYS + 1


def score_row(row: dict) -> int:
    score = 0

    # 1) recency 0-5 pts
    diff = days_old(row.get("job_posted_date") or row.get("posted_at"))
    recency_pts = max(0, int(round(5 * (1 - min(diff, RECENCY_MAX_DAYS) / RECENCY_MAX_DAYS))))
    score += recency_pts

    # 2) job poster present?
    if row.get("job_poster") or row.get("job_poster_url"):
        score += JOB_POSTER_BONUS

    # 3) salary ≥ threshold?
    try:
        salary = float(row.get("salary") or row.get("base_salary_max") or 0)
    except ValueError:
        salary = 0
    if salary >= SALARY_THRESHOLD:
        score += SALARY_BONUS

    # 4) GPT / semantic score (already computed earlier?)
    gpt_score = int(float(row.get("gpt_score", 0)))
    score += gpt_score

    return score


def has_required_skills(text: str) -> bool:
    text_lc = text.lower()
    return any(skill in text_lc for skill in ALL_SKILLS)


# ─── Step 3: Airtable helpers ─────────────────────────────────────────────── #
def get_airtable_fields(base_id: str, table: str, key: str) -> set[str]:
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    resp = session.get(
        url,
        headers={"Authorization": f"Bearer {key}"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    for tbl in data["tables"]:
        if tbl["name"] == table:
            return {field["name"] for field in tbl["fields"]}
    raise RuntimeError(f"Table {table} not found in base {base_id}")


def post_records(
    base_id: str,
    table: str,
    key: str,
    records: Sequence[dict],
    allowed_fields: set[str],
):
    url = f"https://api.airtable.com/v0/{base_id}/{table}"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    clean_records = [
        {"fields": {k: v for k, v in rec.items() if k in allowed_fields}}
        for rec in records
    ]
    resp = session.post(url, headers=headers, json={"records": clean_records}, timeout=30)
    if resp.status_code >= 300:
        log.error("❌ Airtable error %s -> %s", resp.status_code, resp.text)
        resp.raise_for_status()


# ─── Main pipeline ────────────────────────────────────────────────────────── #
def main():
    log.info("🟢  main.py booted – starting pipeline …")

    bright_url = os.environ["BRIGHTDATA_URL"]
    airtable_key = os.environ["AIRTABLE_API_KEY"]
    airtable_base = os.environ["AIRTABLE_BASE_ID"]
    airtable_table = os.environ["AIRTABLE_TABLE_NAME"]
    chunk_size = int(os.getenv("CHUNK_SIZE", "10"))

    latest_url = latest_csv_url(bright_url)

    allowed_fields = get_airtable_fields(airtable_base, airtable_table, airtable_key)
    log.info("🗝️   Retrieved %d Airtable fields", len(allowed_fields))

    batch: List[dict] = []
    processed = kept = 0

    for row in stream_csv(latest_url):
        processed += 1
        if processed % 1000 == 0:
            log.info("… processed %d rows", processed)

        # VERY BASIC concat of text to look for skills
        full_text = " ".join(str(row.get(k, "")) for k in ("job_title", "job_description", "skills"))
        if not has_required_skills(full_text):
            continue

        row["score"] = score_row(row)
        if row["score"] < 3:
            continue  # skip low scoring rows

        # minimal field cleanup
        with suppress(ValueError):
            row["salary"] = float(row.get("salary", 0))

        batch.append(row)
        kept += 1
        if len(batch) >= chunk_size:
            post_records(airtable_base, airtable_table, airtable_key, batch, allowed_fields)
            log.info("🚀 Sent %d records to Airtable (kept=%d/processed=%d)", len(batch), kept, processed)
            batch = []

    if batch:
        post_records(airtable_base, airtable_table, airtable_key, batch, allowed_fields)
        log.info("🚀 Sent final %d records", len(batch))

    log.info("🏁 Done. kept=%d  processed=%d", kept, processed)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pylint: disable=broad-except
        log.exception("💥 Unhandled exception: %s", exc)
        sys.exit(1)
