#!/usr/bin/env python3
"""
main.py
-------

Cron-style script that

1. finds the most-recent job CSV in an S3 bucket/prefix
2. downloads it
3. filters by category keywords
4. ranks remaining rows by “relevancy score”
5. saves the results locally (or wherever you like)

Environment variables required
------------------------------
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION              – e.g. "us-east-1"
S3_BUCKET               – e.g. "brightdata-job-screener"
S3_PREFIX               – e.g. "daily_exports/"   (include trailing slash!)
CATEGORY_KEYWORDS_JSON  – optional JSON dict of
                          {category: [kw1, kw2, ...]}

You can keep using Render env-vars, AWS IAM roles, etc.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import boto3
import pandas as pd

# --------------------------------------------------------------------------- #
#  Logging                                                                    #
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  Config                                                                     #
# --------------------------------------------------------------------------- #

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.getenv("S3_PREFIX", "")

# default category → keyword mapping; override with env-var if you like
DEFAULT_CATS: Dict[str, List[str]] = {
    "Data Engineering": [
        "data engineer",
        "etl",
        "big data",
        "spark",
        "hadoop",
        "redshift",
        "data warehouse",
    ],
    "Machine Learning": [
        "machine learning",
        "ml",
        "tensorflow",
        "pytorch",
        "scikit",
        "deep learning",
        "llm",
        "nlp",
        "computer vision",
    ],
    "Analytics / BI": [
        "analytics",
        "business intelligence",
        "bi",
        "power bi",
        "tableau",
        "lookml",
        "dashboards",
        "insights",
    ],
}

CATEGORY_KEYWORDS: Dict[str, List[str]] = DEFAULT_CATS
if "CATEGORY_KEYWORDS_JSON" in os.environ:
    try:
        CATEGORY_KEYWORDS = json.loads(os.environ["CATEGORY_KEYWORDS_JSON"])
    except Exception:
        log.warning("Could not parse CATEGORY_KEYWORDS_JSON – falling back to default")

OUT_FILE = Path("/tmp/filtered_jobs.csv")

# --------------------------------------------------------------------------- #
#  Helpers                                                                    #
# --------------------------------------------------------------------------- #


def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def latest_object(bucket: str, prefix: str) -> dict | None:
    """
    Return the S3 object metadata for the newest key under the prefix.
    """
    paginator = s3_client().get_paginator("list_objects_v2")
    newest = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if newest is None or obj["LastModified"] > newest["LastModified"]:
                newest = obj
    return newest


def download_to_tmp(bucket: str, key: str, local: Path) -> Path:
    local.parent.mkdir(parents=True, exist_ok=True)
    log.info("⬇️  Downloading s3://%s/%s → %s", bucket, key, local)
    s3_client().download_file(bucket, key, str(local))
    return local


def load_csv(fp: Path) -> pd.DataFrame:
    log.info("📄  Loading %s ...", fp)
    return pd.read_csv(fp)


def score_row(row: pd.Series, kw_map: Dict[str, List[str]]) -> int:
    """
    Simple relevancy score: +1 for each keyword match (case-insensitive).
    """
    haystack = " ".join(str(v).lower() for v in row.astype(str).values)
    score = 0
    for kws in kw_map.values():
        for kw in kws:
            if kw.lower() in haystack:
                score += 1
    return score


def filter_and_rank(df: pd.DataFrame) -> pd.DataFrame:
    # Keep rows with at least one category keyword
    def is_relevant(row: pd.Series) -> bool:
        return score_row(row, CATEGORY_KEYWORDS) > 0

    log.info("🔍  Filtering by category keywords …")
    filtered = df[df.apply(is_relevant, axis=1)].copy()
    if filtered.empty:
        log.warning("No rows matched category keywords!")
        return filtered

    # Add score column & sort descending
    filtered["score"] = filtered.apply(
        lambda r: score_row(r, CATEGORY_KEYWORDS), axis=1
    )
    filtered = filtered.sort_values("score", ascending=False)
    log.info("✅  %d rows remain after filtering", len(filtered))
    return filtered


# --------------------------------------------------------------------------- #
#  Main                                                                       #
# --------------------------------------------------------------------------- #


def main() -> None:
    log.info("🚀 Starting job screener…")

    newest_obj = latest_object(S3_BUCKET, S3_PREFIX)
    if not newest_obj:
        log.error("No objects found under s3://%s/%s", S3_BUCKET, S3_PREFIX)
        sys.exit(1)

    key = newest_obj["Key"]
    tmp_csv = download_to_tmp(S3_BUCKET, key, Path("/tmp/jobs_raw.csv"))

    df = load_csv(tmp_csv)
    filtered = filter_and_rank(df)

    if not filtered.empty:
        filtered.to_csv(OUT_FILE, index=False)
        log.info("💾  Saved %d rows → %s", len(filtered), OUT_FILE)
    else:
        log.info("No rows to save – exiting gracefully.")


if __name__ == "__main__":
    main()
