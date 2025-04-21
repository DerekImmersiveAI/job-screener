import os
import csv
import logging
import time
import boto3
from datetime import datetime
from pyairtable import Table

# === Load config from environment ===
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET = os.getenv("AWS_BUCKET_NAME")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_latest_csv_from_s3():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        response = s3.list_objects_v2(Bucket=AWS_BUCKET)
        files = response.get("Contents", [])
        csv_files = [f for f in files if f["Key"].endswith(".csv")]
        latest = max(csv_files, key=lambda f: f["LastModified"])
        logging.info(f"📥 Downloaded {latest['Key']} to brightdata_latest.csv")
        s3.download_file(AWS_BUCKET, latest["Key"], "brightdata_latest.csv")
        return "brightdata_latest.csv"
    except Exception as e:
        logging.error(f"S3 download error: {e}")
        return None

def score_job(job):
    try:
        posted_time = job.get("job_posted_time")
        poster = job.get("job_poster")
        salary_str = str(job.get("base_salary", "")).replace(",", "").replace("$", "")
        try:
            salary = int("".join(filter(str.isdigit, salary_str)))
        except ValueError:
            salary = 0

        days_since_posted = 999
        if posted_time:
            try:
                post_date = datetime.strptime(posted_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                days_since_posted = (datetime.utcnow() - post_date).days
            except Exception:
                pass

        score = 0
        reasons = []

        if days_since_posted <= 7:
            score += 4
            reasons.append("Job posted recently")
        else:
            reasons.append("Job is older than 7 days")

        if poster and isinstance(poster, str) and len(poster.strip()) > 0:
            score += 3
            reasons.append("Has job poster listed")
        else:
            reasons.append("Missing job poster")

        if salary >= 140000:
            score += 3
            reasons.append("Salary above $140k")
        else:
            reasons.append("Salary below $140k or unspecified")

        return score, "Reason: " + "; ".join(reasons)
    except Exception as e:
        logging.error(f"Scoring error: {e}")
        return 0, f"Reason: Error in scoring: {e}"

def push_to_airtable(job, score, reason):
    try:
        table = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        fields = {
            "job_title": job.get("job_title"),
            "company_name": job.get("company_name"),
            "job_location": job.get("job_location"),
            "job_summary": job.get("job_summary"),
            "job_function": job.get("job_function"),
            "job_industries": job.get("job_industries"),
            "job_base_pay_range": job.get("job_base_pay_range"),
            "url": job.get("url"),
            "job_posted_time": job.get("job_posted_time"),
            "job_num_applicants": job.get("job_num_applicants"),
            "Score": score,
            "Reason": reason,
        }

        poster = job.get("job_poster")
        if isinstance(poster, str) and len(poster.strip()) > 0 and len(poster.strip()) <= 255:
            fields["job_poster"] = poster.strip()

        table.create(fields)
        logging.info(f"✅ Added to Airtable: {job.get('job_title')} at {job.get('company_name')}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

def main():
    logging.info("🚀 Starting job screener...")
    filepath = fetch_latest_csv_from_s3()
    if not filepath:
        logging.error("❌ Job screener failed: no file retrieved from S3.")
        return

    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        jobs = list(reader)

    logging.info(f"📊 Loaded {len(jobs)} jobs from CSV")

    for job in jobs:
        score, reason = score_job(job)
        logging.info(f"🧠 Score: {score}/10")
        push_to_airtable(job, score, reason)
        time.sleep(1)

if __name__ == "__main__":
    main()
