import os
import json
import logging
import time
import boto3
import openai
import pandas as pd
from pyairtable import Table
from datetime import datetime

# === Load config from environment ===
openai.api_key = os.getenv("OPENAI_API_KEY")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET = os.getenv("AWS_BUCKET_NAME")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_latest_json_from_s3():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        response = s3.list_objects_v2(Bucket=AWS_BUCKET)
        files = response.get("Contents", [])
        json_files = [f for f in files if f["Key"].endswith(".json")]
        latest = max(json_files, key=lambda f: f["LastModified"])
        logging.info(f"📥 Downloaded {latest['Key']} to brightdata_latest.json")
        s3.download_file(AWS_BUCKET, latest["Key"], "brightdata_latest.json")
        return "brightdata_latest.json"
    except Exception as e:
        logging.error(f"S3 download error: {e}")
        return None

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Role relevance to 'Data Science'
- Seniority (prefer senior roles)
- Remote work option
- Salary (prefer $140k+)

Job Title: {job.get("job_title")}
Company: {job.get("company_name")}
Summary: {job.get("job_summary")}
Location: {job.get("job_location")}
Salary: {job.get("base_salary")}
Description: {job.get("job_description")}

Respond in this format:
Score: X/10
Reason: [short reason]
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content.strip()
        score_line = next((line for line in content.splitlines() if "Score" in line), "Score: 0/10")
        score = int(score_line.split(":")[1].split("/")[0].strip())
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, f"Score: 0/10\nReason: OpenAI error: {e}"

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
    logging.info("🏃 Starting job screener...")
    filepath = fetch_latest_json_from_s3()
    if not filepath:
        logging.error("🚨 Job screener failed: no file retrieved from S3.")
        return

    with open(filepath, "r") as f:
        jobs = json.load(f)

    if isinstance(jobs, dict) and "data" in jobs:
        jobs = jobs["data"]

    logging.info(f"📊 Loaded {len(jobs)} jobs from JSON")

    for job in jobs:
        score, reason = score_job(job)
        logging.info(f"🧠 GPT score: {score}/10")
        push_to_airtable(job, score, reason)
        time.sleep(1)

if __name__ == "__main__":
    main()
