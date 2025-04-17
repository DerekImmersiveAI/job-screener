import os
import json
import time
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime
import boto3
import pandas as pd
import openai
from pyairtable import Table

# === Load Environment Variables ===
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === S3 Utilities ===
def download_latest_json():
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]
    if not files:
        raise Exception("No JSON files found in S3.")
    latest_file = max(files)
    s3.download_file(AWS_BUCKET_NAME, latest_file, "brightdata_latest.json")
    logging.info(f"✅ Downloaded {latest_file} to brightdata_latest.json")

# === GPT Scoring ===
def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Relevance to Data Science
- Seniority (prefer senior roles)
- Remote work flexibility
- Salary (prefer $140k+)

Job Details:
Title: {job.get("job_title", "")}
Company: {job.get("company_name", "")}
Location: {job.get("job_location", "")}
Summary: {job.get("job_summary", "")}
Industries: {job.get("job_industries", "")}
Function: {job.get("job_function", "")}
Base Pay Range: {job.get("job_base_pay_range", "")}

Respond in this format:
Score: X/10
Reason: [short explanation]
    """.strip()

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content.strip()
        score = int(content.split("Score:")[1].split("/")[0].strip())
        reason = content.split("Reason:")[1].strip() if "Reason:" in content else ""
        return score, reason
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Reason: Error in scoring."

# === Airtable Insertion ===
def push_to_airtable(job, score, reason):
    try:
        table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        fields = {
            "job_title": job.get("job_title", ""),
            "company_name": job.get("company_name", ""),
            "job_location": job.get("job_location", ""),
            "job_summary": job.get("job_summary", ""),
            "job_function": job.get("job_function", ""),
            "job_industries": job.get("job_industries", ""),
            "job_base_pay_range": job.get("job_base_pay_range", ""),
            "url": job.get("url", ""),
            "job_posting_id": job.get("job_posting_id", ""),
            "job_posted_time": job.get("job_posted_time", ""),
            "job_num_applicants": job.get("job_num_applicants", ""),
            "job_posted_date": job.get("job_posted_date", ""),
            "job_poster": job.get("job_poster", ""),
            "base_salary": job.get("base_salary", ""),
            "Score": score,
            "Reason": reason,
        }
        table.create(fields)
        logging.info(f"✅ Added to Airtable: {job.get('job_title')} at {job.get('company_name')}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

# === Main Process ===
def main():
    try:
        logging.info("🚀 Starting job screener...")
        download_latest_json()
        with open("brightdata_latest.json", "r") as f:
            data = json.load(f)

        if isinstance(data, dict) and "data" in data:
            jobs = data["data"]
        elif isinstance(data, list):
            jobs = data
        else:
            raise ValueError("Unexpected JSON format")

        logging.info(f"📥 Loaded {len(jobs)} jobs from JSON")

        for job in jobs:
            score, reason = score_job(job)
            logging.info(f"🧠 GPT score: {score}/10")
            push_to_airtable(job, score, reason)
            time.sleep(1)

    except Exception as e:
        logging.error(f"🚨 Job screener failed: {e}")

if __name__ == "__main__":
    main()
