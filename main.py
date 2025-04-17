import os
import json
import time
import logging
import requests
import openai
import pandas as pd
import boto3
from datetime import datetime
from pyairtable import Table
from dotenv import load_dotenv

# === Load environment variables ===
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
REGION_NAME = os.getenv("REGION_NAME", "us-east-1")

# === Logging setup ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Helper functions ===
def get_latest_json_file_from_s3(bucket, prefix=""):
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY,
                      region_name=REGION_NAME)
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        raise FileNotFoundError("No files found in S3 bucket.")
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
    logging.info(f"📥 Downloaded {latest_file['Key']} from S3")
    s3.download_file(bucket, latest_file["Key"], "brightdata_latest.json")
    return "brightdata_latest.json"

def load_jobs_from_json(path):
    with open(path, "r") as f:
        data = json.load(f)
    if isinstance(data, dict) and "result" in data:
        return data["result"]
    return data

def score_job(job):
    prompt = f"""
You are an AI assistant helping screen job listings for data science relevance. Score the job from 1–10 based on:

- Relevance to Data Science
- Seniority (prefer senior roles)
- Salary (prefer $140k+)
- Remote flexibility

Job Title: {job.get("job_title", "")}
Company: {job.get("company_name", "")}
Location: {job.get("location", "")}
Description: {job.get("job_summary", "")}

Respond in this format:
Score: X/10
Reason: [short reason]
"""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content
        score_line = next((line for line in content.splitlines() if "Score:" in line), "Score: 0/10")
        reason_line = next((line for line in content.splitlines() if "Reason:" in line), "Reason: Not specified.")
        score = int(score_line.split(":")[1].strip().split("/")[0])
        reason = reason_line.split(":", 1)[1].strip()
        return score, reason
    except Exception as e:
        logging.error(f"Error in scoring: {e}")
        return 0, "Error in scoring."

def push_to_airtable(job, score, reason):
    try:
        table = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        fields = {
            "job_title": job.get("job_title", ""),
            "company_name": job.get("company_name", ""),
            "location": job.get("location", ""),
            "job_summary": job.get("job_summary", ""),
            "Score": score,
            "Reason": reason,
            "URL": job.get("job_url", ""),
            "job_type": job.get("job_type", ""),
            "job_industries": job.get("job_industries", "")
        }
        table.create(fields)
        logging.info(f"✅ Added to Airtable: {fields['job_title']} at {fields['company_name']}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

def main():
    try:
        logging.info("🚀 Starting job screener...")
        file_path = get_latest_json_file_from_s3(S3_BUCKET)
        jobs = load_jobs_from_json(file_path)
        logging.info(f"📄 Loaded {len(jobs)} jobs from JSON")

        for job in jobs:
            score, reason = score_job(job)
            logging.info(f"🧠 GPT score: {score}/10")
            push_to_airtable(job, score, reason)
            time.sleep(1)
    except Exception as e:
        logging.error(f"MAIN ERROR: {e}")

if __name__ == "__main__":
    main()
