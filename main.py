import os
import json
import time
import logging
import re
import requests
import boto3
from datetime import datetime
from pyairtable import Table
from dotenv import load_dotenv
import openai

# === Load Environment Variables ===
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

openai.api_key = OPENAI_API_KEY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Utility Functions ===
def extract_score(text):
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Relevance to data science
- Seniority (prefer senior roles)
- Remote flexibility
- Salary ($140k+ preferred)

Job Details:
Title: {job.get("job_title", "N/A")}
Company: {job.get("company_name", "N/A")}
Location: {job.get("job_location", "N/A")}
Summary: {job.get("job_summary", "N/A")}
Description: {job.get("job_description", "N/A")}

Respond in this format:
Score: X/10
Reason: [short explanation]
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        message = response.choices[0].message.content.strip()
        score = extract_score(message)
        return score, message
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

def get_latest_json_from_s3():
    s3 = boto3.client("s3")
    try:
        files = s3.list_objects_v2(Bucket=S3_BUCKET_NAME).get("Contents", [])
        json_files = [f for f in files if f["Key"].endswith(".json")]
        latest_file = max(json_files, key=lambda x: x["LastModified"])
        s3.download_file(S3_BUCKET_NAME, latest_file["Key"], "brightdata_latest.json")
        logging.info(f"✅ Downloaded {latest_file['Key']} to brightdata_latest.json")
        return "brightdata_latest.json"
    except Exception as e:
        logging.error(f"S3 download error: {e}")
        return None

def push_to_airtable(job, score, reason):
    try:
        table = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        fields = {
            "job_title": job.get("job_title"),
            "company_name": job.get("company_name"),
            "job_location": job.get("job_location"),
            "job_description": job.get("job_description"),
            "job_summary": job.get("job_summary"),
            "job_link": job.get("job_link"),
            "job_industries": job.get("job_industries"),
            "Score": score,
            "Reason": reason,
            "Date": datetime.utcnow().strftime("%Y-%m-%d")
        }
        table.create(fields)
        logging.info(f"✅ Added to Airtable: {fields['job_title']} at {fields['company_name']}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

def main():
    logging.info("🚀 Starting job screener...")
    file_path = get_latest_json_from_s3()
    if not file_path:
        return

    with open(file_path, "r") as f:
        jobs = json.load(f)

    logging.info(f"📥 Loaded {len(jobs)} jobs from JSON")

    for job in jobs:
        score, reason = score_job(job)
        logging.info(f"🧠 GPT score: {score}/10")
        push_to_airtable(job, score, reason)
        time.sleep(1)  # Rate limiting

if __name__ == "__main__":
    main()
