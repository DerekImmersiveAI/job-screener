import os
import json
import logging
import re
import requests
import boto3
import openai
from dotenv import load_dotenv
from pyairtable import Table
from datetime import datetime

# === Load environment ===
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET = os.getenv("AWS_BUCKET")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Scoring ===
def extract_score(text):
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Relevance to 'Data Science'
- Seniority (prefer senior roles)
- Remote work option
- Salary (prefer $140k+)

Title: {job.get('job_title')}
Company: {job.get('company_name')}
Location: {job.get('job_location')}
Description: {job.get('job_summary')}

Respond in this format:
Score: X/10
Reason: [short reason]
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content.strip()
        score = extract_score(content)
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

# === Airtable Integration ===
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
            "job_posted_date": job.get("job_posted_date"),
            "job_poster": job.get("job_poster", {}).get("name"),
            "base_salary": job.get("base_salary", {}).get("min_amount"),
            "Score": score,
            "Reason": reason
        }

        # remove nulls/empty
        fields = {k: v for k, v in fields.items() if v not in [None, ""]}

        table.create(fields)
        logging.info(f"✅ Added to Airtable: {fields.get('job_title')} at {fields.get('company_name')}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

# === S3 Download ===
def download_latest_json_from_s3(bucket):
    s3 = boto3.client("s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        objects = s3.list_objects_v2(Bucket=bucket, Prefix="")["Contents"]
        latest_obj = max(objects, key=lambda x: x["LastModified"])
        key = latest_obj["Key"]
        logging.info(f"📥 Downloading {key} from S3...")
        s3.download_file(bucket, key, "brightdata_latest.json")
        return "brightdata_latest.json"
    except Exception as e:
        logging.error(f"S3 download error: {e}")
        return None

# === Main Process ===
def main():
    logging.info("🚀 Starting job screener...")
    file_path = download_latest_json_from_s3(AWS_BUCKET)
    if not file_path:
        return

    try:
        with open(file_path, "r") as f:
            jobs = json.load(f)
            logging.info(f"📥 Loaded {len(jobs)} jobs from JSON")
    except Exception as e:
        logging.error(f"Failed to load JSON file: {e}")
        return

    for job in jobs:
        try:
            score, reason = score_job(job)
            logging.info(f"🧠 GPT score: {score}/10")
            push_to_airtable(job, score, reason)
        except Exception as e:
            logging.error(f"Error processing job: {e}")

if __name__ == "__main__":
    main()
