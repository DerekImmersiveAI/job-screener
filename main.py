import os
import json
import logging
import re
import time
import requests
import boto3
import openai
from datetime import datetime
from pyairtable import Table
from dotenv import load_dotenv

# === Load Environment Variables ===
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET = os.getenv("AWS_BUCKET")

openai.api_key = OPENAI_API_KEY

# === Configure Logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Helper Functions ===

def extract_score(text):
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def download_latest_s3_file():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        response = s3.list_objects_v2(Bucket=AWS_BUCKET)
        all_files = response.get("Contents", [])
        if not all_files:
            raise Exception("No files found in S3 bucket.")

        latest_file = max(all_files, key=lambda x: x["LastModified"])["Key"]
        logging.info(f"📥 Downloaded {latest_file} to brightdata_latest.json")

        s3.download_file(AWS_BUCKET, latest_file, "brightdata_latest.json")
        return "brightdata_latest.json"
    except Exception as e:
        logging.error(f"S3 download error: {e}")
        raise

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Role relevance to 'Data Science'
- Seniority (prefer senior roles)
- Remote work option
- Salary (prefer $140k+)
Here’s the job:

Title: {job.get('job_title')}
Company: {job.get('company_name')}
Location: {job.get('job_location')}
Summary: {job.get('job_summary')}
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content
        score = extract_score(content)
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Error in scoring."

def is_valid_date(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except:
        return False

# === Main Job Screener ===

def main():
    logging.info("🚀 Starting job screener...")

    try:
        filepath = download_latest_s3_file()
        with open(filepath, "r") as f:
            jobs = json.load(f)

        logging.info(f"📂 Loaded {len(jobs)} jobs from JSON")

        table = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

        for job in jobs:
            score, reason = score_job(job)
            logging.info(f"🧠 GPT score: {score}/10")

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
                "base_salary": job.get("base_salary"),
                "Score": score,
                "Reason": reason,
            }

            # Handle job_poster with length validation
           poster = job.get("job_poster")
if isinstance(poster, str):
    poster = poster.strip()
    if 0 < len(poster) <= 255:
        fields["job_poster"] = poster


            # Only include valid dates for 'job_posted_date'
            posted_date = job.get("job_posted_date")
            if is_valid_date(posted_date):
                fields["job_posted_date"] = posted_date

            try:
                table.create(fields)
                logging.info(f"✅ Added to Airtable: {fields['job_title']} at {fields['company_name']}")
            except Exception as e:
                logging.error(f"❌ Airtable error: {e}")

            time.sleep(2)

    except Exception as e:
        logging.error(f"💥 Job screener failed: {e}")

# === Execute ===

if __name__ == "__main__":
    main()
