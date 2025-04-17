import os
import openai
from openai import OpenAI
from dotenv import load_dotenv
from pyairtable import Table
from datetime import datetime
import json
import logging
import time
import re
import boto3

# Load environment
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=openai.api_key)

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def download_latest_file_from_s3():
    try:
        s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME)
        files = response.get("Contents", [])
        if not files:
            raise Exception("❌ No files found in S3 bucket.")
        latest_file = max(files, key=lambda x: x["LastModified"])["Key"]
        logging.info(f"📥 Downloaded {latest_file} to brightdata_latest.json")
        s3.download_file(Bucket=AWS_BUCKET_NAME, Key=latest_file, Filename="brightdata_latest.json")
        return "brightdata_latest.json"
    except Exception as e:
        logging.error(f"S3 download error: {e}")
        raise

def load_jobs(filepath):
    try:
        with open(filepath, "r") as f:
            jobs = json.load(f)
        logging.info(f"📂 Loaded {len(jobs)} jobs from JSON")
        return jobs
    except Exception as e:
        logging.error(f"Error loading JSON: {e}")
        return []

def extract_score(text):
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Role relevance to 'Data Science'
- Seniority (prefer senior roles)
- Remote work option
- Salary (prefer $140k+)

Here’s the job:

Title: {job.get('job_title', 'Untitled')}
Company: {job.get('company_name', 'Unknown')}
Location: {job.get('job_location', 'Unknown')}
Description: {job.get('job_summary', 'No description')}

Respond in this format:
Score: X/10
Reason: [short reason]
    """
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content
        score = extract_score(content)
        logging.info(f"🧠 GPT score: {score}/10")
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

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
            "job_posted_date": job.get("job_posted_date"),
            "job_num_applicants": job.get("job_num_applicants"),
            "job_poster": job.get("job_poster"),
            "base_salary": job.get("base_salary"),
            "Score": score,
            "Reason": reason,
        }
        table.create(fields)
        logging.info(f"✅ Added to Airtable: {job.get('job_title')} at {job.get('company_name')}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

def main():
    logging.info("🚀 Starting job screener...")
    filepath = download_latest_file_from_s3()
    jobs = load_jobs(filepath)
    for job in jobs:
        score, reason = score_job(job)
        push_to_airtable(job, score, reason)
        time.sleep(2)

if __name__ == "__main__":
    main()
