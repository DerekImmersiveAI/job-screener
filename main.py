import os, json, time, logging, re, requests, schedule, boto3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyairtable import Api
from datetime import datetime
from openai import OpenAI
import pandas as pd

# === Load Environment Variables ===
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")

client = OpenAI(api_key=OPENAI_API_KEY)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Scoring ===
def extract_score(text):
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Relevance to Data Science
- Remote work option
- Seniority (prefer senior roles)
- Salary ($140k+ preferred)

Job:
Title: {job.get('job_title')}
Company: {job.get('company_name')}
Location: {job.get('job_location')}
Description: {job.get('job_summary')}

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
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

# === Airtable Push ===
def push_to_airtable(job, score, reason):
    try:
        table = Api(AIRTABLE_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        fields = {
            "job_title": job.get("job_title", ""),
            "company_name": job.get("company_name", ""),
            "job_location": job.get("job_location", ""),
            "job_summary": job.get("job_summary", ""),
            "job_function": job.get("job_function", ""),
            "job_industries": job.get("job_industries", ""),
            "job_base_pay_range": job.get("job_base_pay_range", ""),
            "url": job.get("url", ""),
            "job_posted_time": job.get("job_posted_time", ""),
            "job_num_applicants": job.get("job_num_applicants", ""),
            "job_posted_date": job.get("job_posted_date", ""),
            "job_poster": job.get("job_poster", ""),
            "base_salary": job.get("base_salary", ""),
            "Score": score,
            "Reason": reason
        }
        table.create(fields)
        logging.info(f"✅ Added to Airtable: {job.get('job_title')} at {job.get('company_name')}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

# === S3 Download ===
def download_latest_s3_file():
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=S3_BUCKET)
    objects = response.get("Contents", [])
    json_files = [obj for obj in objects if obj["Key"].endswith(".json")]
    if not json_files:
        raise Exception("No JSON files found in S3.")
    latest = max(json_files, key=lambda x: x["LastModified"])
    logging.info(f"📥 Downloaded {latest['Key']} to brightdata_latest.json")
    s3.download_file(S3_BUCKET, latest["Key"], "brightdata_latest.json")

# === Load and Score Jobs ===
def main():
    try:
        logging.info("🚀 Starting job screener...")
        download_latest_s3_file()
        with open("brightdata_latest.json", "r") as f:
            jobs = json.load(f)

        if isinstance(jobs, dict) and "results" in jobs:
            jobs = jobs["results"]

        logging.info(f"📥 Loaded {len(jobs)} jobs from JSON")
        for job in jobs:
            score, reason = score_job(job)
            logging.info(f"🧠 GPT score: {score}/10")
            push_to_airtable(job, score, reason)
            time.sleep(5)

    except Exception as e:
        logging.error(f"❌ Job screener failed: {e}")

# === Scheduler ===
schedule.every().day.at("09:00").do(main)

if __name__ == "__main__":
    main()
    while True:
        schedule.run_pending()
        time.sleep(30)
