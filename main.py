import os, json, time, logging, re, requests, csv, schedule
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pyairtable.api import Api
from openai import OpenAI
import boto3

# === Load Environment ===
load_dotenv()
BRIGHTDATA_API_TOKEN = os.getenv("BRIGHTDATA_API_TOKEN")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
openai_client = OpenAI()
CACHE_FILE = "seen_jobs.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Bright Data Trigger ===
def trigger_brightdata_scrape():
    url = "https://api.brightdata.com/datasets/v3/trigger"
    headers = {
        "Authorization": f"Bearer {BRIGHTDATA_API_TOKEN}",
        "Content-Type": "application/json",
    }
    params = {
        "dataset_id": "gd_lpfll7v5hcqtkxl6l",
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": "url",
    }
    data = {
        "deliver": {
            "type": "s3",
            "filename": {"template": "{[snapshot_id]}", "extension": "csv"},
            "bucket": S3_BUCKET_NAME,
            "credentials": {
                "aws-access-key": AWS_ACCESS_KEY_ID,
                "aws-secret-key": AWS_SECRET_ACCESS_KEY
            },
            "directory": ""
        },
        "input": [
            {"url": "https://www.linkedin.com/jobs/search/?currentJobId=4168640988&f_C=4680&geoId=103644278"}
        ]
    }

    try:
        response = requests.post(url, headers=headers, params=params, json=data)
        response.raise_for_status()
        logging.info(f"✅ Bright Data scrape triggered: {response.json()}")
        return True
    except Exception as e:
        logging.error(f"❌ Bright Data trigger failed: {e}")
        return False

# === Download CSV from S3 ===
def download_latest_s3_csv(bucket_name, prefix="", timeout_minutes=90):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    logging.info("⏳ Waiting for CSV file from Bright Data...")
    deadline = datetime.utcnow() + timedelta(minutes=timeout_minutes)

    while datetime.utcnow() < deadline:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        contents = response.get("Contents", [])
        csv_files = [f for f in contents if f["Key"].endswith(".csv")]
        if csv_files:
            latest = max(csv_files, key=lambda x: x["LastModified"])
            key = latest["Key"]
            path = "brightdata_latest.csv"
            s3.download_file(bucket_name, key, path)
            logging.info(f"✅ Downloaded {key} to {path}")
            return path
        logging.info("🔄 No file yet, retrying in 60s...")
        time.sleep(60)

    logging.error("❌ Timeout: No CSV appeared in S3.")
    return None

# === GPT Scoring ===
def extract_score(text):
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Relevance to 'Data Science'
- Seniority (prefer senior roles)
- Remote-friendly
- Salary (prefer $140k+)

Job:
Title: {job['title']}
Company: {job['company_name']}
Location: {job['job_location']}
Description: {job['job_summary']}

Respond in this format:
Score: X/10
Reason: [short reason]
"""
    try:
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content
        score = extract_score(content)
        logging.info(f"🔍 GPT scored {job['title']} at {job['company_name']}: {score}/10")
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

# === Airtable Output ===
def push_to_airtable(job, score, reason):
    try:
        table = Api(AIRTABLE_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        fields = {
            "title": job.get("title", ""),
            "company_name": job.get("company_name", ""),
            "job_location": job.get("job_location", ""),
            "job_summary": job.get("job_summary", ""),
            "url": job.get("url", ""),
            "Score": score,
            "Reason": reason,
            "Date": datetime.utcnow().date().isoformat()
        }
        if job.get("job_posted_date"):
            fields["job_posted_date"] = job["job_posted_date"]

        table.create(fields)
        logging.info(f"✅ Added to Airtable: {fields['title']} at {fields['company_name']}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

# === CSV Parser ===
def load_jobs_from_csv(file_path):
    jobs = []
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            jobs.append(row)
    logging.info(f"📥 Loaded {len(jobs)} jobs from CSV")
    return jobs

# === Main Runner ===
def main():
    logging.info("🚀 Starting job screener...")

    if not trigger_brightdata_scrape():
        logging.error("❌ Could not trigger Bright Data scrape. Exiting.")
        return

    csv_file = download_latest_s3_csv(S3_BUCKET_NAME)
    if not csv_file:
        logging.error("❌ Could not download CSV from S3. Exiting.")
        return

    jobs = load_jobs_from_csv(csv_file)
    for job in jobs:
        score, reason = score_job(job)
        if score >= 7:
            push_to_airtable(job, score, reason)
        time.sleep(20)

    logging.info("✅ All done.")

# === Schedule ===
schedule.every().day.at("09:00").do(main)

if __name__ == "__main__":
    main()
    while True:
        schedule.run_pending()
        time.sleep(30)
