import os, csv, time, logging, re, schedule
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pyairtable.api import Api
from openai import OpenAI
import boto3

# === Load Environment Variables ===
load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")  # e.g. "job-screener"
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
openai_client = OpenAI()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Download Latest CSV from S3 ===
def download_latest_s3_csv(bucket_name, prefix="", timeout_minutes=90):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    logging.info("⏳ Waiting for CSV file in S3...")
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
        logging.info("🔄 No file yet. Retrying in 60s...")
        time.sleep(60)

    logging.error("❌ Timeout: No CSV appeared in S3.")
    return None

# === Load CSV Data ===
def load_jobs_from_csv(file_path):
    jobs = []
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            jobs.append(row)
    logging.info(f"📥 Loaded {len(jobs)} jobs from CSV")
    return jobs

# === GPT Scoring ===
def extract_score(text):
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Relevance to Data Science
- Seniority (prefer senior roles)
- Remote-friendly
- Salary ($140k+)

Title: {job.get('job_title', 'Untitled')}
Company: {job.get('company_name', 'Unknown')}
Location: {job.get('job_location', '')}
Description: {job.get('job_summary', '')}

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
        logging.info(f"🧠 GPT score: {score}/10")
        return score, content
    except Exception as e:
        logging.error(f"❌ GPT error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

# === Push to Airtable ===
def push_to_airtable(job, score, reason):
    try:
        table = Api(AIRTABLE_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        fields = {
            "title": job.get("job_title", "Untitled"),
            "company_name": job.get("company_name", "Unknown"),
            "job_location": job.get("job_location", ""),
            "job_summary": job.get("job_summary", ""),
            "url": job.get("url", ""),
            "Score": score,
            "Reason": reason
        }
        table.create(fields)
        logging.info(f"✅ Added to Airtable: {fields['title']} at {fields['company_name']}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

# === Main Process ===
def main():
    logging.info("🚀 Starting job screener...")

    csv_file = download_latest_s3_csv(S3_BUCKET_NAME)
    if not csv_file:
        logging.error("❌ No CSV file found in S3.")
        return

    jobs = load_jobs_from_csv(csv_file)
    for job in jobs:
        score, reason = score_job(job)
        if score >= 7:
            push_to_airtable(job, score, reason)
        time.sleep(20)

    logging.info("✅ Job screener completed.")

# === Schedule Daily Run ===
schedule.every().day.at("09:00").do(main)

if __name__ == "__main__":
    main()
    while True:
        schedule.run_pending()
        time.sleep(30)
