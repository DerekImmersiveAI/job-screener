# === main.py (Bright Data → S3 → GPT v1 → Airtable) ===
import os, json, time, logging, re, requests, schedule, boto3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyairtable.api import Api
from datetime import datetime, timedelta
from openai import OpenAI

client = OpenAI()

# === Load Environment ===
load_dotenv()
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "brightdata-job-screener")
BRIGHTDATA_API_TOKEN = os.getenv("BRIGHTDATA_API_TOKEN")
CACHE_FILE = "seen_jobs.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Trigger Bright Data Scrape ===
def trigger_brightdata_scrape():
    url = "https://api.brightdata.com/datasets/v3/trigger"
    headers = {
        "Authorization": f"Bearer {BRIGHTDATA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    params = {
        "dataset_id": "gd_lpfll7v5hcqtkxl6l",
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": "url"
    }
    data = {
        "deliver": {
            "type": "s3",
            "filename": {"template": "{[snapshot_id]}", "extension": "json"},
            "bucket": S3_BUCKET_NAME,
            "credentials": {
                "aws-access-key": AWS_ACCESS_KEY_ID,
                "aws-secret-key": AWS_SECRET_ACCESS_KEY
            },
            "directory": ""
        },
        "input": [
            {
                "url": "https://www.linkedin.com/jobs/search/?currentJobId=4188551209&f_C=4680&f_TPR=r604800&geoId=103644278&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true"
            }
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

# === S3 Downloader with Long Polling ===
def download_latest_s3_file(bucket_name, prefix="", timeout_minutes=90):
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    logging.info("⏳ Waiting for Bright Data file in S3...")
    deadline = datetime.utcnow() + timedelta(minutes=timeout_minutes)

    while datetime.utcnow() < deadline:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        all_files = response.get("Contents", [])
        if all_files:
            latest = max(all_files, key=lambda x: x["LastModified"])
            key = latest["Key"]
            file_path = "latest_scrape.json"
            s3.download_file(bucket_name, key, file_path)
            logging.info(f"✅ Downloaded {key} to {file_path}")
            return file_path

        logging.info("🔄 No file yet. Retrying in 60 seconds...")
        time.sleep(60)

    logging.error("❌ Timeout: No file appeared in S3 within the wait window.")
    return None

# === Load + Clean JSON Jobs ===
def load_jobs_from_json(file_path):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        jobs = []
        for item in data:
            jobs.append({
                "title": item.get("title", "Untitled"),
                "company": item.get("company", "Unknown"),
                "description": item.get("description", ""),
                "url": item.get("url", "")
            })
        return jobs
    except Exception as e:
        logging.error(f"❌ JSON load error: {e}")
        return []

# === Utilities ===
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

Title: {job['title']}
Company: {job.get('company', 'Unknown')}
Description: {job['description']}

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
        print("🔎 GPT response:", content)
        score = extract_score(content)
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

def push_to_airtable(job, score, reason):
    try:
        fields = {
            "Title": job.get("job_title", "Untitled"),
            "Company": job.get("company_name", "Unknown"),
            "Location": job.get("job_location", ""),
            "Description": job.get("job_summary", ""),
            "URL": job.get("url", ""),
            "employment_type": job.get("job_employment_type", ""),
            "Score": score,
            "Reason": reason,
            "Date": datetime.utcnow().date().isoformat()
        }

        posted_at = job.get("job_posted_date")
        if posted_at:
            fields["posted_at"] = posted_at

        table = Api(AIRTABLE_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        table.create(fields)
        logging.info(f"✅ Added to Airtable: {fields['Title']} at {fields['Company']}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

# === Main Logic ===
def main():
    try:
        logging.info("🚀 Job screener starting...")
        if not trigger_brightdata_scrape():
            return

        file_path = download_latest_s3_file(S3_BUCKET_NAME)
        if not file_path:
            return

        seen_jobs = set()
        if os.path.exists(CACHE_FILE):
            seen_jobs = json.load(open(CACHE_FILE))
        new_seen = set(seen_jobs)

        jobs = load_jobs_from_json(file_path)
        logging.info(f"Fetched {len(jobs)} jobs from Bright Data")

        scored_count = 0
        max_scores_per_day = 20

        for job in jobs:
            if job["url"] in seen_jobs:
                logging.info(f"Skipped (duplicate): {job['title']}")
                continue

            if scored_count >= max_scores_per_day:
                logging.info("✅ Reached daily scoring limit (20 jobs)")
                break

            logging.info(f"Scoring job: {job['title']} at {job.get('company', 'Unknown')}")
            score, explanation = score_job(job)
            scored_count += 1

            logging.info(f"{job['title']} - Scored {score}/10")
            logging.info(f"{job['url']}")
            logging.info(f"{explanation}\n")

            push_to_airtable(job, score, explanation)
            new_seen.add(job["url"])
            time.sleep(20)

        with open(CACHE_FILE, "w") as f:
            json.dump(list(new_seen), f)
        logging.info("✅ Job screener finished.")
    except Exception as e:
        logging.error(f"MAIN FUNCTION CRASHED: {e}")

schedule.every().day.at("09:00").do(main)

if __name__ == "__main__":
    logging.info("📅 Job screener triggered (manual or scheduled)")
    main()
    while True:
        schedule.run_pending()
        time.sleep(30)
