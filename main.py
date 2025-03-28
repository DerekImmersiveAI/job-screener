# === main.py (Bright Data Trigger → S3 → GPT → Airtable) ===
import os, json, time, logging, re, requests, schedule, boto3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyairtable.api import Api
from datetime import datetime
import openai

# === Load Environment ===
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
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
        "discover_by": "url",
        "limit_per_input": "100"
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
            "directory": "linkedin/json/"
        },
        "input": [
            {
                "url": "https://www.linkedin.com/jobs/search/?currentJobId=4192793413&f_C=..."
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

# === S3 Downloader ===
def download_latest_s3_file(bucket_name, prefix="linkedin/json/"):
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        all_files = response.get("Contents", [])
        latest = max(all_files, key=lambda x: x["LastModified"])
        key = latest["Key"]
        file_path = "latest_scrape.json"
        s3.download_file(bucket_name, key, file_path)
        logging.info(f"✅ Downloaded {key} to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"❌ S3 download error: {e}")
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
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content
        score = extract_score(content)
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

def push_to_airtable(job, score, reason):
    try:
        table = Api(AIRTABLE_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        table.create({
            "Title": job["title"],
            "Company": job.get("company", "Unknown"),
            "URL": job["url"],
            "Score": score,
            "Reason": reason,
            "Date": datetime.utcnow().date().isoformat()
        })
        logging.info(f"✅ Added to Airtable: {job['title']} at {job.get('company', 'Unknown')}")
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
            seen_jobs = load_seen_jobs()
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

        save_seen_jobs(new_seen)
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
