import os, json, time, logging, re, requests, schedule
import boto3
from dotenv import load_dotenv
from pyairtable import Table
from datetime import datetime

# === Load environment variables ===
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def extract_score(text):
    if not text:
        return 0
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def fetch_latest_json_from_s3():
    try:
        s3 = boto3.client("s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME)
        files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]
        latest_file = max(files, key=lambda x: s3.head_object(Bucket=AWS_BUCKET_NAME, Key=x)["LastModified"])
        logging.info(f"📥 Downloaded {latest_file} to brightdata_latest.json")
        s3.download_file(Bucket=AWS_BUCKET_NAME, Key=latest_file, Filename="brightdata_latest.json")
    except Exception as e:
        logging.error(f"S3 download error: {e}")
        raise

def score_job(job):
    try:
        import openai
        openai.api_key = OPENAI_API_KEY

        content = f"""You are a job matching assistant. Rate the relevance of the following job to a senior data scientist profile on a scale from 1 to 10.
Job Title: {job['job_title']}
Company: {job['company_name']}
Location: {job['job_location']}
Summary: {job['job_summary']}

Respond in this format:
Score: X/10
Reason: [why this score]
"""
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": content}]
        )
        explanation = response.choices[0].message.content
        score = extract_score(explanation)
        return score, explanation
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, None

def push_to_airtable(job, score, reason):
    try:
        table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
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
        logging.info(f"✅ Added to Airtable: {job['job_title']} at {job['company_name']}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

def main():
    try:
        logging.info("🚀 Starting job screener...")
        fetch_latest_json_from_s3()

        with open("brightdata_latest.json") as f:
            job_data = json.load(f)

        jobs = job_data if isinstance(job_data, list) else job_data.get("data", [])
        logging.info(f"📊 Loaded {len(jobs)} jobs from JSON")

        for job in jobs:
            score, reason = score_job(job)
            logging.info(f"🧠 GPT score: {score}/10")
            if reason:
                push_to_airtable(job, score, reason)
            time.sleep(1.5)

    except Exception as e:
        logging.error(f"❌ Job screener failed: {e}")

schedule.every().day.at("09:00").do(main)

if __name__ == "__main__":
    logging.info("🏃 Running 'python main.py'")
    main()
    while True:
        schedule.run_pending()
        time.sleep(30)
