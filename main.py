# === main.py (Enhanced with Company Name + Clean URL Output) ===
import os, json, time, logging, re, requests, schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyairtable.api import Api
from datetime import datetime
import openai

# === Load Environment ===
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
BRIGHT_DATA_API_TOKEN = os.getenv("BRIGHT_DATA_API_TOKEN")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
BRIGHT_DATA_DATASET_ID = "hl_7ccd6dac"
CACHE_FILE = "seen_jobs.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Company Filter ===
RELEVANT_ACCOUNTS = [
    "Netflix", "Spotify", "ESPN", "Rockstar Games", "Electronic Arts", "Charter",
    "T-Mobile", "Kroger", "Allstate", "Meta", "Apple", "CVS", "Pfizer", "Vanguard",
    "AbbVie", "Intel", "Samsung", "P&G", "Proctor & Gamble", "Disney", "NBCUniversal"
]

# === Utilities ===
def extract_score(text):
    match = re.search(r"Score:\s*(\d+)/10", text)
    return int(match.group(1)) if match else 0

def clean_html(raw_html):
    return BeautifulSoup(raw_html, "html.parser").get_text()

def load_seen_jobs():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_seen_jobs(seen):
    with open(CACHE_FILE, "w") as f:
        json.dump(list(seen), f)

# === Bright Data Scraper ===
def fetch_brightdata_jobs():
    url = f"https://api.brightdata.com/dca/dataset?id={BRIGHT_DATA_DATASET_ID}"
    headers = {"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        jobs = []
        for item in data:
            title = item.get("title", "Untitled")
            company = item.get("company", "")
            if any(account.lower() in company.lower() for account in RELEVANT_ACCOUNTS):
                jobs.append({
                    "title": title,
                    "company": company,
                    "description": item.get("description", ""),
                    "url": item.get("url", "")
                })
        logging.info(f"Bright Data returned {len(jobs)} relevant job(s)")
        return jobs
    except Exception as e:
        logging.error(f"Bright Data error: {e}")
        return []

# === Scoring via OpenAI ===
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

# === Airtable Output ===
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

# === Job Gatherer ===
def gather_jobs():
    return fetch_brightdata_jobs()

# === Main Runner ===
def main():
    try:
        logging.info("🚀 Job screener starting...")
        seen_jobs = load_seen_jobs()
        new_seen = set(seen_jobs)
        jobs = gather_jobs()
        logging.info(f"Fetched {len(jobs)} total job(s)")

        scored_count = 0
        max_scores_per_day = 5

        for job in jobs:
            if job["url"] in seen_jobs:
                logging.info(f"Skipped (duplicate): {job['title']}")
                continue

            if scored_count >= max_scores_per_day:
                logging.info("✅ Reached daily scoring limit (5 jobs)")
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
