# === main.py (Airtable-Ready Version, No Slack) ===
import os, json, time, logging, re, requests, schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyairtable import Table
from datetime import datetime
import openai

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
CACHE_FILE = "seen_jobs.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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

def score_job(job):
    prompt = f"""
You are an AI job screener. Rate this job on a scale from 1 to 10 based on:
- Role relevance to 'Data Science'
- Seniority (prefer senior roles)
- Remote work option
- Salary (prefer $140k+)
Here’s the job:

Title: {job['title']}
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
        table = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        table.create({
            "Title": job["title"],
            "URL": job["url"],
            "Score": score,
            "Reason": reason,
            "Date": datetime.utcnow().isoformat()
        })
        logging.info(f"✅ Added to Airtable: {job['title']}")
    except Exception as e:
        logging.error(f"❌ Airtable error: {e}")

def fetch_remoteok_jobs():
    try:
        response = requests.get("https://remoteok.com/api")
        jobs = response.json()[1:]
        results = [
            {
                "title": job.get("position"),
                "description": clean_html(job.get("description", ""))[:1000],
                "url": f"https://remoteok.com{job.get('url')}"
            }
            for job in jobs if "data" in job.get("position", "").lower()
        ]
        logging.info(f"RemoteOK returned {len(results)} job(s)")
        return results
    except Exception as e:
        logging.error(f"RemoteOK error: {e}")
        return []

def fetch_ycombinator_jobs():
    try:
        soup = BeautifulSoup(requests.get("https://www.ycombinator.com/jobs").text, "html.parser")
        jobs = [
            {
                "title": jc.select_one("h2").text.strip(),
                "description": jc.select_one("p").text.strip() if jc.select_one("p") else "",
                "url": f"https://www.ycombinator.com{jc['href']}"
            }
            for jc in soup.select("a[class*=JobPreview_jobPreview]")
            if "data" in jc.select_one("h2").text.lower() and "intern" not in jc.select_one("h2").text.lower()
        ]
        logging.info(f"Y Combinator returned {len(jobs)} job(s)")
        return jobs
    except Exception as e:
        logging.error(f"YC error: {e}")
        return []

def fetch_google_jobs(query="data scientist", location="remote"):
    try:
        params = {
            "engine": "google_jobs",
            "q": f"{query} {location}",
            "hl": "en",
            "api_key": SERPAPI_KEY
        }
        response = requests.get("https://serpapi.com/search", params=params)
        jobs = response.json().get("jobs_results", [])
        logging.info(f"Google Jobs returned {len(jobs)} job(s)")
        return [
            {
                "title": job["title"],
                "description": job["description"],
                "url": job.get("related_links", [{}])[0].get("link", "")
            } for job in jobs
        ]
    except Exception as e:
        logging.error(f"Google Jobs error: {e}")
        return []

def gather_jobs():
    return fetch_remoteok_jobs() + fetch_ycombinator_jobs() + fetch_google_jobs()

def main():
    try:
        logging.info("🚀 Job screener starting...")
        logging.info(f"OpenAI key present: {bool(openai.api_key)}")
        logging.info(f"SerpAPI key present: {bool(SERPAPI_KEY)}")

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

            logging.info(f"Scoring job: {job['title']}")
            score, explanation = score_job(job)
            scored_count += 1

            logging.info(f"{job['title']} - Scored {score}/10")
            logging.info(f"{job['url']}")
            logging.info(f"{explanation}\n")

            push_to_airtable(job, score, explanation)
            new_seen.add(job["url"])
            time.sleep(20)  # Prevent OpenAI rate limit

        save_seen_jobs(new_seen)
        logging.info("✅ Job screener finished.")

    except Exception as e:
        logging.error(f"MAIN FUNCTION CRASHED: {e}")

schedule.every().day.at("09:00").do(main)

if __name__ == "__main__":
    logging.info("📅 Job screener triggered (manual or scheduled)")
    main()
    while True:
        schedule.run_pendi
