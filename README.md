# job-screener
Daily AI-powered job lead screener that posts high-quality roles to Slack.
# === main.py ===
import os
import re
import time
import json
import logging
import openai
import requests
import schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# === Load Config ===
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
CACHE_FILE = "seen_jobs.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Utils ===
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

# === Scoring ===
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
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content
        score = extract_score(content)
        return score, content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: Error in scoring."

# === Slack Poster ===
def send_to_slack(message):
    try:
        payload = {"text": message}
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Slack error: {e}")

# === Job Feed: Remote OK ===
def fetch_remoteok_jobs():
    try:
        response = requests.get("https://remoteok.com/api")
        jobs = response.json()[1:]
        return [
            {
                "title": job.get("position"),
                "description": clean_html(job.get("description", ""))[:1000],
                "url": f"https://remoteok.com{job.get('url')}"
            }
            for job in jobs if "data" in job.get("position", "").lower()
        ]
    except Exception as e:
        logging.error(f"RemoteOK error: {e}")
        return []

# === Job Feed: Y Combinator ===
def fetch_ycombinator_jobs():
    try:
        soup = BeautifulSoup(requests.get("https://www.ycombinator.com/jobs").text, "html.parser")
        return [
            {
                "title": jc.select_one("h2").text.strip(),
                "description": jc.select_one("p").text.strip() if jc.select_one("p") else "",
                "url": f"https://www.ycombinator.com{jc['href']}"
            }
            for jc in soup.select("a[class*=JobPreview_jobPreview]")
            if "data" in jc.select_one("h2").text.lower() and "intern" not in jc.select_one("h2").text.lower()
        ]
    except Exception as e:
        logging.error(f"YC error: {e}")
        return []

# === Job Feed: Google Jobs (via SerpAPI) ===
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

# === Combine All Feeds ===
def gather_jobs():
    return fetch_remoteok_jobs() + fetch_ycombinator_jobs() + fetch_google_jobs()

# === Main Job Runner ===
def main():
    seen_jobs = load_seen_jobs()
    new_seen = set(seen_jobs)
    for job in gather_jobs():
        if job["url"] in seen_jobs:
            logging.info(f"Skipped (duplicate): {job['title']}")
            continue

        score, explanation = score_job(job)
        logging.info(f"{job['title']} - Scored {score}/10")

        if score >= 7:
            msg = f"*📢 {job['title']}*\n<{job['url']}|View job post>\n\n*Score:* {score}/10\n{explanation}"
            send_to_slack(msg)
            new_seen.add(job["url"])
        else:
            logging.info(f"Skipped (low score): {job['title']} ({score}/10)")

        time.sleep(2)

    save_seen_jobs(new_seen)

schedule.every().day.at("09:00").do(main)

if __name__ == "__main__":
    logging.info("Job Screener started. Waiting for scheduled run...")
    while True:
        schedule.run_pending()
        time.sleep(30)


# === requirements.txt ===
openai
requests
python-dotenv
beautifulsoup4
schedule


# === Dockerfile ===
FROM python:3.10-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"]

git add Dockerfile
git commit -m "Add Dockerfile"
git push origin main


# === .gitignore ===
.env
__pycache__/
seen_jobs.json
*.pyc
*.pyo
*.pyd
.Python
env/
build/
dist/
*.egg-info/
