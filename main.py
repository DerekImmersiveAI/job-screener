import os, json, time, logging, re, requests, schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import openai

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
CACHE_FILE = "seen_jobs.json"

def extract_score(text): match = re.search(r"Score:\s*(\d+)/10", text); return int(match.group(1)) if match else 0
def clean_html(html): return BeautifulSoup(html, "html.parser").get_text()
def load_seen(): return set(json.load(open(CACHE_FILE))) if os.path.exists(CACHE_FILE) else set()
def save_seen(seen): json.dump(list(seen), open(CACHE_FILE, "w"))

def score_job(job):
    prompt = f"""You are an AI job screener. Rate this job 1–10 based on:
- Data Science relevance
- Seniority (prefer senior)
- Remote-friendly
- Salary ($140k+ preferred)

Title: {job['title']}
Description: {job['description']}
Respond in this format:
Score: X/10
Reason: [short reason]"""
    try:
        res = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        content = res.choices[0].message.content
        return extract_score(content), content
    except Exception as e:
        logging.error(f"OpenAI error: {e}")
        return 0, "Score: 0/10\nReason: API error."

def send_to_slack(msg):
    try: requests.post(SLACK_WEBHOOK_URL, json={"text": msg}).raise_for_status()
    except Exception as e: logging.error(f"Slack error: {e}")

def fetch_remoteok():
    try:
        jobs = requests.get("https://remoteok.com/api").json()[1:]
        return [{"title": j["position"], "description": clean_html(j.get("description", "")), "url": f"https://remoteok.com{j['url']}"} for j in jobs if "data" in j.get("position", "").lower()]
    except Exception as e: logging.error(f"RemoteOK error: {e}"); return []

def fetch_yc():
    try:
        soup = BeautifulSoup(requests.get("https://www.ycombinator.com/jobs").text, "html.parser")
        return [{"title": j.select_one("h2").text.strip(), "description": j.select_one("p").text.strip(), "url": f"https://www.ycombinator.com{j['href']}"} for j in soup.select("a[class*=JobPreview_jobPreview]") if "data" in j.select_one("h2").text.lower()]
    except Exception as e: logging.error(f"YC error: {e}"); return []

def fetch_google_jobs():
    try:
        res = requests.get("https://serpapi.com/search", params={
            "engine": "google_jobs", "q": "data scientist remote", "api_key": SERPAPI_KEY, "hl": "en"
        }).json()
        return [{"title": j["title"], "description": j["description"], "url": j.get("related_links", [{}])[0].get("link", "")} for j in res.get("jobs_results", [])]
    except Exception as e: logging.error(f"Google Jobs error: {e}"); return []

def gather_jobs(): return fetch_remoteok() + fetch_yc() + fetch_google_jobs()

def main():
    seen = load_seen()
    new_seen = set(seen)
    for job in gather_jobs():
        if job["url"] in seen: continue
        score, reason = score_job(job)
        if score >= 7:
            msg = f"*📢 {job['title']}*\n<{job['url']}|View job post>\n\n*Score:* {score}/10\n{reason}"
            send_to_slack(msg)
            new_seen.add(job["url"])
        time.sleep(2)
    save_seen(new_seen)

schedule.every().day.at("09:00").do(main)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Job screener started")
    while True:
        schedule.run_pending()
        time.sleep(30)
