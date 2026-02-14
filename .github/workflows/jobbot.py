import os
import json
import hashlib
from datetime import datetime, timezone

import requests

STATE_FILE = "state.json"

# --------------- helpers ---------------

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"seen": []}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def h(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def slack_post(webhook_url: str, text: str):
    r = requests.post(webhook_url, json={"text": text}, timeout=30)
    r.raise_for_status()

def safe_get(d, key, default=""):
    v = d.get(key, default)
    return v if v is not None else default

# --------------- JSearch ---------------

def jsearch_query(api_key: str, query: str, location: str = "Canada", page: int = 1, num_pages: int = 1):
    """
    JSearch endpoint used by the PixelForge API on RapidAPI.
    """
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{query} in {location}",
        "page": str(page),
        "num_pages": str(num_pages),
        "date_posted": "today",  # we will also run every 6h; keeps it fresh
        "remote_jobs_only": "true",
        "employment_types": "FULLTIME,CONTRACT",
    }
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }
    res = requests.get(url, headers=headers, params=params, timeout=30)
    res.raise_for_status()
    return res.json()

def format_job(job: dict) -> str:
    title = safe_get(job, "job_title", "Untitled role")
    company = safe_get(job, "employer_name", "Company not listed")
    city = safe_get(job, "job_city", "")
    state = safe_get(job, "job_state", "")
    country = safe_get(job, "job_country", "")
    location = ", ".join([x for x in [city, state, country] if x]).strip() or "Remote / Not specified"

    apply_link = safe_get(job, "job_apply_link", "") or safe_get(job, "job_google_link", "") or safe_get(job, "job_link", "")
    posted = safe_get(job, "job_posted_at_datetime_utc", "")

    # Optional salary fields if present
    sal_min = safe_get(job, "job_min_salary", "")
    sal_max = safe_get(job, "job_max_salary", "")
    sal_period = safe_get(job, "job_salary_period", "")
    salary_line = ""
    if sal_min or sal_max:
        salary_line = f"\nSalary: {sal_min}–{sal_max} ({sal_period})".replace("–", "–")

    posted_line = f"\nPosted: {posted.replace('T',' ').replace('Z',' UTC')}" if posted else ""

    return (
        f"*{title}*\n"
        f"Company: {company}\n"
        f"Location: {location}"
        f"{posted_line}"
        f"{salary_line}\n"
        f"Apply: {apply_link}"
    )

# --------------- main ---------------

def main():
    api_key = os.environ.get("RAPIDAPI_KEY")
    webhook = os.environ.get("SLACK_WEBHOOK_URL")

    if not api_key:
        raise SystemExit("Missing RAPIDAPI_KEY (GitHub Secret)")
    if not webhook:
        raise SystemExit("Missing SLACK_WEBHOOK_URL (GitHub Secret)")

    # One channel feed: curated tech roles
    # (We keep it focused and premium)
    queries = [
        # Business + Product + Data
        "Business Analyst OR IT Business Analyst OR Technical Business Analyst OR Systems Analyst",
        "Product Owner OR Technical Product Owner OR Product Manager",
        "Data Analyst OR BI Analyst OR Business Intelligence Analyst OR Reporting Analyst OR SQL Analyst",
        "Power BI Developer OR Tableau Developer",

        # Engineering
        "Software Engineer OR Software Developer OR Backend Developer OR Full Stack Developer OR Frontend Developer",
        "QA Automation Engineer OR DevOps Engineer",

        # Security
        "Cybersecurity Analyst OR SOC Analyst OR Security Engineer OR GRC Analyst OR IT Risk Analyst OR IAM Analyst",
    ]

    state = load_state()
    seen = set(state.get("seen", []))

    new_jobs = []

    for q in queries:
        data = jsearch_query(api_key, q, location="Canada", page=1, num_pages=1)
        for job in data.get("data", []):
            # Use a stable unique key: apply link or job_id
            unique = safe_get(job, "job_id", "") or safe_get(job, "job_apply_link", "") or safe_get(job, "job_link", "")
            if not unique:
                continue
            key = h(unique)
            if key in seen:
                continue
            seen.add(key)
            new_jobs.append(job)

    # Keep state from growing forever
    state["seen"] = list(seen)[-3000:]
    save_state(state)

    if not new_jobs:
        print("No new jobs found.")
        return

    # Limit messages so Slack does not get spammy
    new_jobs = new_jobs[:20]

    header = f"🧲 *Aidrr Tech Jobs (New Today)* — {len(new_jobs)} new roles"
    body = "\n\n".join(format_job(j) for j in new_jobs)

    slack_post(webhook, f"{header}\n\n{body}")
    print(f"Posted {len(new_jobs)} jobs.")

if __name__ == "__main__":
    main()
