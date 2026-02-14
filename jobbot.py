import os
import time
import requests
from typing import Dict, List, Set, Any, Optional

# -----------------------------
# ENV (GitHub Secrets)
# -----------------------------
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()

# Accept either secret name so you never get stuck again:
# - SLACK_WEBHOOK_URL (recommended)
# - SLACK_WEBHOOK (what your workflow currently uses)
SLACK_WEBHOOK_URL = (
    os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    or os.environ.get("SLACK_WEBHOOK", "").strip()
)

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")
if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL (or SLACK_WEBHOOK) secret in GitHub Actions.")

# -----------------------------
# RapidAPI JSearch (host you used in console)
# -----------------------------
API_URL = "https://jsearch27.p.rapidapi.com/search"
HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch27.p.rapidapi.com",
}

# -----------------------------
# What you asked for: ALL TECH ROLES (grouped)
# NOTE: We filter to Canada ONLY after results come back.
# -----------------------------
ROLE_QUERIES: List[str] = [
    # Software / Web / Mobile
    '("software engineer" OR "software developer" OR "frontend developer" OR "backend developer" OR "full stack developer" OR "mobile developer" OR "ios developer" OR "android developer")',

    # Data / BI / Analytics
    '("data analyst" OR "business intelligence" OR "bi analyst" OR "analytics engineer" OR "data engineer" OR "sql developer")',

    # ML / AI
    '("machine learning engineer" OR "ml engineer" OR "ai engineer" OR "data scientist" OR "applied scientist")',

    # Cloud / DevOps / SRE / Platform
    '("devops engineer" OR "site reliability engineer" OR "sre" OR "cloud engineer" OR "cloud architect" OR "platform engineer" OR "kubernetes" OR "terraform")',

    # Cybersecurity
    '("cybersecurity" OR "security engineer" OR "security analyst" OR "soc analyst" OR "incident response" OR "iam" OR "identity access management")',

    # Product / Program / Delivery
    '("product manager" OR "product owner" OR "technical product owner" OR "technical program manager" OR "program manager" OR "project manager" OR "scrum master" OR "delivery manager")',

    # BA / QA / Implementation / Solutions
    '("business analyst" OR "technical analyst" OR "requirements analyst" OR "qa engineer" OR "test analyst" OR "automation tester" OR "implementation specialist" OR "solutions consultant")',

    # CRM / ERP / Dynamics / Salesforce / Power Platform
    '("salesforce administrator" OR "salesforce developer" OR "dynamics 365" OR "power platform" OR "power bi" OR "crm analyst" OR "erp analyst" OR "implementation consultant")',

    # IT Support / Systems / Network (still tech roles)
    '("systems administrator" OR "system administrator" OR "network engineer" OR "it support" OR "help desk" OR "desktop support" OR "cloud administrator" OR "windows administrator")',
]

# -----------------------------
# Settings (tuned to avoid 429)
# -----------------------------
DATE_POSTED = "week"      # today / 3days / week / month / all
NUM_PAGES = 1             # keep 1 to reduce rate limit
SLEEP_BETWEEN_QUERIES = 7 # seconds between each search call
MAX_POSTS_TO_SLACK = 30   # avoid massive spam

# -----------------------------
# Helpers
# -----------------------------
def slack_post(text: str) -> None:
    """Send a message to Slack via incoming webhook."""
    payload = {"text": text}
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)
    r.raise_for_status()

def request_with_backoff(params: Dict[str, Any], max_retries: int = 5) -> Optional[Dict[str, Any]]:
    """
    Call JSearch with backoff. If rate-limited (429), wait and retry.
    Returns JSON dict or None if all retries fail.
    """
    delay = 10
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=60)

            # Handle rate limit
            if resp.status_code == 429:
                time.sleep(delay)
                delay = min(delay * 2, 90)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException:
            # Do not spam Slack with errors; just retry.
            time.sleep(delay)
            delay = min(delay * 2, 90)

    return None

def normalize(s: str) -> str:
    return (s or "").strip()

def is_canada_job(job: Dict[str, Any]) -> bool:
    """
    HARD FILTER: Canada only.
    JSearch often returns US even if query says 'in Canada'.
    This ensures only Canadian jobs are posted.
    """
    country = normalize(job.get("job_country", "")).upper()
    return country == "CA"

def job_key(job: Dict[str, Any]) -> str:
    """Unique key to dedupe jobs."""
    return normalize(job.get("job_id", "")) or normalize(job.get("job_apply_link", ""))

def format_job(job: Dict[str, Any], query_label: str) -> str:
    title = normalize(job.get("job_title", ""))
    company = normalize(job.get("employer_name", ""))
    city = normalize(job.get("job_city", ""))
    state = normalize(job.get("job_state", ""))
    publisher = normalize(job.get("job_publisher", ""))
    is_remote = job.get("job_is_remote", False)
    remote_label = "Remote" if is_remote else "On-site/Hybrid"
    link = normalize(job.get("job_apply_link", ""))

    location = ", ".join([p for p in [city, state, "CA"] if p])
    header = f"*{title}* | {company}"
    meta = f"{location} • {remote_label} • {publisher}"
    return f"{header}\n{meta}\n{link}\n"

# -----------------------------
# Main
# -----------------------------
def main() -> None:
    all_found: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for q in ROLE_QUERIES:
        params = {
            "query": f"{q} in Canada",
            "page": 1,
            "num_pages": NUM_PAGES,
            "date_posted": DATE_POSTED,
            "remote_jobs_only": False,
        }

        data = request_with_backoff(params)
        if not data:
            # skip silently (no Slack errors)
            time.sleep(SLEEP_BETWEEN_QUERIES)
            continue

        jobs = data.get("data", []) or []

        # Filter to Canada ONLY
        canada_jobs = [j for j in jobs if is_canada_job(j)]

        # Dedupe + collect
        for job in canada_jobs:
            k = job_key(job)
            if not k or k in seen:
                continue
            seen.add(k)
            # store query label for formatting context
            job["_query_label"] = q
            all_found.append(job)

        time.sleep(SLEEP_BETWEEN_QUERIES)

    # Sort newest first if timestamp exists
    all_found.sort(key=lambda x: x.get("job_posted_at_timestamp", 0), reverse=True)

    if not all_found:
        slack_post("No Canada tech jobs found in this run.")
        return

    # Limit how many we post
    to_post = all_found[:MAX_POSTS_TO_SLACK]

    # Build one clean Slack message (avoid many webhook calls)
    chunks: List[str] = []
    for job in to_post:
        chunks.append(format_job(job, job.get("_query_label", "")))

    message = "🇨🇦 *Aidrr Tech Jobs (Canada Only)*\n\n" + "\n".join(chunks)
    slack_post(message)

    slack_post(f"✅ Done. Posted {len(to_post)} Canada jobs (hard-filtered).")

if __name__ == "__main__":
    main()
