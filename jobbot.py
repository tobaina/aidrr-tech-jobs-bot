import os
import time
import requests

# ------------------------
# ENV (GitHub Secrets)
# ------------------------
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()

# Accept either secret name so you never get stuck again
SLACK_WEBHOOK_URL = (
    os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    or os.environ.get("SLACK_WEBHOOK", "").strip()
)

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")
if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL (or SLACK_WEBHOOK) secret in GitHub Actions.")

# ------------------------
# RapidAPI JSearch Endpoint (use the host that works in your RapidAPI console)
# Your screenshot shows: jsearch27.p.rapidapi.com
# ------------------------
API_URL = "https://jsearch27.p.rapidapi.com/search"
HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch27.p.rapidapi.com",
}

# ------------------------
# Tech roles (grouped to avoid an overly long URL)
# NOTE: We force Canada in the query, then we ALSO filter response to job_country == "CA"
# ------------------------
ROLE_QUERIES = [
    # Software / Engineering
    '("software engineer" OR "software developer" OR "senior software engineer" OR "full stack developer" OR "backend developer" OR "frontend developer" OR "mobile developer" OR "ios developer" OR "android developer") in Canada',

    # Cloud / DevOps / SRE
    '("devops engineer" OR "site reliability engineer" OR "sre" OR "cloud engineer" OR "cloud architect" OR "platform engineer" OR "kubernetes" OR "terraform") in Canada',

    # Data / Analytics / ML
    '("data analyst" OR "business intelligence" OR "bi analyst" OR "analytics engineer" OR "data engineer" OR "machine learning engineer" OR "ml engineer") in Canada',

    # Cybersecurity
    '("cybersecurity" OR "security engineer" OR "security analyst" OR "soc analyst" OR "incident response" OR "iam" OR "identity access management") in Canada',

    # IT / Systems / Network
    '("systems administrator" OR "system administrator" OR "network engineer" OR "it support" OR "help desk" OR "desktop support" OR "cloud administrator" OR "windows administrator") in Canada',

    # Product / Project / Delivery
    '("product manager" OR "product owner" OR "technical product owner" OR "project manager" OR "program manager" OR "scrum master" OR "delivery manager") in Canada',

    # BA / QA / Implementation
    '("business analyst" OR "technical analyst" OR "requirements analyst" OR "qa engineer" OR "test analyst" OR "automation tester" OR "implementation specialist" OR "solutions consultant") in Canada',

    # CRM / ERP / Salesforce
    '("salesforce administrator" OR "salesforce developer" OR "dynamics 365" OR "power platform" OR "crm analyst" OR "erp analyst" OR "implementation consultant") in Canada',
]

DATE_POSTED = "week"     # options: all, today, 3days, week, month
NUM_PAGES = 2            # pull more results per query, then filter down
MAX_POSTS_TOTAL = 25     # cap what you send to Slack each run
SLEEP_BETWEEN_CALLS = 1.0


def slack_post(text: str) -> None:
    payload = {"text": text}
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)
    r.raise_for_status()


def fetch_jobs(query: str) -> list[dict]:
    params = {
        "query": query,
        "page": "1",
        "num_pages": str(NUM_PAGES),
        "date_posted": DATE_POSTED,
        "remote_jobs_only": "false",
    }
    resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", []) or []


def is_canada_job(job: dict) -> bool:
    # Most reliable field is job_country (typically "CA" for Canada)
    jc = (job.get("job_country") or "").strip().upper()
    if jc == "CA":
        return True

    # Backup checks (sometimes APIs are inconsistent)
    city = (job.get("job_city") or "").lower()
    state = (job.get("job_state") or "").lower()
    title = (job.get("job_title") or "").lower()
    desc = (job.get("job_description") or "").lower()

    canada_hints = ["canada", "ontario", "toronto", "vancouver", "calgary", "edmonton", "montreal", "ottawa", "waterloo", "kitchener", "gta"]
    text_blob = " ".join([city, state, title, desc])
    return any(h in text_blob for h in canada_hints)


def job_key(job: dict) -> str:
    # stable dedupe key
    return (job.get("job_id") or "") + "|" + (job.get("job_apply_link") or "")


def format_job(job: dict) -> str:
    title = (job.get("job_title") or "Untitled").strip()
    company = (job.get("employer_name") or "Unknown Company").strip()
    city = (job.get("job_city") or "").strip()
    state = (job.get("job_state") or "").strip()
    country = (job.get("job_country") or "").strip()
    publisher = (job.get("job_publisher") or "").strip()
    is_remote = job.get("job_is_remote")

    location_bits = [b for b in [city, state, country] if b]
    location = ", ".join(location_bits) if location_bits else "Canada (location not listed)"
    remote_tag = "Remote" if is_remote else "On-site/Hybrid"

    link = (job.get("job_apply_link") or job.get("job_google_link") or "").strip()
    if not link:
        link = "Link not provided"

    return f"*{title}* | {company}\n{location} • {remote_tag} • {publisher}\n{link}"


def main():
    all_jobs: list[dict] = []
    seen = set()

    for q in ROLE_QUERIES:
        try:
            jobs = fetch_jobs(q)
        except Exception as e:
            slack_post(f"⚠️ JSearch error for query:\n`{q}`\nError: {e}")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        for job in jobs:
            k = job_key(job)
            if not k or k in seen:
                continue
            seen.add(k)

            if is_canada_job(job):
                all_jobs.append(job)

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Sort by newest if available
    def posted_ts(j: dict) -> int:
        return int(j.get("job_posted_at_timestamp") or 0)

    all_jobs.sort(key=posted_ts, reverse=True)

    if not all_jobs:
        slack_post("✅ Ran Jobs Bot (Canada-only). No Canada jobs matched today/week for the selected role groups.")
        return

    # Post top N
    to_post = all_jobs[:MAX_POSTS_TOTAL]
    for job in to_post:
        slack_post(format_job(job))

    slack_post(f"✅ Done. Posted {len(to_post)} Canada jobs (filtered).")


if __name__ == "__main__":
    main()
