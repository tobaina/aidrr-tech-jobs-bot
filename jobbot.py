import os
import time
import requests

# =========================
# ENV (GitHub Secrets)
# =========================
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

# =========================
# RapidAPI JSearch Settings
# =========================
API_URL = "https://jsearch27.p.rapidapi.com/search"
HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch27.p.rapidapi.com",
}

DATE_POSTED = os.environ.get("DATE_POSTED", "week")  # allowed: all, today, 3days, week, month
PAGES_PER_QUERY = int(os.environ.get("PAGES_PER_QUERY", "2"))  # increase if you want more results per role

# =========================
# ALL TECH ROLES (Canada-only)
# =========================
TECH_ROLE_QUERIES = [
    # Software / Engineering
    "software engineer in Canada",
    "senior software engineer in Canada",
    "full stack developer in Canada",
    "frontend developer in Canada",
    "backend developer in Canada",
    "mobile developer in Canada",
    "ios developer in Canada",
    "android developer in Canada",

    # Cloud / DevOps / SRE
    "devops engineer in Canada",
    "site reliability engineer in Canada",
    "cloud engineer in Canada",
    "cloud architect in Canada",
    "solutions architect in Canada",

    # Data
    "data analyst in Canada",
    "business intelligence analyst in Canada",
    "power bi developer in Canada",
    "data engineer in Canada",
    "analytics engineer in Canada",
    "data scientist in Canada",
    "machine learning engineer in Canada",

    # Security / Infrastructure
    "cybersecurity analyst in Canada",
    "information security analyst in Canada",
    "security engineer in Canada",
    "network engineer in Canada",
    "systems administrator in Canada",
    "system analyst in Canada",
    "IT analyst in Canada",

    # Product / Project / Agile
    "business analyst in Canada",
    "technical business analyst in Canada",
    "product manager in Canada",
    "technical product manager in Canada",
    "product owner in Canada",
    "project manager in Canada",
    "technical project manager in Canada",
    "scrum master in Canada",

    # QA / Testing
    "qa analyst in Canada",
    "quality assurance engineer in Canada",
    "automation tester in Canada",

    # Enterprise Applications
    "salesforce administrator in Canada",
    "salesforce developer in Canada",
    "dynamics 365 consultant in Canada",
    "d365 business analyst in Canada",
    "sap analyst in Canada",
    "servicenow developer in Canada",
]

# =========================
# Helpers
# =========================
def jsearch_request(query: str, page: int = 1, num_pages: int = 1):
    params = {
        "query": query,
        "page": str(page),
        "num_pages": str(num_pages),
        "date_posted": DATE_POSTED,
        "remote_jobs_only": "false",  # includes on-site/hybrid/remote
    }
    resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()

def format_job(job: dict) -> str:
    title = job.get("job_title") or "Unknown title"
    company = job.get("employer_name") or "Unknown company"
    city = job.get("job_city") or ""
    state = job.get("job_state") or ""
    country = job.get("job_country") or ""
    location = ", ".join([x for x in [city, state, country] if x]).strip() or "Location not listed"

    link = job.get("job_apply_link") or job.get("job_google_link") or ""
    publisher = job.get("job_publisher") or ""
    remote = job.get("job_is_remote")

    work_mode = "Remote" if remote is True else "On-site/Hybrid"
    pub_text = f" • {publisher}" if publisher else ""

    # Slack-friendly
    msg = f"*{title}* | {company}\n{location} • {work_mode}{pub_text}\n{link}".strip()
    return msg

def post_to_slack(jobs: list):
    # Canada-only HARD filter
    canada_jobs = []
    for job in jobs:
        jc = (job.get("job_country") or "").upper().strip()
        if jc not in ("CA", "CANADA"):
            continue
        canada_jobs.append(job)

    if not canada_jobs:
        print("No Canadian jobs found after filtering.")
        return

    # Slack message size limits → post in chunks
    messages = [format_job(j) for j in canada_jobs]
    chunk_size = 25

    total_posted = 0
    for i in range(0, len(messages), chunk_size):
        chunk = messages[i:i + chunk_size]

        payload = {
            "text": "🇨🇦 *Tech Jobs in Canada (Latest)*\n\n" + "\n\n".join(chunk)
        }
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=60)
        r.raise_for_status()

        total_posted += len(chunk)
        print(f"Posted {len(chunk)} jobs to Slack (running total: {total_posted}).")

        time.sleep(1)

    # final confirmation ping
    requests.post(SLACK_WEBHOOK_URL, json={"text": f"✅ Done. Posted {total_posted} Canada jobs."}, timeout=60)

def main():
    all_jobs_by_id = {}

    for q in TECH_ROLE_QUERIES:
        try:
            data = jsearch_request(query=q, page=1, num_pages=PAGES_PER_QUERY)
            jobs = data.get("data", []) or []
            print(f"Query: {q} -> {len(jobs)} results (before dedupe/filter)")

            for job in jobs:
                job_id = job.get("job_id")
                if not job_id:
                    continue
                all_jobs_by_id[job_id] = job

            time.sleep(1)  # be nice to API rate limits

        except requests.HTTPError as e:
            print(f"HTTP error for query '{q}': {e}")
        except Exception as e:
            print(f"Error for query '{q}': {e}")

    all_jobs = list(all_jobs_by_id.values())
    print(f"Total unique jobs pulled: {len(all_jobs)}")

    post_to_slack(all_jobs)

if __name__ == "__main__":
    main()
