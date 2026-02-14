import os
import time
import requests

# ----------------------------
# ENV (GitHub Secrets)
# ----------------------------
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()

# Accept either secret name (so you never get stuck again)
SLACK_WEBHOOK_URL = (
    os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    or os.environ.get("SLACK_WEBHOOK", "").strip()
)

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")
if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL (or SLACK_WEBHOOK) secret in GitHub Actions.")

# ----------------------------
# RapidAPI JSearch (use the host that worked in your console)
# If your console shows a different host, replace both lines below.
# ----------------------------
API_URL = "https://jsearch27.p.rapidapi.com/search"
HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch27.p.rapidapi.com",
}

# ----------------------------
# SETTINGS
# ----------------------------
DATE_POSTED = "week"      # today | 3days | week | month | all
PAGES_PER_QUERY = 1       # Keep low to avoid rate limits
MAX_POSTS_TOTAL = 15      # Total jobs sent to Slack per run
SLEEP_BETWEEN_CALLS = 1.2 # seconds, helps avoid throttling

# ----------------------------
# ROLES (ALL roles we discussed) — Canada focused, NOT remote-only
# We split them into small batches so the URL never becomes gigantic.
# ----------------------------
ROLE_GROUPS = [
    # Core Tech
    ["software engineer", "full stack developer", "backend developer", "frontend developer", "mobile developer"],
    ["ios developer", "android developer", "devops engineer", "site reliability engineer", "cloud engineer"],
    ["data analyst", "business intelligence analyst", "data engineer", "machine learning engineer", "ai engineer"],
    ["cybersecurity analyst", "security engineer", "network engineer", "systems administrator", "it support specialist"],
    ["qa engineer", "automation tester", "test analyst", "solutions architect", "enterprise architect"],

    # Product / Delivery
    ["business analyst", "requirements analyst", "systems analyst", "technical business analyst", "data business analyst"],
    ["product owner", "technical product owner", "scrum master", "project manager", "program manager", "delivery manager"],

    # ERP / Apps
    ["erp analyst", "dynamics 365", "microsoft dynamics", "sap analyst", "netsuite", "salesforce administrator", "salesforce developer"],
    ["implementation specialist", "application analyst", "integration analyst", "crm analyst"],

    # Finance / Ops (since you asked earlier too)
    ["accounts receivable", "accounts payable", "billing specialist", "cash application specialist", "collections specialist", "reconciliation analyst"],
    ["operations analyst", "process analyst", "process improvement", "business operations"],
]

def slack_post(text: str) -> None:
    payload = {"text": text}
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)
    resp.raise_for_status()

def build_query(terms):
    # Canada focus (not remote-only)
    # You can change "in Canada" to "in Ontario, Canada" if you want tighter results.
    joined = " OR ".join([f'"{t}"' for t in terms])
    return f'({joined}) in Canada'

def fetch_jobs(query: str):
    params = {
        "query": query,
        "page": "1",
        "num_pages": str(PAGES_PER_QUERY),
        "date_posted": DATE_POSTED,
        "remote_jobs_only": "false",
    }

    r = requests.get(API_URL, headers=HEADERS, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data.get("data", []) or []

def format_job(job: dict) -> str:
    title = job.get("job_title") or job.get("job_job_title") or "Untitled role"
    company = job.get("employer_name") or "Unknown company"
    city = job.get("job_city") or ""
    state = job.get("job_state") or ""
    country = job.get("job_country") or ""
    location = ", ".join([x for x in [city, state, country] if x]).strip() or "Location not listed"

    link = job.get("job_apply_link") or job.get("job_google_link") or "Link not available"
    publisher = job.get("job_publisher") or "Source"
    is_remote = job.get("job_is_remote")
    remote_tag = "Remote" if is_remote else "On-site/Hybrid"

    return f"*{title}* | {company}\n{location} • {remote_tag} • {publisher}\n{link}"

def main():
    posted = 0
    seen_ids = set()

    # Header message
    slack_post(f"🧠 *Aidrr Job Bot* — Canada search (not remote-only) • Date filter: *{DATE_POSTED}*")

    for group in ROLE_GROUPS:
        if posted >= MAX_POSTS_TOTAL:
            break

        query = build_query(group)

        try:
            jobs = fetch_jobs(query)
        except Exception as e:
            slack_post(f"⚠️ Error fetching jobs for query:\n`{query}`\nError: `{e}`")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        for job in jobs:
            if posted >= MAX_POSTS_TOTAL:
                break

            job_id = job.get("job_id") or job.get("job_apply_link") or job.get("job_google_link")
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)

            msg = format_job(job)
            try:
                slack_post(msg)
                posted += 1
            except Exception as e:
                slack_post(f"⚠️ Slack post failed: `{e}`")
                break

        time.sleep(SLEEP_BETWEEN_CALLS)

    if posted == 0:
        slack_post("No jobs found this run. Try changing DATE_POSTED to `month` or increase MAX_POSTS_TOTAL.")

    slack_post(f"✅ Done. Posted *{posted}* jobs.")

if __name__ == "__main__":
    main()
