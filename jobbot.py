import os
import time
import requests

# ==============================
# ENV (GitHub Secrets)
# ==============================

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()

SLACK_WEBHOOK_URL = (
    os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    or os.environ.get("SLACK_WEBHOOK", "").strip()
)

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")

if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL (or SLACK_WEBHOOK) secret.")

# ==============================
# API CONFIG
# ==============================

API_URL = "https://jsearch27.p.rapidapi.com/search"

HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch27.p.rapidapi.com",
}

DATE_POSTED = "week"
NUM_PAGES = 1

# ==============================
# ROLE GROUPS (ALL TECH ROLES, but fewer calls)
# ==============================

QUERIES = [
    # Engineering + DevOps + Cloud
    (
        '"software engineer" OR "software developer" OR "full stack developer" OR '
        '"backend developer" OR "frontend developer" OR "mobile developer" OR '
        '"ios developer" OR "android developer" OR '
        '"devops engineer" OR "site reliability engineer" OR "sre" OR '
        '"cloud engineer" OR "cloud architect" OR "platform engineer" OR '
        '"kubernetes" OR "terraform"'
    ),

    # Data + Security
    (
        '"data analyst" OR "business intelligence" OR "bi analyst" OR "analytics engineer" OR '
        '"data engineer" OR "machine learning engineer" OR "ml engineer" OR '
        '"cybersecurity" OR "security engineer" OR "security analyst" OR "soc analyst" OR '
        '"incident response" OR "iam" OR "identity access management"'
    ),

    # Product + Project + BA/QA + ERP/CRM + IT Support/Infra
    (
        '"product manager" OR "product owner" OR "technical product owner" OR '
        '"project manager" OR "program manager" OR "scrum master" OR "delivery manager" OR '
        '"business analyst" OR "technical analyst" OR "requirements analyst" OR '
        '"qa engineer" OR "test analyst" OR "automation tester" OR '
        '"implementation specialist" OR "solutions consultant" OR '
        '"salesforce administrator" OR "salesforce developer" OR "dynamics 365" OR '
        '"power platform" OR "crm analyst" OR "erp analyst" OR "implementation consultant" OR '
        '"systems administrator" OR "system administrator" OR "network engineer" OR '
        '"it support" OR "help desk" OR "desktop support" OR "cloud administrator" OR "windows administrator"'
    )
]

# ==============================
# LOCATION FILTER
# ==============================

CAN_PROVINCES = {"AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"}
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO",
    "MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY","DC"
}

def is_canada_job(job):
    # Strong signals first
    country = (job.get("job_country") or "").strip().upper()
    state = (job.get("job_state") or "").strip().upper()

    if country == "CA":
        return True
    if country == "US":
        return False

    # Fallback: state/province code
    if state in CAN_PROVINCES:
        return True
    if state in US_STATES:
        return False

    # Fallback: sometimes APIs omit country — check text fields
    text_blob = " ".join([
        str(job.get("job_city") or ""),
        str(job.get("job_state") or ""),
        str(job.get("job_country") or ""),
        str(job.get("job_description") or "")
    ]).lower()

    # Must contain Canada indicators and NOT contain strong US indicators
    if "canada" in text_blob and "united states" not in text_blob:
        return True

    return False

# ==============================
# SLACK
# ==============================

def post_to_slack(text):
    requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)

# ==============================
# REQUEST WITH RETRIES (NO ERROR SPAM)
# ==============================

def fetch_jobs(params, max_retries=6):
    delay = 5
    for attempt in range(max_retries):
        try:
            r = requests.get(API_URL, headers=HEADERS, params=params, timeout=20)

            # Rate limit: wait + retry (DO NOT SKIP)
            if r.status_code == 429:
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue

            # Forbidden / other errors: just stop this query quietly
            if r.status_code >= 400:
                return []

            data = r.json()
            return data.get("data", []) or []

        except Exception:
            time.sleep(2)

    # If we tried many times and still rate limited, return empty
    return []

# ==============================
# MAIN
# ==============================

def main():
    total_posted = 0
    seen_links = set()

    for query in QUERIES:

        params = {
            "query": query,
            "country": "ca",  # API-level Canada filter
            "page": 1,
            "num_pages": NUM_PAGES,
            "date_posted": DATE_POSTED,
            "remote_jobs_only": False
        }

        jobs = fetch_jobs(params)

        for job in jobs:
            if not is_canada_job(job):
                continue

            link = (job.get("job_apply_link") or "").strip()
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            title = job.get("job_title") or "Tech Role"
            company = job.get("employer_name") or "Company"
            city = job.get("job_city") or ""
            prov = job.get("job_state") or ""

            location = ", ".join([p for p in [city, prov, "Canada"] if p])

            msg = f"*{title}* | {company}\n{location}\n{link}"
            post_to_slack(msg)
            total_posted += 1

        # Slow down between queries to avoid rate limit
        time.sleep(8)

    if total_posted > 0:
        post_to_slack(f"✅ Done. Posted {total_posted} Canada tech jobs.")
    else:
        post_to_slack("No Canada tech jobs found in this run (or API rate-limited this run).")

if __name__ == "__main__":
    main()
