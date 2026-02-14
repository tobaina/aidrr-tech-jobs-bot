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
# ROLE GROUPS (ALL TECH ROLES)
# ==============================

QUERIES = [

    # Software / Engineering
    '"software engineer" OR "software developer" OR "senior software engineer" OR "full stack developer" OR "backend developer" OR "frontend developer" OR "mobile developer" OR "ios developer" OR "android developer"',

    # DevOps / Cloud
    '"devops engineer" OR "site reliability engineer" OR "sre" OR "cloud engineer" OR "cloud architect" OR "platform engineer" OR "kubernetes" OR "terraform"',

    # Data / AI
    '"data analyst" OR "business intelligence" OR "bi analyst" OR "analytics engineer" OR "data engineer" OR "machine learning engineer" OR "ml engineer"',

    # Product / Project
    '"product manager" OR "product owner" OR "technical product owner" OR "project manager" OR "program manager" OR "scrum master" OR "delivery manager"',

    # Business Analysis / QA
    '"business analyst" OR "technical analyst" OR "requirements analyst" OR "qa engineer" OR "test analyst" OR "automation tester" OR "implementation specialist" OR "solutions consultant"',

    # CRM / ERP
    '"salesforce administrator" OR "salesforce developer" OR "dynamics 365" OR "power platform" OR "crm analyst" OR "erp analyst" OR "implementation consultant"',

    # Cybersecurity
    '"cybersecurity" OR "security engineer" OR "security analyst" OR "soc analyst" OR "incident response" OR "iam" OR "identity access management"',

    # IT / Infrastructure
    '"systems administrator" OR "system administrator" OR "network engineer" OR "it support" OR "help desk" OR "desktop support" OR "cloud administrator" OR "windows administrator"'
]

# ==============================
# LOCATION FILTER
# ==============================

CAN_PROVINCES = {
    "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"
}

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO",
    "MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY","DC"
}

def is_canada_job(job):
    country = (job.get("job_country") or "").strip().upper()
    state = (job.get("job_state") or "").strip().upper()
    city = (job.get("job_city") or "").strip().lower()

    if country == "CA":
        return True
    if country == "US":
        return False

    if state in CAN_PROVINCES:
        return True
    if state in US_STATES:
        return False

    canada_cities = [
        "toronto","vancouver","calgary","edmonton","ottawa",
        "montreal","mississauga","waterloo","kitchener",
        "hamilton","winnipeg","halifax"
    ]

    if any(c in city for c in canada_cities):
        return True

    return False

# ==============================
# SLACK FUNCTION
# ==============================

def post_to_slack(text):
    requests.post(
        SLACK_WEBHOOK_URL,
        json={"text": text},
        timeout=10
    )

# ==============================
# MAIN
# ==============================

def main():
    total_posted = 0

    for query in QUERIES:

        params = {
            "query": query,
            "country": "ca",   # HARD Canada filter
            "page": 1,
            "num_pages": NUM_PAGES,
            "date_posted": DATE_POSTED,
            "remote_jobs_only": False,
        }

        try:
            response = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)

            # Handle rate limit quietly
            if response.status_code == 429:
                time.sleep(3)
                continue

            response.raise_for_status()

        except Exception:
            continue  # No Slack error spam

        data = response.json()
        jobs = data.get("data", [])

        for job in jobs:

            if not is_canada_job(job):
                continue

            title = job.get("job_title")
            company = job.get("employer_name")
            city = job.get("job_city")
            state = job.get("job_state")
            link = job.get("job_apply_link")

            location = ", ".join([p for p in [city, state, "Canada"] if p])

            message = f"*{title}* | {company}\n{location}\n{link}"

            post_to_slack(message)
            total_posted += 1

        time.sleep(1)  # Prevent rate limit

    if total_posted > 0:
        post_to_slack(f"✅ Done. Posted {total_posted} Canada tech jobs.")
    else:
        post_to_slack("No Canada tech jobs found in this run.")

if __name__ == "__main__":
    main()
