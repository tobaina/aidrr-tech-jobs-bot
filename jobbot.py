import os
import requests

# --------- ENV (GitHub Secrets) ---------
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")
if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL secret in GitHub Actions.")

# --------- RapidAPI JSearch Endpoint ---------
url = "https://jsearch.p.rapidapi.com/search"

# ✅ Canada jobs (NOT remote only)
querystring = {
    "query": "software engineer OR data analyst OR cybersecurity OR cloud engineer OR devops",
    "page": "1",
    "num_pages": "1",
    "country": "ca",              # Canada
    "date_posted": "7days",       # posted in last 7 days
}

headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
}

# --------- Fetch Jobs ---------
try:
    response = requests.get(url, headers=headers, params=querystring, timeout=30)
except Exception as e:
    raise RuntimeError(f"Request failed: {e}")

print("HTTP Status:", response.status_code)

if response.status_code != 200:
    print("Response text:", response.text[:1000])
    raise RuntimeError("RapidAPI request failed.")

data = response.json()

if "data" not in data:
    print("Unexpected JSON keys:", list(data.keys()))
    print("Full response:", str(data)[:1000])
    raise RuntimeError("RapidAPI response missing 'data' field.")

jobs = data.get("data", []) or []

if not jobs:
    print("No jobs found.")
    exit(0)

# --------- Post Jobs to Slack ---------
posted = 0

for job in jobs[:5]:
    title = job.get("job_title") or "Untitled role"
    company = job.get("employer_name") or "Unknown company"
    city = job.get("job_city") or ""
    province = job.get("job_state") or ""
    location = f"{city}, {province}".strip(", ")
    link = job.get("job_apply_link") or job.get("job_google_link") or ""

    message = f"*{title}*\n{company} — {location}\n{link}".strip()

    slack_payload = {"text": message}

    slack_res = requests.post(SLACK_WEBHOOK_URL, json=slack_payload, timeout=30)

    if slack_res.status_code in (200, 204):
        posted += 1
    else:
        print("Slack error:", slack_res.status_code, slack_res.text)

print(f"Jobs posted successfully: {posted}")
