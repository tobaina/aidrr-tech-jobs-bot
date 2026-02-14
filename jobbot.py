import os
import json
import hashlib
from datetime import datetime, timezone

import requests

# ----------------------------
# ENV (GitHub Secrets)
# ----------------------------
RAPIDAPI_KEY = (os.environ.get("RAPIDAPI_KEY") or "").strip()
SLACK_WEBHOOK_URL = (os.environ.get("SLACK_WEBHOOK_URL") or "").strip()

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")
if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL secret in GitHub Actions.")

# ----------------------------
# SETTINGS
# ----------------------------
STATE_FILE = "state.json"          # stores dedupe hashes
MAX_POSTS_PER_RUN = 15             # how many jobs to post each run
NUM_PAGES = 2                      # how many pages to fetch from API (increase if you want more)
DATE_POSTED = "7days"              # last 7 days
COUNTRY = "ca"                     # Canada

# Broad roles (NOT remote-only)
QUERY = (
    "software engineer OR developer OR full stack OR frontend OR backend OR "
    "mobile developer OR iOS developer OR Android developer OR "
    "data engineer OR data analyst OR analytics OR BI analyst OR "
    "machine learning engineer OR AI engineer OR data scientist OR "
    "cloud engineer OR cloud architect OR Azure OR AWS OR GCP OR "
    "devops OR site reliability engineer OR SRE OR platform engineer OR "
    "cybersecurity OR security analyst OR SOC analyst OR "
    "network engineer OR systems administrator OR IT support OR IT analyst OR "
    "QA engineer OR test analyst OR automation tester OR "
    "product manager OR product owner OR technical product owner OR "
    "project manager OR program manager OR scrum master OR delivery manager OR "
    "business analyst OR requirements analyst OR systems analyst OR "
    "solutions architect OR enterprise architect OR "
    "implementation specialist OR customer success manager OR "
    "salesforce administrator OR salesforce developer OR "
    "accounts receivable OR accounts payable OR AR specialist OR AP specialist OR "
    "billing specialist OR cash application OR collections OR reconciliation analyst OR "
    "operations analyst OR process analyst OR process improvement OR "
    "ERP analyst OR Dynamics 365 OR D365 OR Business Central OR JD Edwards OR SAP OR NetSuite"
)

API_URL = "https://jsearch.p.rapidapi.com/search"
HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
}

# ----------------------------
# HELPERS
# ----------------------------
def load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"seen": []}
    except Exception:
        return {"seen": []}

def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def h(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def slack_post(text: str) -> None:
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=30)
    r.raise_for_status()

def safe_get(d: dict, key: str, default: str = "") -> str:
    v = d.get(key, default)
    return v if v is not None else default

# ----------------------------
# MAIN
# ----------------------------
state = load_state()
seen = set(state.get("seen", []))

params = {
    "query": QUERY,
    "page": "1",
    "num_pages": str(NUM_PAGES),
    "country": COUNTRY,
    "date_posted": DATE_POSTED,
}

resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
resp.raise_for_status()
payload = resp.json()

jobs = payload.get("data", []) or []

if not jobs:
    slack_post("🟡 Aidrr Jobs Bot: No jobs found in the last 7 days for this search.")
    print("No jobs found.")
    raise SystemExit(0)

posted = 0
now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

for job in jobs:
    if posted >= MAX_POSTS_PER_RUN:
        break

    title = safe_get(job, "job_title", "").strip()
    company = safe_get(job, "employer_name", "").strip()
    city = safe_get(job, "job_city", "").strip()
    state_region = safe_get(job, "job_state", "").strip()
    country = safe_get(job, "job_country", "").strip()
    location = ", ".join([x for x in [city, state_region, country] if x]) or "Canada"
    apply_link = safe_get(job, "job_apply_link", "").strip()
    job_link = safe_get(job, "job_google_link", "").strip()

    link = apply_link or job_link
    if not link:
        continue

    job_id = safe_get(job, "job_id", "").strip()
    fingerprint = h(f"{job_id}|{title}|{company}|{link}")

    if fingerprint in seen:
        continue

    msg = (
        f"🔥 *{title}*\n"
        f"*Company:* {company or 'N/A'}\n"
        f"*Location:* {location}\n"
        f"*Link:* {link}\n"
        f"_Found by Aidrr Jobs Bot • {now}_"
    )

    slack_post(msg)
    seen.add(fingerprint)
    posted += 1

state["seen"] = list(seen)[-2000:]  # keep last 2000 hashes only
save_state(state)

summary = f"✅ Aidrr Jobs Bot: Posted *{posted}* new job(s)."
slack_post(summary)
print(summary)
