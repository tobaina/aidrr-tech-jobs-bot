import os
import time
import json
import requests
from typing import Dict, List, Any, Tuple, Set

# ----------------------------
# ENV (GitHub Secrets)
# ----------------------------
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()

# accept either secret name so you never get stuck again
SLACK_WEBHOOK_URL = (
    os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    or os.environ.get("SLACK_WEBHOOK", "").strip()
    or os.environ.get("SLACK_WEBHOOK_URL".lower(), "").strip()
)

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")
if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL (or SLACK_WEBHOOK) secret in GitHub Actions.")

# ----------------------------
# RapidAPI JSearch endpoint (your working host)
# ----------------------------
API_URL = "https://jsearch27.p.rapidapi.com/search"
HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch27.p.rapidapi.com",
}

# ----------------------------
# Canada targeting helpers
# ----------------------------
CANADA_PROVINCE_HINTS = [
    "ON", "Ontario",
    "BC", "British Columbia",
    "AB", "Alberta",
    "QC", "Quebec", "Québec",
    "NS", "Nova Scotia",
    "NB", "New Brunswick",
    "MB", "Manitoba",
    "SK", "Saskatchewan",
    "NL", "Newfoundland", "Labrador",
    "PE", "Prince Edward Island",
    "NT", "Northwest Territories",
    "NU", "Nunavut",
    "YT", "Yukon",
]

CANADA_CITY_HINTS = [
    "Toronto", "Vancouver", "Calgary", "Ottawa", "Montreal", "Montréal",
    "Edmonton", "Waterloo", "Kitchener", "Cambridge", "Mississauga",
    "Winnipeg", "Halifax", "Victoria", "Quebec City", "Québec",
    "Brampton", "Hamilton", "London", "Markham", "Burnaby", "Surrey",
]

# We will search these as "location anchors" to get real Canadian results
CANADA_LOCATION_QUERIES = [
    "Remote Canada",
    "Toronto, ON", "Waterloo, ON", "Ottawa, ON", "Mississauga, ON",
    "Vancouver, BC", "Calgary, AB", "Edmonton, AB",
    "Montreal, QC", "Halifax, NS", "Winnipeg, MB",
    "Canada",  # fallback
]

# ----------------------------
# TECH ROLE GROUPS (your “all tech roles”)
# Keep it broad but not insane to avoid rate limits
# ----------------------------
ROLE_GROUPS: List[Tuple[str, str]] = [
    ("Software Engineering",
     '"software engineer" OR "software developer" OR "senior software engineer" OR "full stack developer" OR "backend developer" OR "frontend developer" OR "mobile developer" OR "ios developer" OR "android developer"'),
    ("DevOps / Cloud / SRE",
     '"devops engineer" OR "site reliability engineer" OR "sre" OR "cloud engineer" OR "cloud architect" OR "platform engineer" OR kubernetes OR terraform OR "cloud administrator"'),
    ("Data / Analytics / ML",
     '"data analyst" OR "business intelligence" OR "bi analyst" OR "analytics engineer" OR "data engineer" OR "machine learning engineer" OR "ml engineer"'),
    ("Cybersecurity",
     'cybersecurity OR "security engineer" OR "security analyst" OR "soc analyst" OR "incident response" OR iam OR "identity access management"'),
    ("Product / Delivery",
     '"product manager" OR "technical product manager" OR "product owner" OR "technical product owner" OR "project manager" OR "program manager" OR "scrum master" OR "delivery manager"'),
    ("Business Analysis / QA / Implementation",
     '"business analyst" OR "technical analyst" OR "requirements analyst" OR "qa engineer" OR "test analyst" OR "automation tester" OR "implementation specialist" OR "solutions consultant"'),
    ("ERP / CRM (D365 / Salesforce / Power Platform)",
     '"dynamics 365" OR "power platform" OR "power bi" OR "salesforce administrator" OR "salesforce developer" OR "crm analyst" OR "erp analyst" OR "implementation consultant"'),
    ("IT Support / Systems / Network",
     '"systems administrator" OR "system administrator" OR "network engineer" OR "it support" OR "help desk" OR "desktop support"'),
]

# ----------------------------
# Posting control
# ----------------------------
MAX_JOBS_TO_POST = 25              # keep Slack readable
DATE_POSTED = "week"               # today/3days/week/month
REMOTE_ONLY = False                # set True if you only want remote jobs
NUM_PAGES_PER_SEARCH = 1           # keep low to avoid 429
SLEEP_BETWEEN_CALLS_SEC = 1.25     # gentle throttling

# Dedup store
SEEN_FILE = "seen_jobs.json"


def load_seen() -> Set[str]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
            return set()
    except Exception:
        return set()


def save_seen(seen: Set[str]) -> None:
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(seen))[:5000], f)  # cap file size
    except Exception:
        pass


def slack_post(text: str) -> None:
    payload = {"text": text}
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=20)
    r.raise_for_status()


def safe_str(x: Any) -> str:
    return (x or "").strip()


def looks_like_canada(job: Dict[str, Any]) -> bool:
    """
    Strict Canada detection with fallbacks.
    """
    country = safe_str(job.get("job_country")).upper()
    if country == "CA":
        return True

    # Build one combined location string
    city = safe_str(job.get("job_city"))
    state = safe_str(job.get("job_state"))
    loc = f"{city} {state}".strip()

    title = safe_str(job.get("job_title"))
    desc = safe_str(job.get("job_description"))
    combined = f"{loc} {title} {desc}".lower()

    # If explicitly remote and mentions Canada clearly
    if job.get("job_is_remote") is True:
        if "remote canada" in combined or "canada (remote)" in combined or "remote (canada)" in combined:
            return True
        # Some postings say "Canada" in text for remote roles
        if "canada" in combined and any(h.lower() in combined for h in ["toronto", "vancouver", "montreal", "calgary", "ottawa", "ontario", "british columbia", "alberta", "quebec"]):
            return True

    # Province hints
    for hint in CANADA_PROVINCE_HINTS:
        if hint.lower() in combined:
            return True

    # City hints
    for hint in CANADA_CITY_HINTS:
        if hint.lower() in combined:
            return True

    return False


def build_search_queries() -> List[Tuple[str, str, str]]:
    """
    Returns list of (group_name, location_anchor, query_string)
    """
    items = []
    for group_name, role_query in ROLE_GROUPS:
        for loc in CANADA_LOCATION_QUERIES:
            # This format forces Canadian locality better than just "in Canada"
            q = f"({role_query}) {loc}"
            items.append((group_name, loc, q))
    return items


def rapidapi_search(query: str, page: int = 1) -> Dict[str, Any]:
    params = {
        "query": query,
        "page": page,
        "num_pages": 1,
        "date_posted": DATE_POSTED,
        "remote_jobs_only": REMOTE_ONLY,
    }
    # Retry on 429
    max_tries = 5
    backoff = 2.0

    for attempt in range(1, max_tries + 1):
        r = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 429:
            # rate limited: wait and retry
            time.sleep(backoff)
            backoff *= 1.8
            continue
        # Other errors: raise
        r.raise_for_status()
        return r.json()

    # Still 429 after retries
    return {"status": "RATE_LIMITED", "data": []}


def format_job(job: Dict[str, Any], group_name: str) -> str:
    title = safe_str(job.get("job_title")) or safe_str(job.get("job_job_title")) or "Job"
    company = safe_str(job.get("employer_name")) or "Company"
    city = safe_str(job.get("job_city"))
    state = safe_str(job.get("job_state"))
    country = safe_str(job.get("job_country"))
    is_remote = job.get("job_is_remote")
    publisher = safe_str(job.get("job_publisher"))
    link = safe_str(job.get("job_apply_link")) or safe_str(job.get("job_google_link"))

    loc_bits = []
    if city:
        loc_bits.append(city)
    if state:
        loc_bits.append(state)
    if country:
        loc_bits.append(country)
    loc = ", ".join(loc_bits) if loc_bits else "Location not listed"

    remote_tag = "Remote" if is_remote else "On-site/Hybrid"
    pub = publisher if publisher else "Source"

    return (
        f"*{group_name}*\n"
        f"{title} | {company}\n"
        f"{loc} • {remote_tag} • {pub}\n"
        f"{link}\n"
    )


def main():
    seen = load_seen()
    posted: List[str] = []
    rate_limited_count = 0
    searches_run = 0

    queries = build_search_queries()

    for group_name, loc_anchor, q in queries:
        if len(posted) >= MAX_JOBS_TO_POST:
            break

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
        searches_run += 1

        data = rapidapi_search(q, page=1)
        if data.get("status") == "RATE_LIMITED":
            rate_limited_count += 1
            continue

        jobs = data.get("data") or []
        if not jobs:
            continue

        for job in jobs:
            if len(posted) >= MAX_JOBS_TO_POST:
                break

            job_id = safe_str(job.get("job_id"))
            if not job_id or job_id in seen:
                continue

            # Strict Canada filter (but smarter than "in Canada" keyword)
            if not looks_like_canada(job):
                continue

            # Passed
            msg = format_job(job, group_name)
            posted.append(msg)
            seen.add(job_id)

    save_seen(seen)

    # Slack output (clean + no scary errors)
    if posted:
        header = f"✅ Aidrr Tech Jobs Bot: Posted {len(posted)} Canada jobs."
        if rate_limited_count:
            header += f" (Rate-limited on {rate_limited_count} searches; continued.)"
        slack_post(header)
        # post jobs in chunks so Slack does not reject large payload
        chunk = []
        char_count = 0
        for line in posted:
            if char_count + len(line) > 3300:
                slack_post("\n".join(chunk))
                chunk = []
                char_count = 0
            chunk.append(line)
            char_count += len(line)
        if chunk:
            slack_post("\n".join(chunk))
    else:
        msg = "No Canada tech jobs found in this run."
        if rate_limited_count:
            msg = "No Canada tech jobs found in this run (or API rate-limited this run)."
        slack_post(msg)


if __name__ == "__main__":
    main()
