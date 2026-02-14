import os
import time
import json
import requests
from typing import Dict, List, Any, Tuple, Set, Optional
from urllib.parse import quote_plus

# ----------------------------
# ENV (GitHub Secrets)
# ----------------------------
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()

SLACK_WEBHOOK_URL = (
    os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    or os.environ.get("SLACK_WEBHOOK", "").strip()
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
# Canada strict detection (STRUCTURED FIELDS ONLY)
# ----------------------------
CANADA_PROVINCE_CODES = {
    "ON", "BC", "AB", "QC", "NS", "NB", "MB", "SK", "NL", "PE", "NT", "NU", "YT"
}

# City hints only used against job_city (not title)
CANADA_MAJOR_CITIES = {
    "toronto", "vancouver", "calgary", "ottawa", "montreal", "montréal",
    "edmonton", "waterloo", "kitchener", "cambridge", "mississauga",
    "winnipeg", "halifax", "victoria", "brampton", "hamilton", "london",
    "markham", "burnaby", "surrey", "quebec", "québec"
}

# Search anchors (to find Canada jobs)
CANADA_LOCATION_QUERIES = [
    "Remote Canada",
    "Toronto, ON", "Waterloo, ON", "Ottawa, ON", "Mississauga, ON",
    "Vancouver, BC", "Calgary, AB", "Edmonton, AB",
    "Montreal, QC", "Halifax, NS", "Winnipeg, MB",
    "Canada",
]

# ----------------------------
# TECH ROLE GROUPS
# ----------------------------
ROLE_GROUPS: List[Tuple[str, str]] = [
    ("Software Engineering",
     '"software engineer" OR "software developer" OR "senior software engineer" OR "full stack developer" OR "backend developer" OR "frontend developer" OR "mobile developer" OR "ios developer" OR "android developer"'),
    ("DevOps / Cloud / SRE",
     '"devops engineer" OR "site reliability engineer" OR "sre" OR "cloud engineer" OR "cloud architect" OR "platform engineer" OR kubernetes OR terraform'),
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
MAX_JOBS_TO_POST = 25
DATE_POSTED = "month"          # week is often too tight for CA; month returns more
REMOTE_ONLY = False
NUM_PAGES_PER_SEARCH = 1
SLEEP_BETWEEN_CALLS_SEC = 1.25
SEEN_FILE = "seen_jobs.json"

# Prefer better links
MIN_QUALITY_SCORE = 0.70       # reduce junk
REQUIRE_DIRECT_APPLY = False   # set True if you want only job_apply_is_direct == True

# ----------------------------
# Helpers
# ----------------------------
def safe_str(x: Any) -> str:
    return (x or "").strip()

def load_seen() -> Set[str]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()

def save_seen(seen: Set[str]) -> None:
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(seen))[:5000], f)
    except Exception:
        pass

def slack_post(text: str) -> None:
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=20)
    r.raise_for_status()

def build_queries() -> List[Tuple[str, str]]:
    queries: List[Tuple[str, str]] = []
    for group_name, role_query in ROLE_GROUPS:
        for loc in CANADA_LOCATION_QUERIES:
            queries.append((group_name, f"({role_query}) {loc}"))
    return queries

def rapidapi_search(query: str) -> Dict[str, Any]:
    params = {
        "query": query,
        "page": 1,
        "num_pages": NUM_PAGES_PER_SEARCH,
        "date_posted": DATE_POSTED,
        "remote_jobs_only": REMOTE_ONLY,
    }

    max_tries = 5
    backoff = 2.0

    for _ in range(max_tries):
        r = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(backoff)
            backoff *= 1.8
            continue
        r.raise_for_status()
        return r.json()

    return {"status": "RATE_LIMITED", "data": []}

def is_canada_job(job: Dict[str, Any]) -> bool:
    """
    FINAL strict rule:
    - Accept only if structured fields indicate Canada.
    - We DO NOT look at job_title for Canada hints (because query text gets echoed into titles).
    """
    country = safe_str(job.get("job_country")).upper()
    state = safe_str(job.get("job_state")).upper()
    city = safe_str(job.get("job_city")).lower()

    if country == "CA":
        return True

    # Some results omit country but have province code
    if state in CANADA_PROVINCE_CODES:
        return True

    # City-only hint (only if city itself is clearly Canadian)
    if city in CANADA_MAJOR_CITIES:
        return True

    # Remote roles: require country == CA (do not guess)
    if job.get("job_is_remote") is True and country == "CA":
        return True

    return False

def quality_ok(job: Dict[str, Any]) -> bool:
    score_str = safe_str(job.get("job_apply_quality_score"))
    try:
        score = float(score_str) if score_str else 0.0
    except Exception:
        score = 0.0

    if score and score < MIN_QUALITY_SCORE:
        return False

    if REQUIRE_DIRECT_APPLY and job.get("job_apply_is_direct") is not True:
        return False

    return True

def make_google_link(title: str, company: str, city: str, state: str) -> str:
    q = f'{title} {company} {city} {state} Canada'
    return "https://www.google.com/search?q=" + quote_plus(q)

def link_is_alive(url: str) -> bool:
    """
    Quick, low-cost validation.
    If it fails, we will post a Google link instead.
    """
    if not url.startswith("http"):
        return False

    try:
        # HEAD first
        r = requests.head(url, timeout=10, allow_redirects=True)
        if 200 <= r.status_code < 400:
            return True
        # Fallback GET (some sites block HEAD)
        r = requests.get(url, timeout=10, allow_redirects=True)
        return 200 <= r.status_code < 400
    except Exception:
        return False

def pick_best_link(job: Dict[str, Any]) -> str:
    title = safe_str(job.get("job_job_title")) or safe_str(job.get("job_title")) or "Job"
    company = safe_str(job.get("employer_name")) or "Company"
    city = safe_str(job.get("job_city"))
    state = safe_str(job.get("job_state"))

    apply_link = safe_str(job.get("job_apply_link"))
    google_link = safe_str(job.get("job_google_link")) or make_google_link(title, company, city, state)

    # Validate apply link. If dead, use Google link (more stable).
    if apply_link and link_is_alive(apply_link):
        return apply_link
    return google_link

def format_job(job: Dict[str, Any], group_name: str) -> str:
    title = safe_str(job.get("job_job_title")) or safe_str(job.get("job_title")) or "Job"
    company = safe_str(job.get("employer_name")) or "Company"
    city = safe_str(job.get("job_city"))
    state = safe_str(job.get("job_state"))
    country = safe_str(job.get("job_country"))
    is_remote = job.get("job_is_remote")
    publisher = safe_str(job.get("job_publisher")) or "Source"
    link = pick_best_link(job)

    loc_bits = [b for b in [city, state, country] if b]
    loc = ", ".join(loc_bits) if loc_bits else "Location not listed"
    remote_tag = "Remote" if is_remote else "On-site/Hybrid"

    return (
        f"*{group_name}*\n"
        f"{title} | {company}\n"
        f"{loc} • {remote_tag} • {publisher}\n"
        f"{link}\n"
    )

def main():
    seen = load_seen()
    posted: List[str] = []
    rate_limited = 0
    searches = 0

    for group_name, q in build_queries():
        if len(posted) >= MAX_JOBS_TO_POST:
            break

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
        searches += 1

        data = rapidapi_search(q)
        if data.get("status") == "RATE_LIMITED":
            rate_limited += 1
            continue

        jobs = data.get("data") or []
        for job in jobs:
            if len(posted) >= MAX_JOBS_TO_POST:
                break

            job_id = safe_str(job.get("job_id"))
            if not job_id or job_id in seen:
                continue

            # Hard Canada filter (structured fields only)
            if not is_canada_job(job):
                continue

            # Quality filter
            if not quality_ok(job):
                continue

            posted.append(format_job(job, group_name))
            seen.add(job_id)

    save_seen(seen)

    # Slack output (clean)
    if posted:
        header = f"✅ Aidrr Tech Jobs Bot: Posted {len(posted)} Canada jobs."
        if rate_limited:
            header += f" (Rate-limited on {rate_limited} searches; continued.)"
        slack_post(header)

        # Chunk posts to avoid Slack payload limits
        chunk: List[str] = []
        size = 0
        for item in posted:
            if size + len(item) > 3300:
                slack_post("\n".join(chunk))
                chunk = []
                size = 0
            chunk.append(item)
            size += len(item)
        if chunk:
            slack_post("\n".join(chunk))
    else:
        msg = "No Canada tech jobs found in this run."
        if rate_limited:
            msg = "No Canada tech jobs found in this run (or API rate-limited this run)."
        slack_post(msg)

if __name__ == "__main__":
    main()
