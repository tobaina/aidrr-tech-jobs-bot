import os
import json
import time
import requests
from typing import List, Dict, Any, Set, Tuple

# =========================
# ENV (GitHub Secrets)
# =========================
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

# =========================
# JSearch (RapidAPI) endpoint
# =========================
API_URL = "https://jsearch27.p.rapidapi.com/search"

HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch27.p.rapidapi.com",
}

# =========================
# Settings
# =========================
SEEN_FILE = "seen_jobs.json"

# Keep this low to avoid rate limits and dead links
MAX_JOBS_PER_GROUP = 12
PAGES_PER_QUERY = 1
RESULTS_PER_PAGE = 20

# Prefer direct apply links (reduces "job not available" frustration)
ONLY_DIRECT_APPLY = True  # set False if you want everything

# Add a small delay between API calls
API_CALL_DELAY_SECONDS = 2.5

# =========================
# Tech role groups (broad)
# =========================
TECH_GROUPS: Dict[str, str] = {
    "Software Engineering": (
        '("software engineer" OR "software developer" OR "senior software engineer" OR '
        '"full stack developer" OR "backend developer" OR "frontend developer" OR '
        '"mobile developer" OR "ios developer" OR "android developer")'
    ),
    "DevOps / Cloud / SRE": (
        '("devops engineer" OR "site reliability engineer" OR "sre" OR '
        '"cloud engineer" OR "cloud architect" OR "platform engineer" OR '
        '"kubernetes" OR "terraform")'
    ),
    "Data / Analytics / ML": (
        '("data analyst" OR "business intelligence" OR "bi analyst" OR '
        '"analytics engineer" OR "data engineer" OR "machine learning engineer" OR "ml engineer")'
    ),
    "Cybersecurity": (
        '("cybersecurity" OR "security engineer" OR "security analyst" OR '
        '"soc analyst" OR "incident response" OR "iam" OR "identity access management")'
    ),
    "QA / Testing": (
        '("qa engineer" OR "test analyst" OR "automation tester" OR "sdet" OR "quality assurance")'
    ),
    "Product / Project / Agile": (
        '("product manager" OR "product owner" OR "technical product owner" OR '
        '"project manager" OR "program manager" OR "scrum master" OR "delivery manager")'
    ),
    "IT Support / SysAdmin / Network": (
        '("systems administrator" OR "system administrator" OR "network engineer" OR '
        '"it support" OR "help desk" OR "desktop support" OR '
        '"cloud administrator" OR "windows administrator")'
    ),
    "ERP / CRM / Implementation": (
        '("dynamics 365" OR "power platform" OR "crm analyst" OR '
        '"erp analyst" OR "implementation consultant" OR "solutions consultant" OR '
        '"salesforce administrator" OR "salesforce developer")'
    ),
    "Business Analysis / Technical Analyst": (
        '("business analyst" OR "technical analyst" OR "requirements analyst" OR '
        '"implementation specialist" OR "systems analyst")'
    ),
}

# =========================
# Canada filter helpers
# =========================
CANADA_PROVINCES = ["ON", "BC", "AB", "QC", "MB", "NS", "NB", "NL", "SK", "PE", "NT", "NU", "YT"]

def is_canada(job: Dict[str, Any]) -> bool:
    """
    Strong Canada-only filter.
    Uses job_country_code first, then job_country, then location string fallback.
    """
    country = (job.get("job_country") or job.get("country") or "").strip()
    country_code = (job.get("job_country_code") or job.get("country_code") or "").strip()

    if country_code.upper() == "CA":
        return True
    if country.lower() in ["canada", "ca"]:
        return True

    # fallback: location text
    loc_parts = [
        str(job.get("job_city", "") or ""),
        str(job.get("job_state", "") or ""),
        str(job.get("job_location", "") or ""),
        str(job.get("job_address", "") or ""),
    ]
    loc = " ".join([p for p in loc_parts if p]).strip()

    # Province matches (ON, BC, etc.)
    for p in CANADA_PROVINCES:
        if f", {p}" in loc or f" {p}" in loc:
            return True

    # Also accept "Canada" written inside location
    if "canada" in loc.lower():
        return True

    return False


def is_direct_apply(job: Dict[str, Any]) -> bool:
    """
    Prefer direct apply links if available.
    JSearch typically includes job_apply_is_direct boolean.
    """
    val = job.get("job_apply_is_direct")
    if isinstance(val, bool):
        return val
    # sometimes API returns "true"/"false" as string
    if isinstance(val, str):
        return val.strip().lower() == "true"
    return False


def pick_best_link(job: Dict[str, Any]) -> str:
    """
    Choose best link available.
    Prefer direct apply link if possible, otherwise fallback.
    """
    # Common fields in JSearch responses:
    # job_apply_link, job_google_link, job_offer_expiration_datetime_utc, etc.
    direct = job.get("job_apply_link") or ""
    fallback = job.get("job_google_link") or job.get("job_url") or job.get("job_link") or ""

    # If ONLY_DIRECT_APPLY is enabled, use job_apply_link only (when direct)
    if ONLY_DIRECT_APPLY:
        return direct.strip()

    # Otherwise prefer apply link then fallback
    return (direct.strip() or str(fallback).strip())


# =========================
# Seen jobs tracking
# =========================
def load_seen() -> Set[str]:
    if not os.path.exists(SEEN_FILE):
        return set()
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data)
        return set()
    except Exception:
        return set()


def save_seen(seen: Set[str]) -> None:
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(seen)), f)
    except Exception:
        pass


# =========================
# Slack posting
# =========================
def slack_post(text: str) -> None:
    payload = {"text": text}
    requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)


def format_job(job: Dict[str, Any], group_name: str) -> str:
    title = job.get("job_title") or "Untitled role"
    company = job.get("employer_name") or "Unknown company"

    city = job.get("job_city") or ""
    state = job.get("job_state") or ""
    country = job.get("job_country") or "Canada"

    location = ", ".join([x for x in [city, state] if x]).strip()
    if not location:
        location = (job.get("job_location") or "").strip()

    link = pick_best_link(job)

    # Clean, compact Slack format
    # Example:
    # *Software Engineer* — Shopify (Software Engineering)
    # 📍 Toronto, ON, Canada
    # 🔗 https://...
    line1 = f"*{title}* — {company} ({group_name})"
    line2 = f"📍 {location}, {country}".replace(" ,", "").strip()
    line3 = f"🔗 {link}" if link else "🔗 Link unavailable"

    return "\n".join([line1, line2, line3])


# =========================
# API fetch with retry
# =========================
def fetch_jobs_for_query(query: str) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Returns (jobs, rate_limited_flag)
    """
    rate_limited = False
    all_jobs: List[Dict[str, Any]] = []

    for page in range(1, PAGES_PER_QUERY + 1):
        params = {
            "query": query,
            "page": page,
            "num_pages": 1,
            "date_posted": "week",
            "remote_jobs_only": False,
            "employment_types": "FULLTIME,CONTRACT,PARTTIME,INTERN",
            "country": "Canada",  # request-level hint; still do strict filter below
            "language": "en",
        }

        # Retry with backoff if 429
        resp = None
        for attempt in range(4):
            try:
                resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
                if resp.status_code == 429:
                    rate_limited = True
                    time.sleep(5 * (attempt + 1))
                    continue
                resp.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 3:
                    return (all_jobs, rate_limited)
                time.sleep(2 * (attempt + 1))

        if not resp:
            return (all_jobs, rate_limited)

        data = resp.json()
        jobs = data.get("data") or []
        if isinstance(jobs, list):
            all_jobs.extend(jobs)

        time.sleep(API_CALL_DELAY_SECONDS)

    return (all_jobs, rate_limited)


# =========================
# Main
# =========================
def main() -> None:
    seen = load_seen()

    posted: List[str] = []
    rate_limited_count = 0
    total_found = 0
    total_kept = 0

    for group_name, group_query in TECH_GROUPS.items():
        # Build the query in a way JSearch handles better
        # Keep it simple and consistent
        query = f"{group_query} in Canada"

        jobs, rl = fetch_jobs_for_query(query)
        if rl:
            rate_limited_count += 1

        if not jobs:
            continue

        # Strict Canada filter
        jobs = [j for j in jobs if is_canada(j)]

        # Prefer direct apply to reduce "job not available"
        if ONLY_DIRECT_APPLY:
            jobs = [j for j in jobs if is_direct_apply(j) and (pick_best_link(j) != "")]

        total_found += len(jobs)

        # De-duplicate within the run + seen file
        count_group = 0
        for job in jobs:
            job_id = (
                str(job.get("job_id") or "")
                or str(job.get("job_apply_link") or "")
                or str(job.get("job_google_link") or "")
            ).strip()

            if not job_id:
                continue
            if job_id in seen:
                continue

            posted.append(format_job(job, group_name))
            seen.add(job_id)
            count_group += 1
            total_kept += 1

            if count_group >= MAX_JOBS_PER_GROUP:
                break

    save_seen(seen)

    # =========================
    # Slack output (clean)
    # =========================
    if posted:
        header = f"✅ Aidrr Tech Jobs Bot: Posted {len(posted)} Canada tech jobs."
        if rate_limited_count:
            header += f" (Rate-limited on {rate_limited_count} searches; auto-retried.)"
        slack_post(header)

        # Chunk posts to avoid Slack payload limits
        chunk: List[str] = []
        size = 0
        for item in posted:
            # +2 for separators/newlines
            if size + len(item) + 2 > 3200:
                slack_post("\n\n".join(chunk))
                chunk = []
                size = 0
            chunk.append(item)
            size += len(item) + 2

        if chunk:
            slack_post("\n\n".join(chunk))
    else:
        # No noisy error spam — just a clean message
        msg = "No new Canada tech jobs found in this run."
        if rate_limited_count:
            msg += f" (Some searches were rate-limited: {rate_limited_count}. Auto-retry ran.)"
        slack_post(msg)


if __name__ == "__main__":
    main()
