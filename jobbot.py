import os
import json
import time
import random
from typing import Dict, List, Any, Set, Tuple, Optional

import requests

# =========================
# JSEARCH CONFIG
# =========================
API_URL = "https://jsearch27.p.rapidapi.com/search"
API_HOST = "jsearch27.p.rapidapi.com"

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()
SLACK_WEBHOOK_URL = (
    os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    or os.environ.get("SLACK_WEBHOOK", "").strip()
)

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")
if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL (or SLACK_WEBHOOK) secret in GitHub Actions.")

HEADERS = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": API_HOST}

# =========================
# TUNING (SAFE DEFAULTS)
# =========================
DATE_POSTED = os.getenv("DATE_POSTED", "week")           # day | week | month | all
PAGES_PER_QUERY = int(os.getenv("PAGES_PER_QUERY", "1")) # keep 1 for fewer 429
REMOTE_ONLY = os.getenv("REMOTE_ONLY", "false").lower() == "true"

API_CALL_DELAY_SECONDS = float(os.getenv("API_CALL_DELAY_SECONDS", "7"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = float(os.getenv("BACKOFF_BASE_SECONDS", "6"))

SLACK_CHUNK_CHAR_LIMIT = 3300
SEEN_FILE = "seen_jobs.json"

# =========================
# YOU (BA/PRODUCT PRIORITY)
# =========================
# Make queries "small + specific" to avoid garbage results.
ROLE_QUERIES = [
    # Business Analyst family
    'business analyst',
    'business systems analyst',
    'requirements analyst',
    'technical business analyst',
    'functional analyst',
    'process analyst',
    'data analyst',
    'bi analyst',
    'reporting analyst',
    'implementation analyst',

    # Product family
    'product analyst',
    'product owner',
    'technical product owner',
    'product manager',
    'associate product manager',

    # Delivery family
    'project manager',
    'technical project manager',
    'scrum master',
    'delivery manager',
    'program manager',
]

# Canada coverage boosters for fallback mode
CANADA_BOOST_TERMS = [
    "Canada", "Ontario", "Toronto", "Waterloo", "Kitchener", "Cambridge",
    "Ottawa", "Montreal", "Vancouver", "Calgary", "Edmonton",
]

CANADIAN_PROVINCES = {"on", "bc", "ab", "mb", "qc", "ns", "nb", "nl", "pe", "sk", "nt", "nu", "yt"}

# Biggest source of dead links / geo mismatch
BANNED_SOURCES = {
    "dice",
    "ziprecruiter",
    "monster",
    "careerbuilder",
}

# =========================
# SLACK
# =========================
def slack_post(text: str) -> None:
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=30)
    r.raise_for_status()

def chunk_and_post(lines: List[str]) -> None:
    chunk: List[str] = []
    size = 0
    for line in lines:
        if size + len(line) + 2 > SLACK_CHUNK_CHAR_LIMIT:
            slack_post("\n\n".join(chunk))
            chunk = []
            size = 0
        chunk.append(line)
        size += len(line) + 2
    if chunk:
        slack_post("\n\n".join(chunk))

# =========================
# SEEN CACHE
# =========================
def load_seen() -> Set[str]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data)
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return set()

def save_seen(seen: Set[str]) -> None:
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(seen)), f, indent=2)
    except Exception:
        pass

# =========================
# FILTERS
# =========================
def looks_like_canada(job: Dict[str, Any]) -> bool:
    country = str(job.get("job_country", "")).strip().lower()
    if country == "canada":
        return True
    if country in ("united states", "usa"):
        return False

    state = str(job.get("job_state", "")).strip().lower()
    if state in CANADIAN_PROVINCES:
        return True

    loc = " ".join([
        str(job.get("job_city", "")),
        str(job.get("job_state", "")),
        str(job.get("job_country", "")),
        str(job.get("job_location", "")),
    ]).lower()

    # Strong Canada check
    if "canada" in loc:
        return True

    # Sometimes JSearch returns remote roles with weak location fields.
    # Keep them ONLY if they mention Canada in the description/title.
    title = str(job.get("job_title", "")).lower()
    desc = str(job.get("job_description", "")).lower()
    if "canada" in title or "canada" in desc:
        return True

    return False

def source_is_banned(job: Dict[str, Any]) -> bool:
    publisher = str(job.get("job_publisher", "")).strip().lower()
    if publisher in BANNED_SOURCES:
        return True
    # Also ban by url domains
    link = best_link(job) or ""
    link_l = link.lower()
    return any(bad in link_l for bad in BANNED_SOURCES)

def best_link(job: Dict[str, Any]) -> str:
    # Prefer direct apply links
    for k in ["job_apply_link", "job_offer_url", "job_link", "job_google_link"]:
        v = job.get(k)
        if isinstance(v, str) and v.startswith("http"):
            return v
    return ""

def link_is_alive(url: str) -> bool:
    """
    Fast validation so you do not post dead links.
    """
    if not url or not url.startswith("http"):
        return False

    try:
        # HEAD is usually enough; if blocked, fallback to GET.
        r = requests.head(url, allow_redirects=True, timeout=12)
        if r.status_code in (404, 410):
            return False
        if r.status_code >= 500:
            return False
        if r.status_code == 405:
            # Some sites block HEAD
            rg = requests.get(url, allow_redirects=True, timeout=12)
            if rg.status_code in (404, 410) or rg.status_code >= 500:
                return False
        return True
    except Exception:
        return False

# =========================
# API CALLS
# =========================
def jsearch(params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool]:
    rate_limited = False
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(API_URL, headers=HEADERS, params=params, timeout=45)
            if r.status_code == 429:
                rate_limited = True
                time.sleep(BACKOFF_BASE_SECONDS * attempt + random.uniform(0, 2))
                continue
            r.raise_for_status()
            data = r.json()
            jobs = data.get("data") or data.get("jobs") or []
            if not isinstance(jobs, list):
                jobs = []
            return jobs, rate_limited
        except Exception:
            time.sleep(BACKOFF_BASE_SECONDS * attempt + random.uniform(0, 2))
            continue
    return [], rate_limited

def search_one_role(role: str, use_fallback: bool = False) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Two-pass search:
    - Primary: location=Canada (cleaner)
    - Fallback: no location param, but adds Canada booster terms into the query string
    """
    time.sleep(API_CALL_DELAY_SECONDS)

    if not use_fallback:
        params = {
            "query": f'"{role}"',
            "location": "Canada",
            "date_posted": DATE_POSTED,
            "remote_jobs_only": REMOTE_ONLY,
            "page": 1,
            "num_pages": PAGES_PER_QUERY,
        }
    else:
        # Fallback mode if location-index is weak today
        boosters = " OR ".join([f'"{b}"' for b in CANADA_BOOST_TERMS])
        params = {
            "query": f'("{role}") AND ({boosters})',
            "date_posted": DATE_POSTED,
            "remote_jobs_only": REMOTE_ONLY,
            "page": 1,
            "num_pages": PAGES_PER_QUERY,
        }

    return jsearch(params)

# =========================
# FORMAT
# =========================
def clean(s: Any, n: int = 120) -> str:
    if not s:
        return ""
    t = str(s).replace("\n", " ").strip()
    return t if len(t) <= n else t[: n - 3] + "..."

def format_job(job: Dict[str, Any]) -> str:
    title = clean(job.get("job_title"), 120) or "Job"
    company = clean(job.get("employer_name"), 80) or "Company"
    city = clean(job.get("job_city"), 40)
    state = clean(job.get("job_state"), 10)
    country = clean(job.get("job_country"), 20)
    location = ", ".join([x for x in [city, state, country] if x]) or clean(job.get("job_location"), 80)

    link = best_link(job)
    publisher = clean(job.get("job_publisher"), 30)

    return f"*{title}* | {company}\n{location} • {publisher}\n{link}"

# =========================
# MAIN
# =========================
def main() -> None:
    seen = load_seen()

    posted: List[str] = []
    rate_limited_count = 0
    searched = 0
    raw_found = 0
    canada_found = 0
    alive_found = 0

    # Pass 1: cleanest mode (location=Canada)
    for role in ROLE_QUERIES:
        searched += 1
        jobs, rl = search_one_role(role, use_fallback=False)
        if rl:
            rate_limited_count += 1
        raw_found += len(jobs)

        # Filters
        for job in jobs:
            if not looks_like_canada(job):
                continue
            canada_found += 1

            if source_is_banned(job):
                continue

            link = best_link(job)
            if not link_is_alive(link):
                continue
            alive_found += 1

            job_id = str(job.get("job_id") or link or "")
            if not job_id or job_id in seen:
                continue

            seen.add(job_id)
            posted.append(format_job(job))

    # If we got nothing, do Pass 2 fallback (query boosters)
    if not posted:
        for role in ROLE_QUERIES[:10]:  # limit fallback calls to avoid 429
            searched += 1
            jobs, rl = search_one_role(role, use_fallback=True)
            if rl:
                rate_limited_count += 1
            raw_found += len(jobs)

            for job in jobs:
                if not looks_like_canada(job):
                    continue
                canada_found += 1

                if source_is_banned(job):
                    continue

                link = best_link(job)
                if not link_is_alive(link):
                    continue
                alive_found += 1

                job_id = str(job.get("job_id") or link or "")
                if not job_id or job_id in seen:
                    continue

                seen.add(job_id)
                posted.append(format_job(job))

    save_seen(seen)

    # Slack summary (so you can see WHAT is failing)
    summary = (
        f"🔎 Run summary: searched={searched} • raw={raw_found} • canada-ish={canada_found} • link-alive={alive_found} • posted-new={len(posted)}"
    )
    if rate_limited_count:
        summary += f"\n⚠️ Rate-limited on {rate_limited_count} searches (429). Consider API_CALL_DELAY_SECONDS=8–10."

    if posted:
        slack_post("✅ *Aidrr Canada BA/Product Jobs*\n" + summary)
        chunk_and_post(posted[:30])  # cap posts per run to keep channel clean
    else:
        slack_post("❌ *No new Canada BA/Product/Delivery jobs found this run.*\n" + summary + "\n\nNext step: increase delay OR remove more aggregators OR widen DATE_POSTED=month.")

if __name__ == "__main__":
    main()
