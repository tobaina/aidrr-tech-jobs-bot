# jobbot.py
# Aidrr Tech Jobs Bot (Canada-only) — JSearch (RapidAPI) -> Slack
# Final adjustment goals:
# 1) Canada-only filtering (strict by country + location checks)
# 2) Prefer working links (direct apply > apply link > job url)
# 3) Reduce 429 spam (retry + backoff + pacing)
# 4) Avoid “No Canada jobs” noise — show what happened

import os
import time
import json
import hashlib
import requests
from typing import Dict, Any, List, Optional, Set, Tuple

# =========================
# CONFIG (EDIT THESE)
# =========================

# RapidAPI JSearch
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "").strip()
API_URL = os.environ.get("JSEARCH_API_URL", "https://jsearch.p.rapidapi.com/search").strip()
API_HOST = os.environ.get("JSEARCH_API_HOST", "jsearch.p.rapidapi.com").strip()

# Slack Webhook (supports either secret name)
SLACK_WEBHOOK_URL = (
    os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    or os.environ.get("SLACK_WEBHOOK", "").strip()
)

# Canada-only
COUNTRY_CODE = "CA"
CANADA_KEYWORDS = [
    "canada", "on, canada", "bc, canada", "ab, canada", "mb, canada", "sk, canada", "ns, canada", "nb, canada",
    "nl, canada", "pe, canada", "yt, canada", "nt, canada", "nu, canada",
    # common Canadian city/province abbreviations that show up in location strings
    "ontario", "toronto", "mississauga", "brampton", "waterloo", "kitchener", "cambridge, on",
    "vancouver", "burnaby", "surrey", "victoria", "calgary", "edmonton", "ottawa", "montreal", "winnipeg",
    "halifax", "regina", "saskatoon", "quebec"
]

# IMPORTANT: Keep this FALSE so you do not filter out most jobs.
# We will still prefer direct links when available.
ONLY_DIRECT_APPLY = False

# How many pages per query (reduce if you get rate-limited)
PAGES_PER_QUERY = 1
RESULTS_PER_PAGE = 10

# Recency (more forgiving than "week")
DATE_POSTED = "month"  # options vary: "today", "3days", "week", "month" etc.

# Remote filter: keep False so we capture remote + hybrid + onsite in Canada
REMOTE_ONLY = False

# Pace requests + rate limit handling
API_CALL_DELAY_SECONDS = 2.5
MAX_RETRIES_429 = 3

# Slack payload chunk size (Slack webhooks can choke on huge posts)
SLACK_CHUNK_CHAR_LIMIT = 3000

# Seen cache
SEEN_FILE = "seen_jobs.json"

# Role groups (broad tech — edit as you like)
TECH_ROLE_GROUPS: Dict[str, List[str]] = {
    "Software Engineering": [
        "software engineer", "software developer", "frontend developer", "backend developer", "full stack developer",
        "mobile developer", "ios developer", "android developer", "react developer", "java developer",
        "python developer", ".net developer", "node.js developer"
    ],
    "DevOps / Cloud": [
        "devops engineer", "site reliability engineer", "sre", "cloud engineer", "cloud architect",
        "platform engineer", "kubernetes", "terraform"
    ],
    "Data / Analytics": [
        "data analyst", "business intelligence", "bi analyst", "analytics engineer", "data engineer",
        "machine learning engineer", "ml engineer"
    ],
    "Product / Delivery": [
        "product manager", "product owner", "technical product owner", "project manager",
        "program manager", "scrum master", "delivery manager"
    ],
    "QA / Implementation": [
        "business analyst", "technical analyst", "requirements analyst", "qa engineer", "test analyst",
        "automation tester", "implementation specialist", "solutions consultant"
    ],
    "ERP / CRM": [
        "dynamics 365", "power platform", "salesforce administrator", "salesforce developer",
        "crm analyst", "erp analyst", "implementation consultant"
    ],
    "IT Support / Systems": [
        "systems administrator", "system administrator", "network engineer", "it support",
        "help desk", "desktop support", "cloud administrator", "windows administrator"
    ]
}

# =========================
# SAFETY CHECKS
# =========================

if not RAPIDAPI_KEY:
    raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")

if not SLACK_WEBHOOK_URL:
    raise RuntimeError("Missing SLACK_WEBHOOK_URL (or SLACK_WEBHOOK) secret in GitHub Actions.")

HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": API_HOST,
}

# =========================
# HELPERS
# =========================

def load_seen() -> Set[str]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
            if isinstance(data, dict) and "seen" in data and isinstance(data["seen"], list):
                return set(data["seen"])
    except FileNotFoundError:
        return set()
    except Exception:
        return set()
    return set()

def save_seen(seen: Set[str]) -> None:
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump({"seen": sorted(list(seen))}, f, ensure_ascii=False, indent=2)
    except Exception:
        # do not crash the job for cache issues
        pass

def slack_post(text: str) -> None:
    payload = {"text": text}
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=20)
    r.raise_for_status()

def stable_job_id(job: Dict[str, Any]) -> str:
    # Prefer API-provided job_id; fallback to hash of title+company+location+url
    raw = (
        str(job.get("job_id") or "")
        + "|"
        + str(job.get("job_title") or "")
        + "|"
        + str(job.get("employer_name") or "")
        + "|"
        + str(job.get("job_city") or "")
        + "|"
        + str(job.get("job_state") or "")
        + "|"
        + str(job.get("job_country") or "")
        + "|"
        + str(job.get("job_apply_link") or job.get("job_url") or "")
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

def pick_best_link(job: Dict[str, Any]) -> Optional[str]:
    # Prefer direct apply, then apply link, then job url
    direct = job.get("job_apply_link")
    url = job.get("job_url")
    # JSearch sometimes returns apply_options list
    apply_options = job.get("apply_options") or []
    direct_from_options = None

    if isinstance(apply_options, list):
        for opt in apply_options:
            if not isinstance(opt, dict):
                continue
            link = opt.get("apply_link") or opt.get("link")
            # some options include "direct" flags; if present, prefer it
            if opt.get("is_direct") is True and link:
                direct_from_options = link
                break
            if not direct_from_options and link:
                direct_from_options = link

    return direct_from_options or direct or url

def is_canada_job(job: Dict[str, Any]) -> bool:
    # Primary: country code
    cc = (job.get("job_country_code") or "").strip().upper()
    if cc == COUNTRY_CODE:
        return True

    # Secondary: job_country text
    country_text = (job.get("job_country") or "").strip().lower()
    if "canada" in country_text:
        return True

    # Tertiary: location strings
    city = (job.get("job_city") or "").strip().lower()
    state = (job.get("job_state") or "").strip().lower()
    location = (job.get("job_location") or "").strip().lower()
    combined = f"{city} {state} {location}".strip()

    # Must contain strong Canada signals; avoid US states/cities that slip in
    for kw in CANADA_KEYWORDS:
        if kw in combined:
            return True

    return False

def looks_like_us(job: Dict[str, Any]) -> bool:
    # Hard reject if the API says US
    cc = (job.get("job_country_code") or "").strip().upper()
    if cc == "US":
        return True
    country_text = (job.get("job_country") or "").strip().lower()
    if country_text == "united states" or "usa" in country_text:
        return True
    return False

def format_job(job: Dict[str, Any], group_name: str) -> Optional[str]:
    title = (job.get("job_title") or "").strip()
    company = (job.get("employer_name") or "").strip()
    city = (job.get("job_city") or "").strip()
    state = (job.get("job_state") or "").strip()
    country = (job.get("job_country") or "").strip()
    employment = (job.get("job_employment_type") or "").strip()
    remote = job.get("job_is_remote")

    link = pick_best_link(job)
    if not link:
        return None

    # Optional: filter to direct apply only if user insists (kept FALSE by default)
    if ONLY_DIRECT_APPLY:
        # only consider jobs with apply link
        if not (job.get("job_apply_link") or (job.get("apply_options") or [])):
            return None

    # Build a clean location line
    loc_parts = [p for p in [city, state, country] if p]
    loc = ", ".join(loc_parts) if loc_parts else "Canada"

    remote_tag = "Remote" if remote is True else "On-site/Hybrid"
    emp_tag = employment if employment else "Tech"

    # Slack-friendly single block
    return (
        f"*{group_name}*\n"
        f"*{title}* | {company}\n"
        f"{loc} • {remote_tag} • {emp_tag}\n"
        f"{link}"
    )

def jsearch_request(params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    Returns: (json, rate_limited)
    """
    attempt = 0
    rate_limited = False
    while True:
        attempt += 1
        try:
            r = requests.get(API_URL, headers=HEADERS, params=params, timeout=25)
            if r.status_code == 429:
                rate_limited = True
                if attempt > MAX_RETRIES_429:
                    return None, True
                # exponential backoff
                wait = 5 * attempt
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json(), rate_limited
        except requests.RequestException:
            if attempt > MAX_RETRIES_429:
                return None, rate_limited
            time.sleep(3 * attempt)

# =========================
# MAIN
# =========================

def main() -> None:
    seen = load_seen()
    posted_blocks: List[str] = []
    total_found = 0
    total_kept_canada = 0
    total_new = 0
    rate_limited_queries = 0

    for group_name, keywords in TECH_ROLE_GROUPS.items():
        # OR-based query
        quoted = [f"\"{k}\"" for k in keywords]
        query = " OR ".join(quoted)

        for page in range(1, PAGES_PER_QUERY + 1):
            params = {
                "query": query,
                "page": str(page),
                "num_pages": "1",
                "date_posted": DATE_POSTED,
                "remote_jobs_only": str(REMOTE_ONLY).lower(),
                # Keep country filter ON, but we also validate ourselves
                "country": COUNTRY_CODE,
                "employment_types": "",  # leave blank
            }

            data, rl = jsearch_request(params)
            if rl:
                rate_limited_queries += 1

            time.sleep(API_CALL_DELAY_SECONDS)

            if not data or "data" not in data or not isinstance(data["data"], list):
                continue

            jobs = data["data"]
            total_found += len(jobs)

            for job in jobs:
                if not isinstance(job, dict):
                    continue

                # Reject obvious US jobs first
                if looks_like_us(job):
                    continue

                if not is_canada_job(job):
                    continue

                total_kept_canada += 1

                jid = stable_job_id(job)
                if jid in seen:
                    continue

                block = format_job(job, group_name)
                if not block:
                    continue

                posted_blocks.append(block)
                seen.add(jid)
                total_new += 1

    save_seen(seen)

    # =========================
    # SLACK OUTPUT
    # =========================

    if total_new > 0:
        header = (
            f"✅ Aidrr Tech Jobs Bot: Posted {total_new} *new* Canada jobs.\n"
            f"Scanned: {total_found} results • Canada-kept: {total_kept_canada} • Rate-limited queries: {rate_limited_queries}"
        )
        slack_post(header)

        # Chunk messages to avoid Slack size limits
        chunk: List[str] = []
        size = 0
        for item in posted_blocks:
            if size + len(item) + 2 > SLACK_CHUNK_CHAR_LIMIT:
                slack_post("\n\n".join(chunk))
                chunk = []
                size = 0
            chunk.append(item)
            size += len(item) + 2
        if chunk:
            slack_post("\n\n".join(chunk))
        return

    # No new jobs
    if rate_limited_queries > 0:
        slack_post(
            "⚠️ No *new* Canada tech jobs posted this run.\n"
            f"Note: {rate_limited_queries} searches were rate-limited (429), so results may be incomplete.\n"
            "If this keeps happening, reduce PAGES_PER_QUERY, reduce role groups, or increase API_CALL_DELAY_SECONDS."
        )
    else:
        slack_post(
            "ℹ️ No *new* Canada tech jobs posted this run.\n"
            "This usually means the API returned jobs you already posted (seen cache), or nothing matched filters.\n"
            "If you want a fresh repost, delete/rename seen_jobs.json."
        )

if __name__ == "__main__":
    main()
