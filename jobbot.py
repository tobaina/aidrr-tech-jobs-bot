import os
import json
import time
import random
from typing import Dict, List, Any, Set, Tuple, Optional

import requests

# =========================
# CONFIG (TUNE IF NEEDED)
# =========================

API_URL = "https://jsearch27.p.rapidapi.com/search"
API_HOST = "jsearch27.p.rapidapi.com"

# Reduce calls = fewer 429
PAGES_PER_QUERY = int(os.getenv("PAGES_PER_QUERY", "1"))  # keep 1 to avoid rate-limits
DATE_POSTED = os.getenv("DATE_POSTED", "week")            # day | week | month | all
REMOTE_ONLY = os.getenv("REMOTE_ONLY", "false").lower() == "true"

# Rate-limit protection
API_CALL_DELAY_SECONDS = float(os.getenv("API_CALL_DELAY_SECONDS", "5"))  # 4–8 is good
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = float(os.getenv("BACKOFF_BASE_SECONDS", "5"))      # backoff start

# Slack message chunk limit (Slack safe)
SLACK_CHUNK_CHAR_LIMIT = 3300

# Seen cache (prevents reposting same job_id across runs IF file persists)
SEEN_FILE = "seen_jobs.json"

# =========================
# ROLE GROUPS (BA/Product focus)
# =========================

ROLE_GROUPS: Dict[str, List[str]] = {
    "Business Analysis": [
        '"business analyst"',
        '"business systems analyst"',
        '"requirements analyst"',
        '"technical analyst"',
        '"functional analyst"',
        '"process analyst"',
        '"data analyst"',
        '"bi analyst"',
        '"reporting analyst"',
    ],
    "Product": [
        '"product analyst"',
        '"product owner"',
        '"technical product owner"',
        '"product manager"',
        '"associate product manager"',
        '"implementation analyst"',
        '"implementation specialist"',
        '"solutions consultant"',
    ],
    "Delivery / Program": [
        '"project manager"',
        '"technical project manager"',
        '"program manager"',
        '"delivery manager"',
        '"scrum master"',
        '"agile coach"',
        '"project coordinator"',
    ],
}

# =========================
# CANADA HARD FILTER
# =========================

CANADA_KEYWORDS = [
    "canada", "on, canada", "bc, canada", "ab, canada", "sk, canada", "mb, canada",
    "qc, canada", "nb, canada", "ns, canada", "nl, canada", "pe, canada", "nt, canada",
    "nu, canada", "yt, canada",
    "ontario", "toronto", "mississauga", "brampton", "waterloo", "kitchener", "cambridge",
    "guelph", "london", "ottawa", "hamilton", "vancouver", "calgary", "edmonton",
    "montreal", "winnipeg", "halifax", "victoria", "burnaby", "surrey", "markham",
]

US_COUNTRY_HINTS = [
    "united states", "usa", ", us", " us ", "new york", "san francisco", "chicago",
    "seattle", "boston", "miami", "atlanta", "denver", "los angeles", "dallas",
]


def is_canada_job(job: Dict[str, Any]) -> bool:
    # Strongest signal: job_country
    country = str(job.get("job_country", "")).strip().lower()
    if country == "canada":
        return True
    if country in ("united states", "usa"):
        return False

    # Fallback to location text
    text = " ".join([
        str(job.get("job_city", "")),
        str(job.get("job_state", "")),
        str(job.get("job_country", "")),
        str(job.get("job_location", "")),
    ]).lower()

    # If it smells strongly like US, reject
    if any(bad in text for bad in US_COUNTRY_HINTS):
        return False

    # Otherwise must contain Canada signals
    return any(k in text for k in CANADA_KEYWORDS)


# =========================
# HELPERS
# =========================

def load_seen() -> Set[str]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data)
        return set()
    except FileNotFoundError:
        return set()
    except Exception:
        return set()


def save_seen(seen: Set[str]) -> None:
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(seen)), f, indent=2)
    except Exception:
        pass


def get_secrets() -> Tuple[str, str]:
    rapidapi_key = os.environ.get("RAPIDAPI_KEY", "").strip()

    slack_webhook_url = (
        os.environ.get("SLACK_WEBHOOK_URL", "").strip()
        or os.environ.get("SLACK_WEBHOOK", "").strip()
    )

    if not rapidapi_key:
        raise RuntimeError("Missing RAPIDAPI_KEY secret in GitHub Actions.")
    if not slack_webhook_url:
        raise RuntimeError("Missing SLACK_WEBHOOK_URL (or SLACK_WEBHOOK) secret in GitHub Actions.")
    return rapidapi_key, slack_webhook_url


def slack_post(slack_webhook_url: str, text: str) -> None:
    payload = {"text": text}
    r = requests.post(slack_webhook_url, json=payload, timeout=30)
    r.raise_for_status()


def best_job_link(job: Dict[str, Any]) -> str:
    # Prefer direct apply link (most stable)
    apply_link = job.get("job_apply_link")
    if isinstance(apply_link, str) and apply_link.startswith("http"):
        return apply_link

    # Next best options
    for key in ["job_offer_url", "job_link", "job_google_link"]:
        val = job.get(key)
        if isinstance(val, str) and val.startswith("http"):
            return val

    return ""


def clean_text(s: Any, max_len: int = 160) -> str:
    if not s:
        return ""
    txt = str(s).replace("\n", " ").strip()
    if len(txt) > max_len:
        return txt[: max_len - 3] + "..."
    return txt


def format_job(job: Dict[str, Any], group_name: str) -> str:
    title = clean_text(job.get("job_title"), 120) or "Job"
    company = clean_text(job.get("employer_name"), 80) or "Company"
    city = clean_text(job.get("job_city"), 40)
    state = clean_text(job.get("job_state"), 40)
    country = clean_text(job.get("job_country"), 40)
    location_parts = [p for p in [city, state, country] if p]
    location = ", ".join(location_parts) if location_parts else clean_text(job.get("job_location"), 80)

    remote_type = clean_text(job.get("job_employment_type"), 40)
    posted = clean_text(job.get("job_posted_at_datetime_utc") or job.get("job_posted_at_timestamp"), 60)

    link = best_job_link(job)
    if not link:
        link = "(no link provided)"

    # Slack-friendly single block
    return (
        f"*{group_name}* — *{title}* | {company}\n"
        f"{location}\n"
        f"{link}"
    )


# =========================
# JSEARCH CALL WITH RETRIES
# =========================

def jsearch_call(headers: Dict[str, str], params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Returns (jobs, rate_limited_flag)
    """
    rate_limited = False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(API_URL, headers=headers, params=params, timeout=45)

            # Handle 429
            if r.status_code == 429:
                rate_limited = True
                backoff = BACKOFF_BASE_SECONDS * attempt + random.uniform(0, 2)
                time.sleep(backoff)
                continue

            r.raise_for_status()
            data = r.json()
            jobs = data.get("data") or data.get("jobs") or []
            if not isinstance(jobs, list):
                jobs = []
            return jobs, rate_limited

        except requests.RequestException:
            # retry with backoff
            backoff = BACKOFF_BASE_SECONDS * attempt + random.uniform(0, 2)
            time.sleep(backoff)
            continue
        except Exception:
            break

    return [], rate_limited


def search_group(headers: Dict[str, str], group_name: str, terms: List[str]) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Build one query per group (OR terms), then fetch.
    """
    query = " OR ".join(terms)

    params = {
        "query": query,
        "page": 1,
        "num_pages": PAGES_PER_QUERY,
        "date_posted": DATE_POSTED,
        "remote_jobs_only": REMOTE_ONLY,
        "location": "Canada",  # key improvement over "in Canada" text
    }

    # Gentle delay between groups
    time.sleep(API_CALL_DELAY_SECONDS)

    jobs, rate_limited = jsearch_call(headers, params)

    # Hard-filter Canada results
    jobs = [j for j in jobs if is_canada_job(j)]

    return jobs, rate_limited


def chunk_and_post(slack_webhook_url: str, lines: List[str]) -> None:
    """
    Slack payload limits: keep chunks safe
    """
    chunk: List[str] = []
    size = 0

    for line in lines:
        if size + len(line) + 1 > SLACK_CHUNK_CHAR_LIMIT:
            slack_post(slack_webhook_url, "\n\n".join(chunk))
            chunk = []
            size = 0

        chunk.append(line)
        size += len(line) + 1

    if chunk:
        slack_post(slack_webhook_url, "\n\n".join(chunk))


def main() -> None:
    rapidapi_key, slack_webhook_url = get_secrets()

    headers = {
        "X-RapidAPI-Key": rapidapi_key,
        "X-RapidAPI-Host": API_HOST,
    }

    seen = load_seen()

    posted_lines: List[str] = []
    rate_limited_groups: List[str] = []
    total_found = 0

    for group_name, terms in ROLE_GROUPS.items():
        jobs, was_rate_limited = search_group(headers, group_name, terms)

        if was_rate_limited:
            rate_limited_groups.append(group_name)

        if not jobs:
            continue

        for job in jobs:
            job_id = str(job.get("job_id") or job.get("job_google_link") or job.get("job_link") or "")
            if not job_id:
                continue

            if job_id in seen:
                continue

            seen.add(job_id)
            posted_lines.append(format_job(job, group_name))
            total_found += 1

    save_seen(seen)

    # Output to Slack
    if posted_lines:
        header = f"✅ Aidrr Tech Jobs Bot: Posted {len(posted_lines)} Canada jobs (BA/Product focus)."
        if rate_limited_groups:
            header += f" (Rate-limited on: {', '.join(rate_limited_groups)}; continued.)"

        slack_post(slack_webhook_url, header)
        chunk_and_post(slack_webhook_url, posted_lines)

    else:
        msg = "No new Canada BA/Product/Delivery jobs found in this run."
        if rate_limited_groups:
            msg += f"\n⚠️ Note: {len(rate_limited_groups)} searches were rate-limited (429): {', '.join(rate_limited_groups)}."
            msg += "\nIf this keeps happening: lower PAGES_PER_QUERY=1, increase API_CALL_DELAY_SECONDS=6–8, or reduce role groups."
        slack_post(slack_webhook_url, msg)


if __name__ == "__main__":
    main()
