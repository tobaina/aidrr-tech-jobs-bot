import os
import requests

# Get secrets from GitHub
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"]
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK"]

# RapidAPI endpoint
url = "https://jsearch.p.rapidapi.com/search"

querystring = {
    "query": "software engineer OR data analyst OR cybersecurity OR cloud engineer OR devops OR accounting manager in Canada",
    "page": "1",
    "num_pages": "1"
}

headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()

jobs = data.get("data", [])

if not jobs:
    print("No jobs found.")
    exit()

for job in jobs[:5]:  # Post top 5 jobs
    title = job.get("job_title")
    company = job.get("employer_name")
    location = job.get("job_city")
    link = job.get("job_apply_link")

    message = f"*{title}*\n{company} — {location}\n{link}"

    slack_payload = {
        "text": message
    }

    requests.post(SLACK_WEBHOOK, json=slack_payload)

print("Jobs posted successfully.")
