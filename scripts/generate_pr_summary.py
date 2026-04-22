import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from docx import Document

from copilot import CopilotClient, SubprocessConfig
from copilot.session import PermissionHandler

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
COPILOT_GITHUB_TOKEN = os.environ["COPILOT_GITHUB_TOKEN"]
REPO = os.environ["GITHUB_REPOSITORY"]
BRANCH = os.environ["TARGET_BRANCH"]
START_DATE = os.environ["START_DATE"]
END_DATE = os.environ["END_DATE"]

OUTPUT_DIR = Path("output")


def is_within_range(merged_at: str) -> bool:
    if not merged_at:
        return False

    merged_dt = datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
    start_dt = datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(END_DATE + "T23:59:59").replace(tzinfo=timezone.utc)
    return start_dt <= merged_dt <= end_dt


def fetch_prs():
    url = f"https://api.github.com/repos/{REPO}/pulls"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    prs = []
    page = 1

    while True:
        params = {
            "state": "closed",
            "base": BRANCH,
            "per_page": 100,
            "page": page,
        }

        res = requests.get(url, headers=headers, params=params, timeout=60)
        if res.status_code != 200:
            raise RuntimeError(f"GitHub API failed: {res.status_code} {res.text}")

        data = res.json()
        if not data:
            break

        for pr in data:
            if pr.get("merged_at") and is_within_range(pr["merged_at"]):
                prs.append(pr)

        if len(data) < 100:
            break

        page += 1

    return prs


def prepare_data(prs):
    result = []
    for pr in prs:
        jira = re.findall(r"[A-Z]+-\d+", pr.get("title", ""))
        result.append(
            {
                "id": ", ".join(jira) if jira else f"PR-{pr['number']}",
                "title": pr.get("title", ""),
                "author": pr.get("user", {}).get("login", ""),
                "merged_at": pr.get("merged_at", ""),
                "url": pr.get("html_url", ""),
            }
        )
    return result


def extract_json(text: str):
    text = text.strip()

    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        text = match.group(0)

    return json.loads(text)


async def generate_ai(data):
    client = CopilotClient(
        SubprocessConfig(
            github_token=COPILOT_GITHUB_TOKEN,
            use_logged_in_user=False,
        )
    )

    async with client:
        session = await client.create_session(
            model="gpt-5",
            session_id=f"release-{int(datetime.now().timestamp())}",
            on_permission_request=PermissionHandler.approve_all,
        )

        prompt = f"""
Generate release notes in JSON format only.

Return exactly:
{{
  "introduction": "short intro",
  "dependencies": "dependencies if any",
  "rows": []
}}

Rules:
- Keep it short and professional
- Do not include markdown
- Return ONLY JSON
- Align rows with the PR data below

PR data:
{json.dumps(data, indent=2)}
"""

        response = await session.send_and_wait(prompt)
        return extract_json(response.data.content)


def create_doc(ai, data):
    OUTPUT_DIR.mkdir(exist_ok=True)

    doc = Document()
    doc.add_heading(f"Release Notes - {BRANCH}", 0)

    doc.add_heading("1. Introduction", level=1)
    doc.add_paragraph(ai.get("introduction", "N/A"))

    doc.add_heading("2. Dependencies", level=1)
    doc.add_paragraph(ai.get("dependencies", "N/A"))

    doc.add_heading("3. Summary", level=1)

    table = doc.add_table(rows=1, cols=5)
    table.style = "Table Grid"
    headers = ["ID", "Description", "Author", "Merged Date", "Link"]

    for i, h in enumerate(headers):
        table.rows[0].cells[i].text = h

    for pr in data:
        row = table.add_row().cells
        row[0].text = pr["id"]
        row[1].text = pr["title"]
        row[2].text = pr["author"]
        row[3].text = pr["merged_at"]
        row[4].text = pr["url"]

    file_path = OUTPUT_DIR / f"pr-summary-{BRANCH}.docx"
    doc.save(file_path)
    print(f"Saved: {file_path}")


async def main():
    print("Fetching PRs...")
    prs = fetch_prs()
    print(f"Filtered PRs: {len(prs)}")

    data = prepare_data(prs)

    if not data:
        print("No PRs found in date range.")
        create_doc(
            {
                "introduction": "No merged PRs found for the selected branch and date range.",
                "dependencies": "N/A",
                "rows": [],
            },
            data,
        )
        return

    print("Generating AI content...")
    ai = await generate_ai(data)

    print("Creating document...")
    create_doc(ai, data)


if __name__ == "__main__":
    asyncio.run(main())
