import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path

import requests
from docx import Document
from copilot import CopilotClient
from copilot.session import PermissionHandler

# ENV VARIABLES
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
REPO = os.environ["GITHUB_REPOSITORY"]
BRANCH = os.environ["TARGET_BRANCH"]
START_DATE = os.environ["START_DATE"]
END_DATE = os.environ["END_DATE"]
COPILOT_CLI_URL = os.environ["COPILOT_CLI_URL"]

OUTPUT_DIR = Path("output")


# -------------------------------
# Date filter
# -------------------------------
def is_within_range(merged_at):
    if not merged_at:
        return False

    merged_dt = datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
    start_dt = datetime.fromisoformat(START_DATE)
    end_dt = datetime.fromisoformat(END_DATE + "T23:59:59")

    return start_dt <= merged_dt <= end_dt


# -------------------------------
# Fetch PRs
# -------------------------------
def fetch_prs():
    url = f"https://api.github.com/repos/{REPO}/pulls"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

    prs = []
    page = 1

    while True:
        params = {
            "state": "closed",
            "base": BRANCH,
            "per_page": 100,
            "page": page
        }

        res = requests.get(url, headers=headers, params=params)
        data = res.json()

        if not data:
            break

        for pr in data:
            if pr.get("merged_at") and is_within_range(pr["merged_at"]):
                prs.append(pr)

        page += 1

    return prs


# -------------------------------
# Prepare data
# -------------------------------
def prepare_data(prs):
    result = []

    for pr in prs:
        jira = re.findall(r"[A-Z]+-\d+", pr["title"] or "")

        result.append({
            "id": ", ".join(jira) if jira else f"PR-{pr['number']}",
            "title": pr["title"],
            "author": pr["user"]["login"],
            "merged_at": pr["merged_at"],
            "url": pr["html_url"]
        })

    return result


# -------------------------------
# Copilot SDK
# -------------------------------
async def generate_ai(data):
    client = CopilotClient({"cli_url": COPILOT_CLI_URL})
    await client.start()

    try:
        session = await client.create_session(
            model="gpt-5",
            session_id="release-notes",
            on_permission_request=PermissionHandler.approve_all
        )

        prompt = f"""
        Generate release notes in JSON:

        {{
          "introduction": "...",
          "dependencies": "...",
          "rows": []
        }}

        Data:
        {json.dumps(data)}
        """

        response = await session.send_and_wait({"prompt": prompt})
        return json.loads(response.data.content)

    finally:
        await client.stop()


# -------------------------------
# Generate Word document
# -------------------------------
def create_doc(ai, data):
    OUTPUT_DIR.mkdir(exist_ok=True)

    doc = Document()
    doc.add_heading(f"Release Notes - {BRANCH}", 0)

    doc.add_heading("1. Introduction", 1)
    doc.add_paragraph(ai.get("introduction", ""))

    doc.add_heading("2. Dependencies", 1)
    doc.add_paragraph(ai.get("dependencies", ""))

    doc.add_heading("3. Summary", 1)

    table = doc.add_table(rows=1, cols=5)
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


# -------------------------------
# MAIN
# -------------------------------
async def main():
    print("Fetching PRs...")
    prs = fetch_prs()

    print(f"Filtered PRs: {len(prs)}")

    data = prepare_data(prs)

    print("Generating AI content...")
    ai = await generate_ai(data)

    print("Creating document...")
    create_doc(ai, data)


if __name__ == "__main__":
    asyncio.run(main())
