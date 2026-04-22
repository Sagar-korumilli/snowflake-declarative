import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.enum.table import WD_TABLE_ALIGNMENT

from copilot import CopilotClient, SubprocessConfig
from copilot.session import PermissionHandler

# -------------------------
# ENV
# -------------------------
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
COPILOT_GITHUB_TOKEN = os.environ["COPILOT_GITHUB_TOKEN"]
REPO = os.environ["GITHUB_REPOSITORY"]
BRANCH = os.environ["TARGET_BRANCH"]
START_DATE = os.environ["START_DATE"]
END_DATE = os.environ["END_DATE"]

OUTPUT_DIR = Path("output")


# -------------------------
# Helpers
# -------------------------

def heading(doc, text, level=1):
    doc.add_heading(text, level=level)


def add_cover_page(doc):
    p = doc.add_paragraph()
    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    r = p.add_run("Release Implementation Plan")
    r.bold = True
    r.font.size = Pt(22)

    p = doc.add_paragraph()
    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    p.add_run(f"Branch: {BRANCH}")

    p = doc.add_paragraph()
    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    p.add_run(
        f"Release Window: {START_DATE} through {END_DATE}"
    )

    doc.add_page_break()


def add_document_control(doc):
    heading(doc, "Document Control", 1)

    t = doc.add_table(rows=6, cols=2)
    t.style = "Table Grid"

    rows = [
        ("Document Name", "Release Implementation Plan"),
        ("Repository", REPO),
        ("Branch", BRANCH),
        ("Version", "1.0"),
        ("Prepared By", "GitHub Automated Workflow"),
        ("Generated Date", datetime.utcnow().strftime("%Y-%m-%d")),
    ]

    for i,(k,v) in enumerate(rows):
        t.cell(i,0).text = k
        t.cell(i,1).text = v


def is_within_range(merged_at):
    if not merged_at:
        return False

    merged_dt = datetime.fromisoformat(
        merged_at.replace("Z","+00:00")
    )

    start_dt = datetime.fromisoformat(
        START_DATE
    ).replace(tzinfo=timezone.utc)

    end_dt = datetime.fromisoformat(
        END_DATE + "T23:59:59"
    ).replace(tzinfo=timezone.utc)

    return start_dt <= merged_dt <= end_dt


# -------------------------
# Fetch PRs
# -------------------------

def fetch_prs():
    url = f"https://api.github.com/repos/{REPO}/pulls"

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    prs = []
    page = 1

    while True:
        params = {
            "state": "closed",
            "base": BRANCH,
            "page": page,
            "per_page": 100
        }

        r = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=60
        )

        if r.status_code != 200:
            raise Exception(r.text)

        batch = r.json()

        if not batch:
            break

        for pr in batch:
            if pr.get("merged_at") and is_within_range(pr["merged_at"]):
                prs.append(pr)

        if len(batch) < 100:
            break

        page += 1

    return prs


def prepare_data(prs):
    data=[]

    for pr in prs:
        jira = re.findall(
            r"[A-Z]+-\d+",
            pr.get("title","")
        )

        data.append({
            "id": ", ".join(jira) if jira else f"PR-{pr['number']}",
            "title": pr.get("title",""),
            "author": pr["user"]["login"],
            "merged_at": pr["merged_at"],
            "url": pr["html_url"]
        })

    return data


# -------------------------
# Copilot
# -------------------------

def extract_json(text):
    m = re.search(r"\{.*\}", text, re.DOTALL)

    if m:
        return json.loads(m.group(0))

    return {
      "introduction":"Release changes included.",
      "dependencies":"No major dependencies."
    }


async def generate_ai(data):

    client = CopilotClient(
      SubprocessConfig(
        github_token=COPILOT_GITHUB_TOKEN,
        use_logged_in_user=False
      )
    )

    async with client:

        session = await client.create_session(
            model="gpt-4.1",
            session_id="release-summary",
            on_permission_request=PermissionHandler.approve_all
        )

        prompt = f'''
Generate enterprise release-note JSON:

{{
 "introduction":"...",
 "dependencies":"..."
}}

Summarize business impact, risks and dependencies.

PR Data:
{json.dumps(data,indent=2)}
'''

        resp = await session.send_and_wait(prompt)

        return extract_json(resp.data.content)


# -------------------------
# Word Document
# -------------------------

def create_doc(ai,data):

    OUTPUT_DIR.mkdir(exist_ok=True)

    doc = Document()

    section = doc.sections[0]
    section.header.paragraphs[0].text = "Confidential Release Document"
    section.footer.paragraphs[0].text = "Generated via GitHub Workflow"


    add_cover_page(doc)
    add_document_control(doc)


    heading(doc,"1 Introduction")
    doc.add_paragraph(
      ai.get(
        "introduction",
        "This release contains approved enhancements delivered through merged pull requests."
      )
    )


    heading(doc,"2 Dependencies")
    doc.add_paragraph(
      ai.get(
        "dependencies",
        "No major dependencies identified."
      )
    )


    heading(doc,"3 Implementation Summary")

    t = doc.add_table(rows=1, cols=7)
    t.style = "Table Grid"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER

    headers=[
      "Track ID",
      "Description",
      "Author",
      "Merge Date",
      "Impact",
      "Risk",
      "Reference"
    ]

    for i,v in enumerate(headers):
        t.rows[0].cells[i].text=v

    for pr in data:
        r=t.add_row().cells
        r[0].text=pr["id"]
        r[1].text=pr["title"]
        r[2].text=pr["author"]
        r[3].text=pr["merged_at"]
        r[4].text="Functional Enhancement"
        r[5].text="Low"
        r[6].text=pr["url"]


    heading(doc,"4 Implementation Steps")

    steps=[
      "Validate deployment prerequisites",
      "Execute deployment workflow",
      "Run smoke validation",
      "Confirm object integrity",
      "Communicate completion"
    ]

    for s in steps:
        doc.add_paragraph(s, style="List Bullet")


    heading(doc,"5 Validation Plan")

    vt=doc.add_table(rows=4, cols=3)
    vt.style="Table Grid"

    vt.rows[0].cells[0].text="Test"
    vt.rows[0].cells[1].text="Owner"
    vt.rows[0].cells[2].text="Status"

    rows=[
      ("Schema Validation","QA","Pending"),
      ("Data Validation","QA","Pending"),
      ("Regression Check","Business","Pending")
    ]

    for i,(a,b,c) in enumerate(rows,start=1):
        vt.rows[i].cells[0].text=a
        vt.rows[i].cells[1].text=b
        vt.rows[i].cells[2].text=c


    heading(doc,"6 Communication Plan")

    ct=doc.add_table(rows=4, cols=3)
    ct.style="Table Grid"

    ct.rows[0].cells[0].text="Audience"
    ct.rows[0].cells[1].text="Method"
    ct.rows[0].cells[2].text="Owner"

    vals=[
      ("Stakeholders","Email","Release Manager"),
      ("Support Teams","Teams/Slack","Support Lead"),
      ("Business Users","Release Notice","Project Owner")
    ]

    for i,(a,b,c) in enumerate(vals,start=1):
        ct.rows[i].cells[0].text=a
        ct.rows[i].cells[1].text=b
        ct.rows[i].cells[2].text=c


    heading(doc,"7 Rollback Plan")
    doc.add_paragraph(
      "Approved rollback scripts in repository rollback folder will be executed if issues occur."
    )


    heading(doc,"8 Approvals")

    at=doc.add_table(rows=5, cols=4)
    at.style="Table Grid"

    cols=["Role","Name","Approval","Date"]
    for i,c in enumerate(cols):
        at.rows[0].cells[i].text=c

    roles=[
      "Development Lead",
      "QA Lead",
      "Release Manager",
      "Business Owner"
    ]

    for i,r in enumerate(roles,start=1):
        at.rows[i].cells[0].text=r


    file_path = OUTPUT_DIR / f"pr-summary-{BRANCH}.docx"
    doc.save(file_path)

    print(f"Saved: {file_path}")


# -------------------------
# Main
# -------------------------
async def main():

    print("Fetching PRs...")
    prs = fetch_prs()

    print(f"Filtered PRs: {len(prs)}")

    data = prepare_data(prs)

    if not data:
        create_doc(
            {
             "introduction":"No PRs found in selected range.",
             "dependencies":"N/A"
            },
            data
        )
        return

    print("Generating AI content...")
    ai = await generate_ai(data)

    print("Creating document...")
    create_doc(ai,data)


if __name__=="__main__":
    asyncio.run(main())
