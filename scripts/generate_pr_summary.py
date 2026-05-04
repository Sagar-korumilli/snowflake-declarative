# Full Python Script — simplified client-style release notes

import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.shared import Inches, Pt

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
FONT_NAME = "Times New Roman"


# -------------------------
# STYLE HELPERS
# -------------------------
def apply_font(run, size=None, bold=None, italic=None):
    run.font.name = FONT_NAME
    if size is not None:
        run.font.size = Pt(size)
    if bold is not None:
        run.bold = bold
    if italic is not None:
        run.italic = italic


def set_document_font(doc):
    styles = ["Normal", "Title", "Heading 1", "Heading 2", "Heading 3"]
    for style_name in styles:
        if style_name in doc.styles:
            style = doc.styles[style_name]
            style.font.name = FONT_NAME
            if style_name == "Normal":
                style.font.size = Pt(12)
            elif style_name == "Title":
                style.font.size = Pt(24)
            elif style_name == "Heading 1":
                style.font.size = Pt(16)
            elif style_name == "Heading 2":
                style.font.size = Pt(14)
            elif style_name == "Heading 3":
                style.font.size = Pt(12)


def add_paragraph_with_font(doc, text, size=12, bold=False, italic=False, align=None):
    p = doc.add_paragraph()
    if align is not None:
        p.alignment = align
    run = p.add_run(text)
    apply_font(run, size=size, bold=bold, italic=italic)
    return p


def add_heading_with_font(doc, text, level=1):
    p = doc.add_paragraph()
    if level == 1:
        p.style = doc.styles["Heading 1"]
    elif level == 2:
        p.style = doc.styles["Heading 2"]
    elif level == 3:
        p.style = doc.styles["Heading 3"]
    run = p.add_run(text)
    apply_font(run, size=16 if level == 1 else 14 if level == 2 else 12, bold=True)
    return p


def format_cell(cell, text, size=11, bold=False, align=WD_PARAGRAPH_ALIGNMENT.LEFT):
    cell.text = ""
    p = cell.paragraphs[0]
    p.alignment = align
    run = p.add_run(text if text else "N/A")
    apply_font(run, size=size, bold=bold)
    cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER


# -------------------------
# DATE FILTER
# -------------------------
def is_within_range(merged_at):
    if not merged_at:
        return False

    merged_dt = datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
    start_dt = datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(END_DATE + "T23:59:59").replace(tzinfo=timezone.utc)
    return start_dt <= merged_dt <= end_dt


# -------------------------
# FETCH PRs
# -------------------------
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
            "page": page,
            "per_page": 100,
            "sort": "updated",
            "direction": "desc",
        }

        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"GitHub API failed: {r.status_code} {r.text}")

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
    result = []
    for pr in prs:
        jira = re.findall(r"[A-Z]+-\d+", pr.get("title", "") or "")
        result.append(
            {
                "id": ", ".join(jira) if jira else f"PR-{pr['number']}",
                "title": pr.get("title", ""),
                "author": (pr.get("user") or {}).get("login", ""),
                "merged_at": pr.get("merged_at", ""),
                "url": pr.get("html_url", ""),
            }
        )
    return result


# -------------------------
# COPILOT
# -------------------------
def extract_json(text):
    text = (text or "").strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    return {
        "introduction": "Release changes included.",
        "dependencies": "No major dependencies identified.",
    }


async def generate_ai(data):
    client = CopilotClient(
        SubprocessConfig(
            github_token=COPILOT_GITHUB_TOKEN,
            use_logged_in_user=False,
        )
    )

    async with client:
        session = await client.create_session(
            model="gpt-4.1",
            session_id="release-summary",
            on_permission_request=PermissionHandler.approve_all,
        )

        prompt = f'''
Generate concise enterprise release-note JSON only.

Return exactly:
{{
  "introduction": "...",
  "dependencies": "..."
}}

Use professional language for release notes.
Do not add markdown.

PR Data:
{json.dumps(data, indent=2)}
'''

        resp = await session.send_and_wait(prompt)
        return extract_json(resp.data.content)


# -------------------------
# WORD DOCUMENT
# -------------------------
def create_doc(ai, data):
    OUTPUT_DIR.mkdir(exist_ok=True)

    doc = Document()
    set_document_font(doc)

    # Page margins a little tighter for a corporate report feel
    section = doc.sections[0]
    section.top_margin = Inches(0.6)
    section.bottom_margin = Inches(0.6)
    section.left_margin = Inches(0.7)
    section.right_margin = Inches(0.7)

    # Title
    title = doc.add_paragraph()
    title.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    run = title.add_run("Release Management\nDocument - Release Notes\nfor Dev Main Branch")
    apply_font(run, size=24, bold=False)

    doc.add_paragraph("")

    # Introduction
    add_heading_with_font(doc, "1. Introduction", level=1)
    intro_text = ai.get(
        "introduction",
        "Will be created for a Sprint and will include all user stories and corresponding packages that will be merged to Dev Master."
    )
    add_paragraph_with_font(doc, intro_text, size=12)

    # Dependencies
    add_heading_with_font(doc, "2. Dependencies", level=1)
    dep_text = ai.get(
        "dependencies",
        "This release depends on successful integration of updated SQL views and downstream validation by the data engineering and QA teams."
    )
    add_paragraph_with_font(doc, dep_text, size=12)

    # Summary table only
    add_heading_with_font(doc, "3 Implementation Summary", level=1)

    table = doc.add_table(rows=1, cols=5)
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False

    widths = [1.0, 3.9, 1.25, 1.25, 2.35]
    headers = ["Track ID", "Description", "Author", "Merge Date", "Reference"]

    for i, width in enumerate(widths):
        for cell in table.columns[i].cells:
            cell.width = Inches(width)

    hdr_cells = table.rows[0].cells
    for i, header in enumerate(headers):
        format_cell(hdr_cells[i], header, size=11, bold=True)

    for pr in data:
        row = table.add_row().cells
        format_cell(row[0], pr["id"], size=10)
        format_cell(row[1], pr["title"], size=10)
        format_cell(row[2], pr["author"], size=10)
        format_cell(row[3], pr["merged_at"], size=10)
        format_cell(row[4], pr["url"], size=10)

    file_path = OUTPUT_DIR / f"pr-summary-{BRANCH}.docx"
    doc.save(file_path)
    print(f"Saved: {file_path}")


# -------------------------
# MAIN
# -------------------------
async def main():
    print("Fetching PRs...")
    prs = fetch_prs()
    print(f"Filtered PRs: {len(prs)}")

    data = prepare_data(prs)

    if not data:
        create_doc(
            {
                "introduction": "No PRs found in the selected date range.",
                "dependencies": "N/A",
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
