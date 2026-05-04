# Full Python Script — client-style release notes with better table layout

import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from docx import Document
from docx.enum.section import WD_ORIENT
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
    for style_name, size in [
        ("Normal", 12),
        ("Title", 24),
        ("Heading 1", 16),
        ("Heading 2", 14),
        ("Heading 3", 12),
    ]:
        if style_name in doc.styles:
            style = doc.styles[style_name]
            style.font.name = FONT_NAME
            style.font.size = Pt(size)


def set_landscape(section):
    section.orientation = WD_ORIENT.LANDSCAPE
    section.page_width, section.page_height = section.page_height, section.page_width
    section.top_margin = Inches(0.45)
    section.bottom_margin = Inches(0.45)
    section.left_margin = Inches(0.45)
    section.right_margin = Inches(0.45)


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
        size = 16
    elif level == 2:
        p.style = doc.styles["Heading 2"]
        size = 14
    else:
        p.style = doc.styles["Heading 3"]
        size = 12

    run = p.add_run(text)
    apply_font(run, size=size, bold=True)
    return p


def format_cell(cell, text, size=10, bold=False, align=WD_PARAGRAPH_ALIGNMENT.LEFT):
    cell.text = ""
    p = cell.paragraphs[0]
    p.alignment = align
    p.paragraph_format.space_after = Pt(0)
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.line_spacing = 1.0
    run = p.add_run(text if text else "N/A")
    apply_font(run, size=size, bold=bold)
    cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.TOP


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
        merged_at = pr.get("merged_at", "")
        result.append(
            {
                "id": ", ".join(jira) if jira else f"PR-{pr['number']}",
                "title": pr.get("title", ""),
                "author": (pr.get("user") or {}).get("login", ""),
                "merged_at": format_merge_date(merged_at),
                "url": pr.get("html_url", ""),
            }
        )
    return result


def format_merge_date(merged_at):
    """Make the merge date shorter and more readable for the table."""
    try:
        dt = datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return merged_at or "N/A"


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

    # Landscape layout gives the table enough width for clean alignment.
    section = doc.sections[0]
    set_landscape(section)

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

    # Column widths tuned for landscape page.
    col_widths = [1.15, 4.0, 1.45, 1.7, 2.0]
    headers = ["Track ID", "Description", "Author", "Merge Date", "Reference"]

    for i, width in enumerate(col_widths):
        table.columns[i].width = Inches(width)
        for cell in table.columns[i].cells:
            cell.width = Inches(width)

    hdr_cells = table.rows[0].cells
    for i, header in enumerate(headers):
        format_cell(hdr_cells[i], header, size=10, bold=True, align=WD_PARAGRAPH_ALIGNMENT.CENTER)

    for pr in data:
        row = table.add_row().cells
        format_cell(row[0], pr["id"], size=9)
        format_cell(row[1], pr["title"], size=9)
        format_cell(row[2], pr["author"], size=9)
        format_cell(row[3], pr["merged_at"], size=9)
        format_cell(row[4], pr["url"], size=9)

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
