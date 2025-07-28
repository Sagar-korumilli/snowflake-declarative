# quick_sql_warnings.py
# ----------------------
# Scans all SQL files under each schema-level folder for destructive patterns,
# emitting warnings but never failing the pipeline.

import os
import re
import sys
import argparse

# Destructive patterns to warn on (regex strings)
WARN_PATTERNS = [
    r'DROP\s+TABLE',
    r'DROP\s+COLUMN',
    r'TRUNCATE\s+TABLE',
    r'ALTER\s+TABLE\s+[^\s]+\s+DROP\s+COLUMN'
]

# Compile regexes once
glob_patterns = [re.compile(pat, re.IGNORECASE) for pat in WARN_PATTERNS]

parser = argparse.ArgumentParser(
    description='Scan schema-level folders for destructive SQL patterns.'
)
parser.add_argument(
    '--snowflake-root', required=True,
    help='Top-level directory containing schema-named subfolders'
)
args = parser.parse_args()

warnings = []
# Walk each schema folder
for entry in os.listdir(args.snowflake_root):
    folder_path = os.path.join(args.snowflake_root, entry)
    # ignore non-folders and any backup subfolder
    if not os.path.isdir(folder_path) or entry.lower() == 'backup':
        continue
    # scan each .sql file
    for fname in os.listdir(folder_path):
        if not fname.lower().endswith('.sql'):
            continue
        path = os.path.join(folder_path, fname)
        try:
            content = open(path, 'r', encoding='utf-8').read()
        except Exception as e:
            print(f"⚠️ Could not read {path}: {e}")
            continue
        for rx, pat in zip(glob_patterns, WARN_PATTERNS):
            if rx.search(content):
                warnings.append(f"{entry}/{fname}: matched '{pat}'")

# Print results
if warnings:
    print("⚠️ SQL warnings detected (no blocking patterns):")
    for w in warnings:
        print(f"  - {w}")
else:
    print("✅ No destructive SQL patterns detected.")

# Always succeed
sys.exit(0)
