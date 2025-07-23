import argparse
import os
import subprocess
import re
from datetime import datetime
import snowflake.connector

parser = argparse.ArgumentParser()
parser.add_argument('--migrations-folder', required=True)
args = parser.parse_args()

SNOWFLAKE_ACCOUNT   = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER      = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD  = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE      = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE  = os.getenv('SNOWFLAKE_DATABASE')

# Use git diff from previous to current commit
try:
    base_sha = subprocess.check_output(["git", "rev-parse", "HEAD^"]).decode().strip()
    head_sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    diff_output = subprocess.check_output(
        ["git", "diff", "--name-only", base_sha, head_sha]
    ).decode().splitlines()

    changed_files = [
        f for f in diff_output
        if f.endswith(".sql") and args.migrations_folder in f
    ]
except subprocess.CalledProcessError as e:
    print("⚠️ Git diff failed:", e)
    changed_files = []

if not changed_files:
    print("✅ No relevant changed SQL files found, skipping clone.")
    exit(0)

print("🔍 Changed SQL files:", changed_files)

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE
)
cur = conn.cursor()

# Look for ALTER/TRUNCATE/DROP and clone those tables
for file_path in changed_files:
    with open(file_path, "r") as f:
        sql = f.read()

    matches = re.findall(
        r'(?:ALTER|TRUNCATE|DROP)\s+TABLE\s+(\w+)\.(\w+)', sql, re.IGNORECASE
    )
    for schema, table in matches:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        backup_name = f"{table}_backup_{timestamp}"
        clone_sql = f"""CREATE OR REPLACE TABLE {schema}.{backup_name} 
                        CLONE {schema}.{table} 
                        RETENTION_TIME = 7;"""

        print(f"🛡️ Cloning {schema}.{table} → {schema}.{backup_name}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cur.execute(clone_sql)

cur.close()
conn.close()
