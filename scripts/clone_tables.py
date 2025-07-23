import argparse
import os
import re
from datetime import datetime
from git import Repo
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
SF_SCHEMA           = os.getenv('SNOWFLAKE_SCHEMA')

repo = Repo('.')
head_commit = repo.head.commit
parents = head_commit.parents

if not parents:
    print("🟡 Initial commit, considering all SQL files.")
    changed_files = [os.path.join(dp, f) for dp, _, filenames in os.walk(args.migrations_folder)
                     for f in filenames if f.endswith('.sql')]
else:
    diff = head_commit.diff(parents[0])
    changed_files = [d.b_path for d in diff if d.b_path and d.b_path.endswith('.sql') and args.migrations_folder in d.b_path]

print("🔍 Changed SQL files:", changed_files)
if not changed_files:
    print("✅ No changes in migration SQL files, skipping clone.")
    exit(0)

conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE
)

def clone_table(conn, full_table):
    if '.' in full_table:
        schema, table = full_table.split('.', 1)
    else:
        schema, table = SF_SCHEMA, full_table

    ts = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    backup_name = f"{schema}.{table}_backup_{ts}"
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE OR REPLACE TABLE {backup_name} CLONE {schema}.{table}")
        cur.execute(f"ALTER TABLE {backup_name} SET DATA_RETENTION_TIME_IN_DAYS = 1")
        print(f"✅ Cloned {schema}.{table} → {backup_name} (7‑day retention)")
    except Exception as e:
        print(f"⚠️ Failed to clone {schema}.{table}: {e}")
    finally:
        cur.close()

for file in changed_files:
    with open(file, 'r') as f:
        sql = f.read()

    matches = re.findall(r'ALTER TABLE (\w+)\.(\w+)', sql, re.IGNORECASE) + \
              re.findall(r'TRUNCATE TABLE (\w+)\.(\w+)', sql, re.IGNORECASE) + \
              re.findall(r'DROP TABLE (\w+)\.(\w+)', sql, re.IGNORECASE)

    for schema, table in matches:
        full_table = f"{schema}.{table}"
        clone_table(conn, full_table)

conn.close()
