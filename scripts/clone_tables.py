import argparse
import os
import re
import sys
from datetime import datetime
from git import Repo
import snowflake.connector

parser = argparse.ArgumentParser()
parser.add_argument('--snowflake-root', required=True, help="Root folder containing schema subfolders")
args = parser.parse_args()

# Env vars (do not rename)
SNOWFLAKE_ACCOUNT              = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER                 = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE                 = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE            = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE             = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_PRIVATE_KEY          = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

# Check env vars
for var in [
    'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE',
    'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE',
    'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
]:
    if not globals().get(var):
        raise RuntimeError(f"❌ Missing environment variable: {var}")

# Git changed files (latest commit)
repo = Repo('.')
commit = repo.head.commit
parents = commit.parents

if not parents:
    print("🟡 Initial commit — all .sql files considered.")
    changed_files = [
        os.path.join(dp, f)
        for dp, _, filenames in os.walk(args.snowflake_root)
        if "backup" not in dp
        for f in filenames if f.endswith('.sql')
    ]
else:
    diff = commit.diff(parents[0])
    changed_files = [
        d.b_path for d in diff
        if d.b_path and d.b_path.endswith('.sql') and args.snowflake_root in d.b_path and '/backup/' not in d.b_path
    ]

if not changed_files:
    print("✅ No changed SQL files; skipping backups.")
    sys.exit(0)

print("🔍 Changed SQL files:", changed_files)

# Save key to disk
with open('key.pem', 'w') as f:
    f.write(SNOWFLAKE_PRIVATE_KEY)
os.chmod('key.pem', 0o600)

# Connect
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    private_key_file='key.pem',
    private_key_file_pwd=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
    authenticator='snowflake_jwt'
)

def clone_table(conn, full_table):
    schema, table = full_table.split('.', 1)
    ts = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    backup = f"{schema}.{table}_backup_{ts}"
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE OR REPLACE TABLE {backup} CLONE {schema}.{table}")
        cur.execute(f"ALTER TABLE {backup} SET DATA_RETENTION_TIME_IN_DAYS = 1")
        print(f"✅ Cloned {schema}.{table} → {backup}")
    except Exception as e:
        print(f"⚠️ Failed to clone {schema}.{table}: {e}")
    finally:
        cur.close()

# Extract table names from changed files
for file in changed_files:
    sql = open(file).read()
    matches = (
        re.findall(r'ALTER TABLE (\w+)\.(\w+)', sql, re.IGNORECASE) +
        re.findall(r'TRUNCATE TABLE (\w+)\.(\w+)', sql, re.IGNORECASE) +
        re.findall(r'DROP TABLE (\w+)\.(\w+)', sql, re.IGNORECASE)
    )
    for schema, table in matches:
        clone_table(conn, f"{schema}.{table}")

conn.close()
