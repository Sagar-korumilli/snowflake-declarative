import argparse
import os
import re
from datetime import datetime
from git import Repo
import snowflake.connector

parser = argparse.ArgumentParser()
parser.add_argument('--migrations-folder', required=True)
args = parser.parse_args()

SNOWFLAKE_ACCOUNT             = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER                = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE                = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE           = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE            = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA                     = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_PRIVATE_KEY         = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

# Validate env
for var in [
    'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE',
    'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE',
    'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
]:
    if not globals().get(var):
        raise RuntimeError(f"❌ Missing environment variable: {var}")

# Determine changed .sql files
repo        = Repo('.')
commit      = repo.head.commit
parents     = commit.parents

if not parents:
    print("🟡 Initial commit, considering all SQL files.")
    changed_files = [
        os.path.join(dp, f)
        for dp, _, filenames in os.walk(args.migrations_folder)
        for f in filenames if f.endswith('.sql')
    ]
else:
    diff = commit.diff(parents[0])
    changed_files = [
        d.b_path for d in diff
        if d.b_path and d.b_path.endswith('.sql') and args.migrations_folder in d.b_path
    ]

print("🔍 Changed SQL files:", changed_files)
if not changed_files:
    print("✅ No changes in migration SQL files, skipping clone.")
    exit(0)

# Write key file
with open('key.pem', 'w') as f:
    f.write(SNOWFLAKE_PRIVATE_KEY)
os.chmod('key.pem', 0o600)

# Connect via key‑pair
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    authenticator='snowflake_jwt',
    private_key_file='key.pem',
    private_key_file_pwd=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
)

def clone_table(conn, full_table):
    schema, table = full_table.split('.', 1) if '.' in full_table else (SF_SCHEMA, full_table)
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
