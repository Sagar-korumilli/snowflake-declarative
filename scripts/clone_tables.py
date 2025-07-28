# clone_tables.py
# ----------------
# After detecting changed SQL files under schema folders, this script:
# 1. Identifies changed .sql files in git for each schema-level folder
# 2. Parses those files for ALTER/TRUNCATE/DROP TABLE operations
# 3. Clones each affected table into a timestamped zero-copy backup in its schema

import argparse
import os
import re
from datetime import datetime
from git import Repo
import snowflake.connector

parser = argparse.ArgumentParser(
    description='Clone impacted tables based on SQL schema folder changes'
)
parser.add_argument(
    '--snowflake-root', required=True,
    help='Top-level directory containing schema-named subfolders'
)
args = parser.parse_args()

# Load environment variables
SNOWFLAKE_ACCOUNT    = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER       = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE       = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE  = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE   = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_PRIVATE_KEY           = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE= os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

# Validate environment
for var in [
    'SNOWFLAKE_ACCOUNT','SNOWFLAKE_USER','SNOWFLAKE_ROLE',
    'SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE',
    'SNOWFLAKE_PRIVATE_KEY','SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
]:
    if not os.getenv(var):
        raise RuntimeError(f"❌ Missing environment variable: {var}")

# Prepare key file for authentication
with open('key.pem','w') as f:
    f.write(SNOWFLAKE_PRIVATE_KEY)
os.chmod('key.pem', 0o600)

# Connect to Snowflake
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

# Determine changed .sql files in this commit
repo = Repo('.')
commit = repo.head.commit
parent = commit.parents[0] if commit.parents else None
diffs = commit.diff(parent) if parent else []
changed_files = []

top_level = args.snowflake_root
# Iterate each schema folder (ignore any 'backup')
for entry in os.listdir(top_level):
    folder = os.path.join(top_level, entry)
    if not os.path.isdir(folder) or entry.lower() == 'backup':
        continue
    # Collect changed SQL files under this schema
    for diff in diffs:
        path = diff.b_path
        if path and path.startswith(f"{top_level}/{entry}/") and path.endswith('.sql'):
            rel = path.replace(f"{top_level}/{entry}/", '')
            if not rel.startswith('backup/'):
                changed_files.append(path)

if not changed_files:
    print("✅ No changed SQL files; skipping backups.")
    conn.close()
    sys.exit(0)

print("🔍 Changed SQL files:", changed_files)

# Regex to capture schema & table from table operations
pattern = re.compile(
    r"(?:ALTER|TRUNCATE|DROP)\s+TABLE\s+([0-9A-Za-z_]+)\.([0-9A-Za-z_]+)",
    re.IGNORECASE
)

# Collect unique impacted tables
tables_to_clone = set()
for file_path in changed_files:
    with open(file_path, 'r') as f:
        content = f.read()
    for sch, tbl in pattern.findall(content):
        tables_to_clone.add(f"{sch}.{tbl}")

# Function to clone a table

def clone_table(full_name):
    schema, table = full_name.split('.')
    ts = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    backup_name = f"{schema}.{table}_backup_{ts}"
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE OR REPLACE TABLE {backup_name} CLONE {schema}.{table}")
        cur.execute(f"ALTER TABLE {backup_name} SET DATA_RETENTION_TIME_IN_DAYS = 1")
        print(f"✅ Cloned {schema}.{table} → {backup_name}")
    except Exception as e:
        print(f"⚠️ Failed to clone {schema}.{table}: {e}")
    finally:
        cur.close()

# Execute cloning for each impacted table
for tbl in sorted(tables_to_clone):
    clone_table(tbl)

conn.close()
