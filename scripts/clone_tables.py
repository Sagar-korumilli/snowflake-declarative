# clone_tables.py
# ----------------
# After detecting changed SQL files under schema folders, this script:
# 1. Identifies all SQL files under each schema-level folder
# 2. Finds ALTER/TRUNCATE/DROP operations and extracts schema & table
# 3. Clones each affected table into a timestamped backup in same schema

import argparse
import os
import re
from datetime import datetime
from git import Repo
import snowflake.connector

parser = argparse.ArgumentParser()
parser.add_argument('--snowflake-root', required=True,
                    help='Path to top-level snowflake folder containing schema subfolders')
args = parser.parse_args()

# Load environment variables (must match existing names)
SNOWFLAKE_ACCOUNT               = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER                  = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE                  = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE             = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE              = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA                = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_PRIVATE_KEY           = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE= os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

# Validate env
for var in [
    'SNOWFLAKE_ACCOUNT','SNOWFLAKE_USER','SNOWFLAKE_ROLE',
    'SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE','SNOWFLAKE_SCHEMA',
    'SNOWFLAKE_PRIVATE_KEY','SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
]:
    if not os.getenv(var):
        raise RuntimeError(f"❌ Missing environment variable: {var}")

# Prepare key file for auth
with open('key.pem','w') as f:
    f.write(SNOWFLAKE_PRIVATE_KEY)
os.chmod('key.pem',0o600)

# Connect to Snowflake using key-pair auth
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

# Determine changed SQL files in this commit
repo   = Repo('.')
commit = repo.head.commit
parent = commit.parents[0] if commit.parents else None

diffs = commit.diff(parent) if parent else []
changed_files = []

# Walk each schema folder under root, ignore any 'backup' folder if present
top_level = args.snowflake_root
for entry in os.listdir(top_level):
    path = os.path.join(top_level, entry)
    if not os.path.isdir(path) or entry == 'backup':
        continue
    # collect .sql diffs under this schema folder
    for diff in diffs:
        bpath = diff.b_path
        if bpath and bpath.startswith(f"{top_level}/{entry}/") and bpath.endswith('.sql'):
            # ignore changes in any backup subfolder
            rel = bpath.replace(f"{top_level}/{entry}/", '')
            if not rel.startswith('backup/'):
                changed_files.append(bpath)

if not changed_files:
    print("✅ No changed SQL files; skipping backups.")
    conn.close()
    exit(0)

print("🔍 Changed SQL files:", changed_files)

# Regex for table operations
pattern = re.compile(
    r"(?:ALTER|TRUNCATE|DROP)\s+TABLE\s+([0-9A-Za-z_]+)\.([0-9A-Za-z_]+)",
    re.IGNORECASE
)

# Collect unique tables to clone
tables_to_clone = set()
for file_path in changed_files:
    content = open(file_path,'r').read()
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

# Execute clones
for tbl in tables_to_clone:
    clone_table(tbl)

conn.close()
