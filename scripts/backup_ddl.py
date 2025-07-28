import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path
import snowflake.connector
from git import Repo

# Argument for the snowflake root directory
parser = argparse.ArgumentParser()
parser.add_argument('--snowflake-root', required=True, help="Root folder containing schema subfolders")
args = parser.parse_args()

# Required environment variables
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_PRIVATE_KEY = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

for var in [
    'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE',
    'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE',
    'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']:
    if not globals().get(var):
        raise RuntimeError(f"❌ Missing environment variable: {var}")

# Detect changed SQL files using Git
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

# Write private key to a file
with open('key.pem', 'w') as f:
    f.write(SNOWFLAKE_PRIVATE_KEY)
os.chmod('key.pem', 0o600)

# Connect to Snowflake
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

cur = conn.cursor()

def get_current_ddl(object_type, full_name):
    try:
        cur.execute(f"SELECT GET_DDL('{object_type}', '{full_name}', true)")
        return cur.fetchone()[0]
    except Exception as e:
        print(f"⚠️ Failed to fetch DDL for {object_type} {full_name}: {e}")
        return None

def replace_object(original_sql, object_type, object_name, new_ddl):
    pattern = re.compile(
        rf"CREATE(?:\s+OR\s+REPLACE)?\s+{object_type}\s+{re.escape(object_name)}.*?;",
        re.IGNORECASE | re.DOTALL
    )
    return pattern.sub(new_ddl.strip() + ';', original_sql)

# Process each changed file
for file in changed_files:
    sql = Path(file).read_text()
    schema = Path(file).parts[1]
    backup_dir = Path(file).parent / 'backup'
    backup_dir.mkdir(exist_ok=True)
    backup_file = backup_dir / Path(file).name

    modified_sql = sql

    # Handle ALTER TABLEs (replace table definition)
    table_alters = re.findall(r'ALTER TABLE (\w+)\.(\w+)', sql, re.IGNORECASE)
    for sch, tbl in table_alters:
        full = f"{sch}.{tbl}"
        ddl = get_current_ddl("TABLE", full)
        if ddl:
            modified_sql = replace_object(modified_sql, "TABLE", full, ddl)

    # Handle CREATE OR REPLACE for non-tables (replace object definition)
    creates = re.findall(
        r'CREATE\s+OR\s+REPLACE\s+(VIEW|SEQUENCE|FILE FORMAT|STAGE)\s+(\w+)\.(\w+)',
        sql, re.IGNORECASE
    )
    for obj_type, sch, name in creates:
        full = f"{sch}.{name}"
        ddl = get_current_ddl(obj_type.upper(), full)
        if ddl:
            modified_sql = replace_object(modified_sql, obj_type.upper(), full, ddl)

    # Save updated SQL to backup file
    backup_file.write_text(modified_sql)
    print(f"✅ Backup created at {backup_file}")
    print(f"📄 Content of {backup_file}:\n{'-'*60}\n{modified_sql}\n{'-'*60}\n")

# Cleanup
cur.close()
conn.close()
