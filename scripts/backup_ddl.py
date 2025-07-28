import os
import re
import sys
import snowflake.connector

SCHEMA = sys.argv[1]
FULL_SETUP_FILE = sys.argv[2]
NEW_SQL_FILES = sys.argv[3:]

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    user=os.environ['SNOWFLAKE_USER'],
    private_key_file=os.environ['SNOWFLAKE_PRIVATE_KEY'],
    private_key_file_pwd=os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    role=os.environ['SNOWFLAKE_ROLE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema=SCHEMA
)
cursor = conn.cursor()

def get_object_type(sql_line):
    # Detect object type and name from SQL
    patterns = [
        (r'ALTER\s+TABLE\s+(\w+)', 'TABLE'),
        (r'CREATE\s+OR\s+REPLACE\s+VIEW\s+(\w+)', 'VIEW'),
        (r'CREATE\s+OR\s+REPLACE\s+SEQUENCE\s+(\w+)', 'SEQUENCE'),
        (r'CREATE\s+OR\s+REPLACE\s+FILE\s+FORMAT\s+(\w+)', 'FILE FORMAT'),
        (r'CREATE\s+OR\s+REPLACE\s+STAGE\s+(\w+)', 'STAGE')
    ]
    for pattern, obj_type in patterns:
        match = re.search(pattern, sql_line, re.IGNORECASE)
        if match:
            return obj_type, match.group(1)
    return None, None

def fetch_ddl(object_type, object_name):
    try:
        ddl_type = object_type if object_type != 'FILE FORMAT' else 'FILE FORMAT'
        qualified = f"{SCHEMA}.{object_name}"
        cursor.execute(f"SELECT GET_DDL('{ddl_type}', '{qualified}', TRUE)")
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"❌ Failed to get DDL for {object_type} {qualified}: {e}")
        return None

def replace_object_ddl(content, object_type, object_name, new_ddl):
    # Replace existing DDL block in content with the new DDL
    obj_regex_map = {
        'TABLE': rf'CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+{SCHEMA}\.{object_name}\s*\(.*?\);',
        'VIEW': rf'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+{SCHEMA}\.{object_name}\s+AS\s+.*?;',
        'SEQUENCE': rf'CREATE\s+(OR\s+REPLACE\s+)?SEQUENCE\s+{SCHEMA}\.{object_name}.*?;',
        'FILE FORMAT': rf'CREATE\s+(OR\s+REPLACE\s+)?FILE\s+FORMAT\s+{SCHEMA}\.{object_name}.*?;',
        'STAGE': rf'CREATE\s+(OR\s+REPLACE\s+)?STAGE\s+{SCHEMA}\.{object_name}.*?;'
    }

    pattern = re.compile(obj_regex_map[object_type], re.IGNORECASE | re.DOTALL)
    new_ddl = new_ddl.strip()
    updated, count = pattern.subn(new_ddl, content)

    if count:
        print(f"✅ Updated {object_type} {object_name}")
    else:
        print(f"⚠️ Could not find {object_type} {object_name} in setup file to replace")

    return updated

def ensure_backup_dir(path):
    backup_dir = os.path.join(os.path.dirname(path), 'backup')
    os.makedirs(backup_dir, exist_ok=True)
    return os.path.join(backup_dir, os.path.basename(path))

if not os.path.exists(FULL_SETUP_FILE):
    print(f"❌ Full setup file does not exist: {FULL_SETUP_FILE}")
    sys.exit(1)

with open(FULL_SETUP_FILE, 'r') as f:
    original_content = f.read()

new_content = original_content

for sql_file in NEW_SQL_FILES:
    if not os.path.exists(sql_file):
        continue

    print(f"📸 Taking DDL snapshot for schema '{SCHEMA}' from ALTER statements in {os.path.basename(sql_file)}...")

    with open(sql_file, 'r') as f:
        for line in f:
            object_type, object_name = get_object_type(line)
            if not object_type or not object_name:
                continue
            ddl = fetch_ddl(object_type, object_name)
            if ddl:
                new_content = replace_object_ddl(new_content, object_type, object_name, ddl)

# Write to backup file
backup_path = ensure_backup_dir(FULL_SETUP_FILE)
with open(backup_path, 'w') as f:
    f.write(new_content)

print(f"🗂️  Backup created at: {backup_path}")
print("✅ Setup file updated:", os.path.basename(FULL_SETUP_FILE))

cursor.close()
conn.close()
