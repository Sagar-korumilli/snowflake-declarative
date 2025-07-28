import os
import sys
import re
from pathlib import Path
from datetime import datetime
import snowflake.connector

# === ENV VARS ===
SNOWFLAKE_ACCOUNT              = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER                 = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE                 = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE            = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE             = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_PRIVATE_KEY          = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

if len(sys.argv) < 3:
    print("Usage: python backup_ddl.py <schema_name> <setup_file.sql>")
    sys.exit(1)

schema_name = sys.argv[1]
setup_file = sys.argv[2]

if not Path(setup_file).exists():
    print(f"❌ Setup file not found: {setup_file}")
    sys.exit(1)

print(f"🛠️  Processing schema: {schema_name}")
print(f"📄 Setup file: {setup_file}")

# Write private key to file
key_path = "key.pem"
with open(key_path, "w") as key_file:
    key_file.write(SNOWFLAKE_PRIVATE_KEY)
os.chmod(key_path, 0o600)

# === CONNECT TO SNOWFLAKE ===
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    authenticator='snowflake_jwt',
    private_key_file=key_path,
    private_key_file_pwd=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
)
cursor = conn.cursor()

def get_current_ddl(object_type, object_name):
    try:
        cursor.execute(f"SHOW {object_type}S IN SCHEMA {schema_name}")
        rows = cursor.fetchall()
        for row in rows:
            if row[1].upper() == object_name.upper():
                cursor.execute(f"SELECT GET_DDL('{object_type}', '{schema_name}.{object_name}', true)")
                return cursor.fetchone()[0]
    except Exception as e:
        print(f"⚠️ Failed to get DDL for {object_type} {object_name}: {e}")
    return None

def find_altered_objects(sql_text):
    pattern = re.compile(r"ALTER\s+(TABLE|VIEW|SEQUENCE|FILE\s+FORMAT|STAGE)\s+(\w+)\.(\w+)", re.IGNORECASE)
    return pattern.findall(sql_text)

with open(setup_file, 'r') as f:
    setup_contents = f.read()

schema_folder = str(Path(setup_file).parent)
sql_files = sorted(Path(schema_folder).glob("*.sql"))
sql_files = [f for f in sql_files if f.name != Path(setup_file).name]

all_alters = []
for sql_file in sql_files:
    print(f"🔍 Checking SQL file: {sql_file.name}")
    with open(sql_file) as f:
        sql = f.read()
    alters = find_altered_objects(sql)
    if alters:
        print(f"📸 Taking DDL snapshot for schema '{schema_name}' from ALTER statements in {sql_file.name}...")
        all_alters.extend(alters)

if not all_alters:
    print("✅ No new SQL files; skipping backup.")
    conn.close()
    sys.exit(0)

# Create backup folder and save backup
backup_folder = Path(schema_folder) / "backup"
backup_folder.mkdir(exist_ok=True)
backup_path = backup_folder / Path(setup_file).name
with open(backup_path, "w") as f:
    f.write(setup_contents)
print(f"🗂️  Backup created at: {backup_path}")

# Update setup file with current DDL
modified = False
for obj_type_raw, schema, obj_name in all_alters:
    obj_type = obj_type_raw.upper().replace(" ", "_")
    ddl = get_current_ddl(obj_type_raw.upper(), obj_name)
    if not ddl:
        print(f"⚠️ Could not update {obj_type} {obj_name}, not found in Snowflake.")
        continue

    pattern = re.compile(
        rf"CREATE\s+OR\s+REPLACE\s+{re.escape(obj_type_raw)}\s+{re.escape(schema)}\.{re.escape(obj_name)}.*?;",
        re.IGNORECASE | re.DOTALL
    )
    if pattern.search(setup_contents):
        setup_contents = pattern.sub(ddl.strip() + ";", setup_contents)
        print(f"✅ Updated {obj_type} {obj_name}")
        modified = True
    else:
        print(f"⚠️ Could not update {obj_type} {obj_name}, not found in setup file.")

if modified:
    with open(setup_file, "w") as f:
        f.write(setup_contents)
    print(f"✅ Setup file updated: {setup_file}")
else:
    print("ℹ️  No matching objects found in setup file; no changes made.")

cursor.close()
conn.close()
