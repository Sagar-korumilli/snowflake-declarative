import os
import re
import sys
import shutil
from pathlib import Path
from snowflake.connector import connect

# --- Snowflake credentials from environment variables (user-defined names) ---
SNOWFLAKE_ACCOUNT     = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER        = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE        = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE   = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE    = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_PRIVATE_KEY = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE,
            SNOWFLAKE_DATABASE, SNOWFLAKE_PRIVATE_KEY, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE]):
    sys.exit("❌ Missing required Snowflake credentials.")

# --- Connect to Snowflake ---
def get_connection():
    return connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        private_key=SNOWFLAKE_PRIVATE_KEY.encode(),
        private_key_passphrase=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE
    )

# --- Read current setup DDL ---
def load_setup_file(setup_path):
    if not setup_path.exists():
        print(f"⚠️ Setup file not found: {setup_path}")
        return None
    return setup_path.read_text()

# --- Extract all object names from ALTER SQL files ---
def extract_object_names(sql_path):
    with open(sql_path) as f:
        content = f.read()
    matches = re.findall(r'ALTER\s+(TABLE|VIEW|SEQUENCE|FILE FORMAT)\s+(\w+)\.(\w+)', content, re.IGNORECASE)
    return set((m[0].upper(), m[1].upper(), m[2].upper()) for m in matches)

# --- Get DDL from Snowflake ---
def fetch_current_ddl(conn, obj_type, schema, name):
    full_name = f"{schema}.{name}"
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW {obj_type}s IN SCHEMA {schema}")
        for row in cur:
            if row[1].upper() == name.upper():
                cur.execute(f"SELECT GET_DDL('{obj_type}', '{full_name}', TRUE)")
                return cur.fetchone()[0]
    finally:
        cur.close()
    return None

# --- Replace object DDL in setup file ---
def replace_ddl(original_text, ddl):
    obj_name = re.search(r'CREATE\s+OR\s+REPLACE\s+(TABLE|VIEW|SEQUENCE|FILE FORMAT)\s+(\w+\.\w+)', ddl, re.IGNORECASE)
    if not obj_name:
        return original_text, False
    pattern = re.compile(
        rf'(CREATE\s+OR\s+REPLACE\s+{obj_name.group(1)}\s+{obj_name.group(2)}.*?;)', re.DOTALL | re.IGNORECASE
    )
    if pattern.search(original_text):
        updated_text = pattern.sub(ddl.strip() + ';', original_text)
        return updated_text, True
    return original_text, False

# --- Main ---
if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.exit("Usage: python backup_ddl.py <schema_name> <setup_file>")

    schema = sys.argv[1].upper()
    setup_file = Path(sys.argv[2])
    folder = setup_file.parent
    backup_dir = folder / "backup"
    backup_dir.mkdir(exist_ok=True)

    ddl_before = load_setup_file(setup_file)
    if ddl_before is None:
        sys.exit(1)

    conn = get_connection()
    altered_objects = set()
    for sql_file in sorted(folder.glob("V[1-9]*__*.sql")):
        altered_objects |= extract_object_names(sql_file)

    print(f"📸 Taking DDL snapshot for schema '{schema}' from ALTER statements...")
    updated_ddl = ddl_before
    modified = False

    for obj_type, obj_schema, obj_name in altered_objects:
        ddl = fetch_current_ddl(conn, obj_type, obj_schema, obj_name)
        if ddl:
            updated_ddl, changed = replace_ddl(updated_ddl, ddl)
            if changed:
                modified = True
            else:
                print(f"⚠️ Could not update {obj_type} {obj_name}, not found in setup file.")
        else:
            print(f"⚠️ Could not fetch DDL for {obj_type} {obj_name}")

    if modified:
        backup_path = backup_dir / setup_file.name
        shutil.copy2(setup_file, backup_path)
        print(f"🗂️  Backup created at: {backup_path}")
        setup_file.write_text(updated_ddl)
        print(f"✅ Setup file updated: {setup_file}")
    else:
        print("✅ No new SQL files; skipping backup.")
