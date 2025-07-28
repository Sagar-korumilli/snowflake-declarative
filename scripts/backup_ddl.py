import os
import sys
import re
import snowflake.connector
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load Snowflake credentials from environment variables
SNOWFLAKE_ACCOUNT     = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER        = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE        = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE   = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE    = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_PRIVATE_KEY = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

# Validate environment
if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE,
            SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_PRIVATE_KEY]):
    print("❌ Missing required Snowflake environment variables.")
    sys.exit(1)

def load_private_key():
    key_bytes = SNOWFLAKE_PRIVATE_KEY.encode()
    return serialization.load_pem_private_key(
        key_bytes,
        password=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.encode() if SNOWFLAKE_PRIVATE_KEY_PASSPHRASE else None,
        backend=default_backend()
    )

def get_connection():
    pk = load_private_key().private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        private_key=pk,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE
    )

def parse_altered_objects(file_path):
    altered = []
    with open(file_path, 'r') as f:
        for line in f:
            match = re.match(r'ALTER\s+(TABLE|VIEW|SEQUENCE)\s+(?:IF EXISTS\s+)?(?:[\w]+\.)?([\w]+)', line.strip(), re.IGNORECASE)
            if match:
                altered.append((match.group(1).upper(), match.group(2).upper()))
    return altered

def fetch_object_ddl(conn, schema, obj_type, obj_name):
    cursor = conn.cursor()
    obj_type_upper = obj_type.upper()
    cursor.execute(f"SHOW {obj_type_upper}s LIKE '{obj_name}' IN SCHEMA {SNOWFLAKE_DATABASE}.{schema}")
    row = cursor.fetchone()
    return row[-1] if row else None

def update_setup_file(setup_file, altered_ddls):
    with open(setup_file, 'r') as f:
        content = f.read()

    for (obj_type, obj_name), new_ddl in altered_ddls.items():
        # Pattern to find the old definition
        pattern = rf'--\s*{obj_type}:\s*{obj_name}\s*\n.*?(?=\n--|$)'
        new_block = f"-- {obj_type}: {obj_name}\n{new_ddl};"
        content, count = re.subn(pattern, new_block, content, flags=re.DOTALL | re.IGNORECASE)
        if count == 0:
            print(f"⚠️ Could not update {obj_type} {obj_name}, not found in setup file.")

    with open(setup_file, 'w') as f:
        f.write(content)

def backup_file(setup_file):
    backup_dir = Path(setup_file).parent / 'backup'
    backup_dir.mkdir(exist_ok=True)
    backup_path = backup_dir / Path(setup_file).name
    with open(setup_file, 'r') as src, open(backup_path, 'w') as dst:
        dst.write(src.read())
    print(f"🗂️  Backup created at: {backup_path}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python backup_ddl.py <schema_name> <file1.sql> [<file2.sql> ...]")
        sys.exit(1)

    schema = sys.argv[1]
    sql_files = sys.argv[2:]
    setup_file = next((f for f in sql_files if Path(f).name.startswith("V001__")), None)
    if not setup_file or not Path(setup_file).exists():
        print("❌ Full setup file (V001__*.sql) not found.")
        sys.exit(1)

    other_files = [f for f in sql_files if f != setup_file]
    if not other_files:
        print("✅ No new SQL files; skipping backup.")
        sys.exit(0)

    altered_objects = {}
    for file in other_files:
        altered_objects.update({obj: None for obj in parse_altered_objects(file)})

    if not altered_objects:
        print("✅ No ALTER statements found; skipping setup update.")
        sys.exit(0)

    print(f"📸 Taking DDL snapshot for schema '{schema}' from ALTER statements...")

    conn = get_connection()
    conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_DATABASE}.{schema}")

    altered_ddls = {}
    for obj_type, obj_name in altered_objects:
        ddl = fetch_object_ddl(conn, schema, obj_type, obj_name)
        if ddl:
            altered_ddls[(obj_type, obj_name)] = ddl
        else:
            print(f"⚠️ DDL not found for {obj_type} {obj_name}.")

    if altered_ddls:
        backup_file(setup_file)
        update_setup_file(setup_file, altered_ddls)
        print(f"✅ Setup file updated: {setup_file}")
    else:
        print("⚠️ No valid DDL fetched; setup not updated.")

if __name__ == '__main__':
    main()
