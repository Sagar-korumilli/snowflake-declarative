import os
import sys
import snowflake.connector
from pathlib import Path

# Use your original environment variable names
SNOWFLAKE_ACCOUNT    = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER       = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE       = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE  = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE   = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_PRIVATE_KEY           = os.getenv('SNOWFLAKE_PRIVATE_KEY')
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE= os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_PRIVATE_KEY]):
    print("❌ Missing required Snowflake environment variables.")
    sys.exit(1)

import base64
import tempfile
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_private_key():
    key_bytes = SNOWFLAKE_PRIVATE_KEY.encode()
    private_key = serialization.load_pem_private_key(
        key_bytes,
        password=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.encode() if SNOWFLAKE_PRIVATE_KEY_PASSPHRASE else None,
        backend=default_backend()
    )
    return private_key

def get_connection():
    private_key = load_private_key()
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        private_key=pkb,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE
    )

def fetch_ddl(conn, schema_name):
    ddl_map = {}
    query = f"""
    SELECT OBJECT_NAME, OBJECT_TYPE
    FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.OBJECTS
    WHERE OBJECT_SCHEMA = '{schema_name}'
      AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'SEQUENCE')
    ORDER BY OBJECT_NAME
    """
    cur = conn.cursor()
    cur.execute(f'USE SCHEMA {SNOWFLAKE_DATABASE}.{schema_name}')
    cur.execute(query)
    for obj_name, obj_type in cur.fetchall():
        ddl_cur = conn.cursor()
        ddl_cur.execute(f"SHOW {obj_type}s LIKE '{obj_name}' IN SCHEMA {SNOWFLAKE_DATABASE}.{schema_name}")
        show_result = ddl_cur.fetchone()
        if show_result:
            ddl = show_result[-1]
            ddl_map[(obj_type, obj_name)] = ddl
    return ddl_map

def write_backup(schema: str, original_file: str, ddl_map: dict):
    orig_path = Path(original_file)
    backup_dir = orig_path.parent / 'backup'
    backup_dir.mkdir(exist_ok=True)
    backup_file = backup_dir / orig_path.name

    with open(backup_file, 'w') as f:
        for (obj_type, obj_name), ddl in sorted(ddl_map.items()):
            f.write(f"-- {obj_type}: {obj_name}\n{ddl};\n\n")
    print(f"✅ DDL snapshot written to: {backup_file}")

def main():
    if len(sys.argv) != 3:
        print("Usage: python backup_ddl.py <schema_name> <initial_file.sql>")
        sys.exit(1)

    schema = sys.argv[1]
    initial_file = sys.argv[2]

    if not Path(initial_file).exists():
        print(f"❌ Initial file not found: {initial_file}")
        sys.exit(1)

    print(f"📸 Taking DDL snapshot of schema: {schema}")
    conn = get_connection()
    ddl_map = fetch_ddl(conn, schema)
    write_backup(schema, initial_file, ddl_map)

if __name__ == '__main__':
    main()
