import argparse
import os
import re
import sys
import tempfile
from pathlib import Path
import snowflake.connector

def get_snowflake_connection():
    # Env vars -- do not rename for CI/CD/Secrets store compatibility
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_PRIVATE_KEY = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

    # Check all required env variables
    for var in [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]:
        if not locals().get(var):
            raise RuntimeError(f"❌ Missing environment variable: {var}")

    # Write temp PEM key file securely
    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".pem") as key_file:
        key_file.write(SNOWFLAKE_PRIVATE_KEY)
        key_path = key_file.name
    os.chmod(key_path, 0o600)

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        private_key_file=key_path,
        private_key_file_pwd=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
        authenticator='snowflake_jwt'
    )
    return conn, key_path

def get_current_ddl(conn, object_type: str, full_name: str) -> str:
    with conn.cursor() as cur:
        cur.execute(f"SELECT GET_DDL('{object_type}','{full_name}',TRUE)")
        return cur.fetchone()[0]

def replace_object(sql: str, object_type: str, full_name: str, new_ddl: str) -> str:
    pattern = rf"(?i)(CREATE\s+(?:OR\s+REPLACE\s+)?{object_type}\s+{re.escape(full_name)}\s+.*?;)"
    return re.sub(pattern, new_ddl.strip() + ";", sql, flags=re.DOTALL)

def update_setup_with_changes(schema_path: Path, changed_file: Path, conn):
    # Always backs up, never overwrites real V001
    setup_file = next(schema_path.glob("V001__*.sql"))
    original_sql = setup_file.read_text()
    changed_sql = changed_file.read_text()
    updated_sql = original_sql

    # Process ALTER TABLEs
    for sch, tbl in re.findall(r'ALTER\s+TABLE\s+(\w+)\.(\w+)', changed_sql, re.IGNORECASE):
        full_name = f"{sch}.{tbl}"
        ddl = get_current_ddl(conn, "TABLE", full_name)
        updated_sql = replace_object(updated_sql, "TABLE", full_name, ddl)

    # Process CREATE OR REPLACE (view, stage, sequence, file format) if desired:
    for obj_type, sch, name in re.findall(r'CREATE\s+OR\s+REPLACE\s+(VIEW|SEQUENCE|FILE FORMAT|STAGE)\s+(\w+)\.(\w+)', changed_sql, re.IGNORECASE):
        full_name = f"{sch}.{name}"
        ddl = get_current_ddl(conn, obj_type.upper(), full_name)
        updated_sql = replace_object(updated_sql, obj_type.upper(), full_name, ddl)

    # Backup directory
    backup_dir = schema_path / "backup"
    backup_dir.mkdir(exist_ok=True)
    backup_path = backup_dir / setup_file.name
    backup_path.write_text(updated_sql)
    print(f"✅ Backup created at {backup_path}")

def find_changed_sql_files(sf_root: str) -> list:
    changed = []
    for schema_dir in Path(sf_root).iterdir():
        if schema_dir.is_dir() and schema_dir.name != "rollback":
            for sql_file in sorted(schema_dir.glob("V[0-9]*__*.sql")):
                if not sql_file.name.startswith("V001__"):
                    changed.append(sql_file)
    return changed

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--snowflake-root', required=True, help="Root folder containing schema subfolders")
    args = parser.parse_args()

    sf_root = args.snowflake_root
    changed_files = find_changed_sql_files(sf_root)
    if not changed_files:
        print("✅ No changed SQL files; skipping backups.")
        return

    print(f"🔍 Changed SQL files: {[str(f) for f in changed_files]}")
    conn, key_path = get_snowflake_connection()
    try:
        for changed_file in changed_files:
            schema = changed_file.parts[1]  # e.g., snowflake/schema/file.sql
            schema_path = Path(sf_root) / schema
            update_setup_with_changes(schema_path, changed_file, conn)
    finally:
        conn.close()
        # Clean up temp key file
        os.remove(key_path)

if __name__ == "__main__":
    main()
