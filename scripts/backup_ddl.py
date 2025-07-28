import argparse
import os
import re
import sys
import tempfile
from pathlib import Path
import subprocess
import snowflake.connector

def get_snowflake_connection():
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
        'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]:
        if not locals().get(var):
            raise RuntimeError(f"❌ Missing environment variable: {var}")
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
        cur.execute(f"SELECT GET_DDL('{object_type}', '{full_name}', TRUE)")
        return cur.fetchone()[0]

def replace_object(sql: str, object_type: str, full_name: str, new_ddl: str) -> str:
    pattern = rf"(?i)(CREATE\s+(?:OR\s+REPLACE\s+)?{object_type}\s+{re.escape(full_name)}\s+.*?;)"
    return re.sub(pattern, new_ddl.strip() + ";", sql, flags=re.DOTALL)

def git_add_commit_push(file_path, message):
    file_path = str(file_path)
    try:
        subprocess.run(["git", "add", file_path], check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)
        print(f"✅ Git commit created for {file_path}")
        subprocess.run(["git", "push"], check=True)
        print("✅ Git push completed")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git command failed: {e}")
        # Optionally, you can exit or continue.

def update_setup_with_changes(schema_path: Path, changed_file: Path, conn):
    setup_file = next(schema_path.glob("V001__*.sql"))
    original_sql = setup_file.read_text()
    changed_sql = changed_file.read_text()
    updated_sql = original_sql

    for sch, tbl in re.findall(r'ALTER\s+TABLE\s+(\w+)\.(\w+)', changed_sql, re.IGNORECASE):
        full_name = f"{sch}.{tbl}"
        ddl = get_current_ddl(conn, "TABLE", full_name)
        updated_sql = replace_object(updated_sql, "TABLE", full_name, ddl)

    # Create backup directory if missing
    backup_dir = schema_path / "backup"
    backup_dir.mkdir(exist_ok=True)
    backup_path = backup_dir / setup_file.name
    backup_path.write_text(updated_sql)
    print(f"✅ Backup created at {backup_path}")
    print(f"\n📄 Backup file content ({backup_path}):\n{'-'*60}")
    print(backup_path.read_text())
    print('-' * 60)

    # Automatically git add, commit, and push after backup file is created
    git_message = f"Update DDL backup for {schema_path.name} after {changed_file.name}"
    git_add_commit_push(backup_path, git_message)

def find_changed_sql_files(sf_root: str) -> list:
    changed = []
    for schema_dir in Path(sf_root).iterdir():
        if schema_dir.is_dir() and schema_dir.name != "rollback":
            sql_files = sorted(schema_dir.glob("V[0-9]*__*.sql"))
            latest = None
            latest_version = -1
            for sql_file in sql_files:
                if sql_file.name.startswith("V001__"):
                    continue
                match = re.match(r'V(\d+)', sql_file.name)
                if match:
                    version = int(match.group(1))
                    if version > latest_version:
                        latest = sql_file
                        latest_version = version
            if latest:
                changed.append(latest)
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

    print(f"🔍 Latest changed SQL files: {[str(f) for f in changed_files]}")
    conn, key_path = get_snowflake_connection()
    try:
        for changed_file in changed_files:
            schema = changed_file.parts[1]
            schema_path = Path(sf_root) / schema
            update_setup_with_changes(schema_path, changed_file, conn)
    finally:
        conn.close()
        os.remove(key_path)

if __name__ == "__main__":
    main()
