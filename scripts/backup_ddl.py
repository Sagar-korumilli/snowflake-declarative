#!/usr/bin/env python3
import argparse
import os
import re
import sys
import tempfile
from pathlib import Path
import subprocess
import snowflake.connector

# --------------------
# Configuration
# Environment variables expected for Git:
#   GIT_USER_NAME    - e.g. "Alice Example"
#   GIT_USER_EMAIL   - e.g. "alice@example.com"
#   GIT_PUSH_TOKEN   - Personal access token for push auth
# --------------------

def get_snowflake_connection():
    required = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]
    for var in required:
        if not os.getenv(var):
            raise RuntimeError(f"❌ Missing environment variable: {var}")

    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".pem") as key_file:
        key_file.write(os.getenv('SNOWFLAKE_PRIVATE_KEY'))
        key_path = key_file.name
    os.chmod(key_path, 0o600)

    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        private_key_file=key_path,
        private_key_file_pwd=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
        authenticator='snowflake_jwt'
    )
    return conn, key_path


def get_current_ddl(conn, object_type: str, full_name: str) -> str:
    """
    Fetch the live DDL for the given object from Snowflake.
    object_type: TABLE, VIEW, etc.
    full_name: DATABASE.SCHEMA.OBJECT_NAME
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT GET_DDL('{object_type}', '{full_name}', TRUE)")
        return cur.fetchone()[0]


def configure_git_credentials():
    """
    Configure Git user name, email, and HTTP remote URL using a personal token.
    Requires GIT_USER_NAME, GIT_USER_EMAIL, GIT_PUSH_TOKEN.
    """
    name = os.getenv('GIT_USER_NAME')
    email = os.getenv('GIT_USER_EMAIL')
    token = os.getenv('GIT_PUSH_TOKEN')

    if not (name and email and token):
        raise RuntimeError("❌ Missing one of GIT_USER_NAME, GIT_USER_EMAIL, or GIT_PUSH_TOKEN")

    subprocess.run(["git", "config", "--local", "user.name", name], check=True)
    subprocess.run(["git", "config", "--local", "user.email", email], check=True)

    # rewrite origin URL to include the token for HTTPS authentication
    url = subprocess.check_output(
        ["git", "config", "--get", "remote.origin.url"], text=True
    ).strip()
    if url.startswith('https://'):
        auth_url = url.replace('https://', f'https://{token}@')
        subprocess.run(
            ["git", "remote", "set-url", "origin", auth_url],
            check=True
        )
        print("🔑 Git remote 'origin' updated with token auth.")


def git_add_commit_push(file_path: Path, message: str):
    configure_git_credentials()
    try:
        subprocess.run(["git", "add", str(file_path)], check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"✅ Pushed updated DDL for {file_path.name}")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git error: {e}")


def find_changed_sql_files(sf_root: str) -> list[Path]:
    """
    Return all object-level SQL files that contain ALTER TABLE/VIEW statements.
    """
    altered = []
    for schema_dir in Path(sf_root).iterdir():
        if not schema_dir.is_dir() or schema_dir.name.lower() == 'rollback':
            continue
        for f in schema_dir.glob("*.sql"):
            text = f.read_text()
            if re.search(r'ALTER\s+(TABLE|VIEW)\s+\w+\.\w+', text, re.IGNORECASE):
                altered.append(f)
    return altered


def update_object_file(schema_path: Path, changed_file: Path, conn):
    sql = changed_file.read_text()
    alters = re.findall(r'ALTER\s+(TABLE|VIEW)\s+(\w+)\.(\w+)', sql, re.IGNORECASE)
    if not alters:
        print(f"ℹ️ No ALTER statements found in {changed_file.name}")
        return

    db = os.getenv('SNOWFLAKE_DATABASE')
    for obj_type, sch, tbl in alters:
        # include database in full name
        full_name = f"{db}.{sch}.{tbl}"
        ddl = get_current_ddl(conn, obj_type.upper(), full_name).strip() + "\n"

        # match object-level filename convention
        pattern = f"*__{tbl.lower()}*.sql"
        candidates = list(schema_path.glob(pattern))
        if not candidates:
            print(f"⚠️ No file matching {pattern} in {schema_path}")
            continue

        target = candidates[0]
        target.write_text(ddl)
        msg = f"chore: refresh {obj_type.lower()} DDL for {full_name}"
        git_add_commit_push(target, msg)


def main():
    parser = argparse.ArgumentParser(
        description="Refresh object-level DDL in Git from Snowflake"
    )
    parser.add_argument(
        '--snowflake-root', required=True,
        help="Root dir containing schema subfolders"
    )
    args = parser.parse_args()

    changed_files = find_changed_sql_files(args.snowflake_root)
    if not changed_files:
        print("✅ No ALTER scripts detected; exiting.")
        sys.exit(0)

    print("🔍 Detected ALTER files:", [str(f) for f in changed_files])
    conn, key_path = get_snowflake_connection()
    try:
        for ch in changed_files:
            update_object_file(ch.parent, ch, conn)
    finally:
        conn.close()
        os.remove(key_path)
        print("✔️ Done.")

if __name__ == "__main__":
    main()
