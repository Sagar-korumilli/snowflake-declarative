import argparse
import os
import re
import sys
import tempfile
from pathlib import Path
import subprocess
import snowflake.connector

def get_snowflake_connection():
    # Environment variables
    required = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]
    for var in required:
        if not os.getenv(var):
            raise RuntimeError(f"❌ Missing environment variable: {var}")

    # Write private key to temp file
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
    Fetch the live DDL for the given object.
    object_type: TABLE, VIEW, etc.
    full_name: SCHEMA.OBJECT_NAME
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT GET_DDL('{object_type}', '{full_name}', TRUE)")
        return cur.fetchone()[0]


def git_add_commit_push(file_path: Path, message: str):
    # set local git identity to avoid errors
    subprocess.run(["git", "config", "--local", "user.email", "ci-bot@example.com"], check=True)
    subprocess.run(["git", "config", "--local", "user.name", "CI Bot"], check=True)
    try:
        subprocess.run(["git", "add", str(file_path)], check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"✅ Updated and pushed {file_path.name}")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git error: {e}")


def find_changed_sql_files(sf_root: str) -> list[Path]:
    """
    Return all ALTER scripts under each schema (e.g., files containing ALTER TABLE).
    """
    changed = []
    for schema_dir in Path(sf_root).iterdir():
        if not schema_dir.is_dir() or schema_dir.name == 'rollback':
            continue
        # find any sql containing ALTER TABLE or ALTER VIEW
        for f in schema_dir.glob('*.sql'):
            text = f.read_text()
            if re.search(r'ALTER\s+(?:TABLE|VIEW)\s+', text, re.IGNORECASE):
                changed.append(f)
    return changed


def update_object_file(schema_path: Path, changed_file: Path, conn):
    """
    For each ALTER statement in changed_file, fetch latest DDL and overwrite the object's file.
    """
    sql = changed_file.read_text()
    alters = re.findall(r'ALTER\s+(TABLE|VIEW)\s+(\w+)\.(\w+)', sql, re.IGNORECASE)
    if not alters:
        print(f"ℹ️ No ALTERs found in {changed_file.name}")
        return

    for obj_type, sch, tbl in alters:
        full_name = f"{sch}.{tbl}"
        ddl = get_current_ddl(conn, obj_type.upper(), full_name)

        # find the corresponding object-level file by name
        pattern = f"*__{tbl.lower()}*.sql"
        candidates = list(schema_path.glob(pattern))
        if not candidates:
            print(f"⚠️ No file matching {pattern} in {schema_path}")
            continue
        # pick first match
        target = candidates[0]

        # overwrite with fetched DDL (plus semicolon)
        target.write_text(ddl.strip() + '\n')
        msg = f"chore: refresh {obj_type.lower()} DDL for {sch}.{tbl}"
        git_add_commit_push(target, msg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--snowflake-root', required=True)
    args = parser.parse_args()

    changed_files = find_changed_sql_files(args.snowflake_root)
    if not changed_files:
        print("✅ No ALTER scripts detected; exiting.")
        sys.exit(0)

    conn, key_path = get_snowflake_connection()
    try:
        for ch in changed_files:
            schema = ch.parent.stem
            update_object_file(Path(args.snowflake_root)/schema, ch, conn)
    finally:
        conn.close()
        os.remove(key_path)
        print("✔️ Done.")

if __name__ == '__main__':
    main()
