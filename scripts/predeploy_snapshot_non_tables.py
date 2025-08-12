import os
import re
import tempfile
import snowflake.connector
from datetime import datetime
from pathlib import Path

# === Snowflake Connection ===
def get_snowflake_connection():
    private_key_content = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    if not private_key_content:
        raise ValueError("Missing SNOWFLAKE_PRIVATE_KEY environment variable")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pem", mode="w") as key_file:
        key_file.write(private_key_content)
        key_file_path = key_file.name

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key_file=key_file_path,
        private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )

# === Get Changed SQL Files from GitHub Actions Env ===
def get_changed_sql_files():
    base_ref = os.getenv("GITHUB_BASE_REF")
    head_ref = os.getenv("GITHUB_HEAD_REF")
    repo = os.getenv("GITHUB_REPOSITORY")

    # Fallback: all SQL files if commits not provided
    if not base_ref or not head_ref:
        print("[WARN] Missing commit SHAs — defaulting to all .sql files in repo.")
        return list(Path(".").rglob("*.sql"))

    import subprocess
    diff_cmd = [
        "git", "diff", "--name-only", f"origin/{base_ref}", f"origin/{head_ref}"
    ]
    changed_files = subprocess.check_output(diff_cmd).decode().splitlines()
    return [Path(f) for f in changed_files if f.endswith(".sql")]

# === Extract Schema and Object Type from File ===
def extract_object_details(sql_content):
    # Matches CREATE/ALTER VIEW/SEQUENCE/STAGE/FILE FORMAT/PIPE/STREAM/TASK
    match = re.search(r"(CREATE|ALTER)\s+(VIEW|SEQUENCE|STAGE|FILE\s+FORMAT|PIPE|STREAM|TASK)\s+([A-Z0-9_\.]+)", sql_content, re.IGNORECASE)
    if match:
        return match.group(2).upper(), match.group(3)
    return None, None

# === Fetch Current DDL from Snowflake ===
def fetch_current_ddl(conn, object_type, object_name):
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW {object_type}S LIKE '{object_name.split('.')[-1]}' IN SCHEMA {'.'.join(object_name.split('.')[:-1])}")
        result = cur.fetchone()
        if not result:
            print(f"[WARN] Object not found in Snowflake: {object_name}")
            return None

        cur.execute(f"SELECT GET_DDL('{object_type}', '{object_name}')")
        ddl = cur.fetchone()[0]
        return ddl
    finally:
        cur.close()

# === Save DDL to Rollback Folder ===
def save_to_rollback(object_name, ddl):
    rollback_dir = Path("rollback")
    rollback_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = object_name.replace(".", "_")
    file_path = rollback_dir / f"{safe_name}__{timestamp}.sql"

    with open(file_path, "w") as f:
        f.write(ddl)

    print(f"[INFO] Saved rollback script: {file_path}")

# === Main Logic ===
def main():
    conn = get_snowflake_connection()
    changed_files = get_changed_sql_files()

    for file_path in changed_files:
        with open(file_path, "r") as f:
            sql_content = f.read()

        object_type, object_name = extract_object_details(sql_content)
        if object_type and object_type != "TABLE":  # Only non-table objects
            ddl = fetch_current_ddl(conn, object_type, object_name)
            if ddl:
                save_to_rollback(object_name, ddl)

    conn.close()

if __name__ == "__main__":
    main()
