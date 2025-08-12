import os
import re
import tempfile
import snowflake.connector
from datetime import datetime
from pathlib import Path

# = Snowflake Connection with Private Key =
def get_snowflake_connection():
    private_key_content = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    
    if not private_key_content:
        raise ValueError("Missing SNOWFLAKE_PRIVATE_KEY environment variable")
    
    # Write private key to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pem", mode="w") as key_file:
        key_file.write(private_key_content)
        key_file_path = key_file.name

    try:
        # Private key authentication -- do NOT provide password
        connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            private_key_file=key_file_path,
            private_key_file_pwd=private_key_passphrase,
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            # No password argument here!
        )
    finally:
        # Clean up private key file after connect (recommended security)
        if os.path.exists(key_file_path):
            os.remove(key_file_path)
    return connection

# = Get Changed SQL Files from Git =
def get_changed_sql_files():
    base_ref = os.getenv("GITHUB_BASE_REF")
    head_ref = os.getenv("GITHUB_HEAD_REF")
    
    # Fallback: all SQL files if refs not specified
    if not base_ref or not head_ref:
        print("[WARN] Missing Git refs — defaulting to all .sql files in repo.")
        return list(Path(".").rglob("*.sql"))
    
    import subprocess
    diff_cmd = [
        "git", "diff", "--name-only", f"origin/{base_ref}", f"origin/{head_ref}"
    ]
    changed_files = subprocess.check_output(diff_cmd).decode().splitlines()
    return [Path(f) for f in changed_files if f.endswith(".sql")]

# = Extract Object Details (type & name) from SQL =
def extract_object_details(sql_content):
    # Matches CREATE/ALTER for supported object types
    match = re.search(
        r"(CREATE|ALTER)\s+(VIEW|SEQUENCE|STAGE|FILE\s+FORMAT|PIPE|STREAM|TASK)\s+([A-Z0-9_\.]+)",
        sql_content, re.IGNORECASE
    )
    if match:
        object_type = match.group(2).upper()
        object_name = match.group(3)
        return object_type, object_name
    return None, None

# = Fetch Current DDL from Snowflake =
def fetch_current_ddl(conn, object_type, object_name):
    cur = conn.cursor()
    try:
        # SHOW <OBJECTTYPE>S LIKE '<NAME>' IN SCHEMA <schema>
        object_short_name = object_name.split('.')[-1]
        schema_name = '.'.join(object_name.split('.')[:-1])
        cur.execute(
            f"SHOW {object_type}S LIKE '{object_short_name}' IN SCHEMA {schema_name}"
        )
        result = cur.fetchone()
        if not result:
            print(f"[WARN] Object not found in Snowflake: {object_name}")
            return None
        # Get the DDL using native function
        cur.execute(f"SELECT GET_DDL('{object_type}', '{object_name}')")
        ddl = cur.fetchone()[0]
        return ddl
    finally:
        cur.close()

# = Save DDL to Rollback Directory =
def save_to_rollback(object_name, ddl):
    rollback_dir = Path("rollback")
    rollback_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = object_name.replace(".", "_")
    file_path = rollback_dir / f"{safe_name}__{timestamp}.sql"
    with open(file_path, "w") as f:
        f.write(ddl)
    print(f"[INFO] Saved rollback script: {file_path}")

# = Main Logic =
def main():
    conn = get_snowflake_connection()
    try:
        changed_files = get_changed_sql_files()
        for file_path in changed_files:
            with open(file_path, "r") as f:
                sql_content = f.read()
            object_type, object_name = extract_object_details(sql_content)
            if object_type and object_type != "TABLE":  # Only non-table objects
                ddl = fetch_current_ddl(conn, object_type, object_name)
                if ddl:
                    save_to_rollback(object_name, ddl)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
