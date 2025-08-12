import os
import re
import json
import subprocess
from pathlib import Path
import snowflake.connector
import argparse
import sys
import tempfile
import base64
import requests
import time
from datetime import datetime, timezone


# -----------------------------------------------------------
# 1. Connect to Snowflake using private key auth
# -----------------------------------------------------------
def get_snowflake_connection():
    creds = {k:os.getenv(k) for k in ['SNOWFLAKE_USER','SNOWFLAKE_ACCOUNT','SNOWFLAKE_ROLE','SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE','SNOWFLAKE_PRIVATE_KEY','SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']}
    if not all(creds.values()):
        missing=[k for k,v in creds.items() if not v]
        raise EnvironmentError(f"Missing Snowflake vars: {missing}")
    with tempfile.NamedTemporaryFile('w+',delete=False,suffix='.pem') as f:
        f.write(creds['SNOWFLAKE_PRIVATE_KEY'])
        keypath=f.name
    return snowflake.connector.connect(
        user=creds['SNOWFLAKE_USER'], account=creds['SNOWFLAKE_ACCOUNT'], role=creds['SNOWFLAKE_ROLE'],
        warehouse=creds['SNOWFLAKE_WAREHOUSE'], database=creds['SNOWFLAKE_DATABASE'],
        private_key_file=keypath, private_key_file_pwd=creds['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
    )


# -----------------------------------------------------------
# 2. Get list of changed SQL files in this PR / push
# -----------------------------------------------------------
def get_changed_sql_files():
    changed_files = []

    event_name = os.getenv("GITHUB_EVENT_NAME")
    before_sha = os.getenv("GITHUB_EVENT_BEFORE")
    after_sha = os.getenv("GITHUB_SHA")

    if event_name == "pull_request":
        event_path = os.getenv("GITHUB_EVENT_PATH")
        with open(event_path, "r") as f:
            event = json.load(f)
        base_sha = event["pull_request"]["base"]["sha"]
        head_sha = event["pull_request"]["head"]["sha"]
    else:
        base_sha = before_sha
        head_sha = after_sha

    if not base_sha or not head_sha:
        print("[WARN] Missing commit SHAs — defaulting to all .sql files in repo.")
        return [str(p) for p in Path(".").rglob("*.sql")]

    diff_cmd = ["git", "diff", "--name-only", base_sha, head_sha]
    result = subprocess.run(diff_cmd, capture_output=True, text=True)
    for file_path in result.stdout.splitlines():
        if file_path.lower().endswith(".sql") and Path(file_path).exists():
            changed_files.append(file_path)

    return changed_files


# -----------------------------------------------------------
# 3. Extract object type, schema, and name from SQL
# -----------------------------------------------------------
def parse_sql_metadata(sql_text):
    # Regex excludes TABLE entirely
    regex = r"\b(CREATE|ALTER|DROP)\s+(?:OR\s+REPLACE\s+)?(VIEW|MATERIALIZED\s+VIEW|SEQUENCE|STAGE|FILE\s+FORMAT|PIPE|TASK|FUNCTION|PROCEDURE|ROLE|GRANT)\s+([^\s;]+)"
    match = re.search(regex, sql_text, re.IGNORECASE)
    if not match:
        return None

    action = match.group(1).upper()
    obj_type = match.group(2).upper().replace(" ", "_")  # normalize
    full_name = match.group(3).strip().strip('"')

    # Schema handling
    if "." in full_name:
        parts = full_name.split(".")
        if len(parts) == 3:
            db, schema, name = parts
        elif len(parts) == 2:
            schema, name = parts
        else:
            schema, name = "", parts[0]
    else:
        schema, name = "", full_name

    return {
        "action": action,
        "type": obj_type,
        "schema": schema,
        "name": name
    }


# -----------------------------------------------------------
# 4. Get current DDL from Snowflake
# -----------------------------------------------------------
def get_current_ddl(conn, schema, name, obj_type):
    obj_sql_type = obj_type.replace("_", " ")
    sql = f"SHOW {obj_sql_type}S IN SCHEMA {schema}"
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            if row[1].upper() == name.upper():  # second column is object name
                # Use fully qualified object for GET_DDL
                fq_name = f"{schema}.{name}"
                cur.execute(f"SELECT GET_DDL('{obj_sql_type}', '{fq_name}')")
                ddl = cur.fetchone()[0]
                return ddl
    finally:
        cur.close()
    return None


# -----------------------------------------------------------
# 5. Save rollback DDL in rollback/ directory
# -----------------------------------------------------------
def save_rollback_file(schema, name, obj_type, ddl):
    rollback_dir = Path("rollback/premerge")
    rollback_dir.mkdir(parents=True, exist_ok=True)
    safe_obj_type = obj_type.lower().replace(" ", "_")
    file_name = f"{schema}.{name}.{safe_obj_type}.sql"
    path = rollback_dir / file_name
    with open(path, "w") as f:
        f.write(ddl)
    print(f"[INFO] Saved rollback DDL: {path}")


# -----------------------------------------------------------
# 6. Main logic
# -----------------------------------------------------------
def main():
    changed_files = get_changed_sql_files()
    if not changed_files:
        print("[INFO] No changed SQL files found — nothing to snapshot.")
        return

    conn = get_snowflake_connection()

    for file_path in changed_files:
        with open(file_path, "r") as f:
            sql_text = f.read()

        meta = parse_sql_metadata(sql_text)
        if not meta:
            continue

        if meta["type"] in ["TABLE"]:  # skip tables completely
            continue

        schema = meta["schema"] or os.environ["SNOWFLAKE_DATABASE"]
        ddl = get_current_ddl(conn, schema, meta["name"], meta["type"])
        if ddl:
            save_rollback_file(schema, meta["name"], meta["type"], ddl)

    conn.close()


if __name__ == "__main__":
    main()
