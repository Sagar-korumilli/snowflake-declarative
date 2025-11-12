import os
import re
import sys
import subprocess
import argparse
import logging
from pathlib import Path
import snowflake.connector

# ------------------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("deploy_tool")

# ------------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------------

def run_cmd(cmd, cwd=None):
    """Run a shell command and return output"""
    logger.debug(f"Running command: {cmd}")
    result = subprocess.run(cmd, cwd=cwd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(f"Command failed: {cmd}")
    return result.stdout.strip()

def connect_snowflake():
    """Connect to Snowflake using private key authentication"""
    key_file = "temp_key.pem"
    with open(key_file, "w") as f:
        f.write(os.environ["SNOWFLAKE_PRIVATE_KEY"])

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=key_file,
        private_key_file_pwd=os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )

    os.remove(key_file)
    logger.info("✅ Connected to Snowflake")
    return conn

def execute_sql(conn, sql_file):
    """Execute SQL file on Snowflake"""
    with open(sql_file, "r") as f:
        content = f.read()
    stmts = [s.strip() for s in content.split(";") if s.strip()]
    cur = conn.cursor()
    for idx, stmt in enumerate(stmts, start=1):
        short_stmt = stmt.replace("\n", " ")[:100]
        logger.info(f"▶ Executing statement {idx}: {short_stmt}...")
        cur.execute(stmt)
    cur.close()
    logger.info(f"✅ Executed {sql_file}")

def git_commit_and_push(file_path, msg):
    """Commit and push changes using personal access token"""
    token = os.environ["GIT_PUSH_TOKEN"]
    user_name = os.environ.get("GIT_USER_NAME", "github-actions")
    user_email = os.environ.get("GIT_USER_EMAIL", "actions@github.com")

    run_cmd(f"git config user.name \"{user_name}\"")
    run_cmd(f"git config user.email \"{user_email}\"")

    # Set remote with token
    origin_url = run_cmd("git remote get-url origin")
    if token not in origin_url:
        repo_https = origin_url.replace(
            "https://", f"https://{token}@"
        )
        run_cmd(f"git remote set-url origin {repo_https}")

    run_cmd(f"git add {file_path}")
    run_cmd(f"git commit -m \"{msg}\" || echo 'No changes to commit'")
    run_cmd("git push")
    logger.info(f"✅ Committed and pushed rollback for {file_path}")

def get_previous_commit_content(file_path):
    """Return previous commit version of the file"""
    try:
        content = run_cmd(f"git show HEAD^:{file_path}")
        return content
    except Exception:
        logger.warning(f"⚠️ Could not get previous commit for {file_path}")
        return None

def update_file_with_content(file_path, new_content):
    with open(file_path, "w") as f:
        f.write(new_content)
    logger.info(f"📝 Updated file with previous commit: {file_path}")

# ------------------------------------------------------------------------------
# Rollback Logic
# ------------------------------------------------------------------------------

def handle_rollback(conn, files):
    """Rollback logic for both rollback/*.sql and snowflake/* paths"""
    if not files:
        logger.info("ℹ️ No rollback files provided — skipping rollback.")
        return

    rollback_files = [f for f in files if f.startswith("rollback/")]
    ddl_files = [f for f in files if f.startswith("snowflake/")]

    # Execute rollback SQLs directly
    for fpath in rollback_files:
        logger.info(f"▶ Executing rollback SQL file: {fpath}")
        execute_sql(conn, fpath)

    # Process DDL-based rollback for non-table objects
    for fpath in ddl_files:
        parts = Path(fpath).parts
        if len(parts) < 3:
            continue
        object_type = parts[2].lower()
        if object_type == "tables":
            logger.info(f"⏩ Skipping table rollback: {fpath}")
            continue

        prev_content = get_previous_commit_content(fpath)
        if not prev_content:
            continue

        update_file_with_content(fpath, prev_content)
        git_commit_and_push(fpath, f"rollback: reverted {fpath} to previous commit")

        # Execute reverted DDL
        execute_sql(conn, fpath)

# ------------------------------------------------------------------------------
# Deployment Logic
# ------------------------------------------------------------------------------

def handle_deploy(conn, files):
    """Execute deployment SQL files"""
    if not files:
        files = [str(p) for p in Path("deploy").rglob("*.sql")]
    logger.info(f"🚀 Starting deploy for {len(files)} SQL files")
    for fpath in files:
        execute_sql(conn, fpath)

# ------------------------------------------------------------------------------
# Initial setup
# ------------------------------------------------------------------------------

def handle_initial_setup(conn):
    """Run initial setup scripts once"""
    setup_files = [str(p) for p in Path("initial_setup").rglob("*.sql")]
    logger.info(f"🧩 Running initial setup for {len(setup_files)} SQL files")
    for fpath in setup_files:
        execute_sql(conn, fpath)

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Snowflake deployment tool")
    parser.add_argument("--mode", required=True, choices=["deploy", "rollback", "initial_setup"])
    parser.add_argument("--files", default="", help="Comma-separated list of SQL files")
    args = parser.parse_args()

    files = [f.strip() for f in args.files.split(",") if f.strip()]
    conn = connect_snowflake()

    try:
        if args.mode == "initial_setup":
            handle_initial_setup(conn)
        elif args.mode == "deploy":
            handle_deploy(conn, files)
        elif args.mode == "rollback":
            handle_rollback(conn, files)
    finally:
        conn.close()
        logger.info("🔚 Connection closed.")

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
