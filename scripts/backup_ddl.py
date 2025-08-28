#!/usr/bin/env python3
"""
backup_ddl.py

- Only replaces existing DDL files (does NOT create new files).
- Creates a timestamped backup under <schema>/backup/ before overwriting.
- Pushes commits back to a specified branch provided via env TARGET_PUSH_BRANCH.
"""
import argparse
import os
import re
import sys
import time
import tempfile
import logging
from pathlib import Path
import subprocess
from typing import List, Optional, Tuple
import snowflake.connector

# ---- logging ----
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ---- Snowflake connection (private key file written temporarily) ----
def get_snowflake_connection() -> Tuple[snowflake.connector.SnowflakeConnection, str]:
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_PRIVATE_KEY",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    ]
    for v in required:
        if not os.getenv(v):
            raise RuntimeError(f"❌ Missing environment variable: {v}")

    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".pem") as tf:
        tf.write(os.getenv("SNOWFLAKE_PRIVATE_KEY"))
        key_path = tf.name
    os.chmod(key_path, 0o600)

    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            private_key_file=key_path,
            private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            authenticator="snowflake_jwt",
        )
        logger.info("✅ Successfully connected to Snowflake")
        return conn, key_path
    except Exception as e:
        try:
            os.remove(key_path)
        except Exception:
            pass
        raise

def get_current_ddl(conn: snowflake.connector.SnowflakeConnection, object_type: str, full_name: str) -> Optional[str]:
    try:
        with conn.cursor() as cur:
            query = f"SELECT GET_DDL('{object_type}', '{full_name}', TRUE)"
            cur.execute(query)
            row = cur.fetchone()
            if row and row[0]:
                logger.info(f"✅ Retrieved DDL for {full_name}")
                return row[0]
            else:
                logger.warning(f"⚠️ No DDL returned for {full_name}")
                return None
    except Exception as e:
        logger.error(f"❌ Failed to get DDL for {full_name}: {e}")
        return None

# ---- Git helpers ----
def configure_git_credentials(name: str, email: str):
    try:
        subprocess.run(["git", "config", "--local", "user.name", name], check=True)
        subprocess.run(["git", "config", "--local", "user.email", email], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ Failed to set git identity: {e}")

def has_changes_to_commit(file_path: Path) -> bool:
    try:
        res = subprocess.run(["git", "status", "--porcelain", str(file_path)], capture_output=True, text=True, check=True)
        return bool(res.stdout.strip())
    except subprocess.CalledProcessError:
        return False

def git_add_commit_push(file_path: Path, message: str, target_branch: Optional[str], dry_run: bool):
    if dry_run:
        logger.info(f"🔍 [DRY RUN] Would git-add/commit/push: {file_path}")
        return

    if not has_changes_to_commit(file_path):
        logger.info(f"ℹ️ No changes detected in {file_path}")
        return

    try:
        subprocess.run(["git", "add", str(file_path)], check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)

        # If caller passed a target branch, push detached HEAD to that branch
        if target_branch:
            logger.info(f"➡️ Pushing commit to origin {target_branch}")
            subprocess.run(["git", "push", "origin", f"HEAD:{target_branch}"], check=True)
        else:
            logger.info("➡️ Pushing commit to origin HEAD")
            subprocess.run(["git", "push", "origin", "HEAD"], check=True)
        logger.info(f"✅ Pushed {file_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Git operation failed for {file_path}: {e}")

# ---- SQL detection ----
def find_changed_sql_files(sf_root: str) -> List[Path]:
    altered: List[Path] = []
    alter_pattern = r"ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+"
    root = Path(sf_root)
    if not root.exists():
        raise FileNotFoundError(f"❌ Snowflake root not found: {sf_root}")

    for p in root.rglob("*.sql"):
        # skip rollback and .git
        if any(part.lower() in ("rollback", ".git") for part in p.parts):
            continue
        try:
            txt = p.read_text(encoding="utf-8")
            if re.search(alter_pattern, txt, re.IGNORECASE):
                altered.append(p)
                logger.info(f"🔍 Found ALTER statement in: {p}")
        except Exception as e:
            logger.warning(f"⚠️ Could not read {p}: {e}")
    return altered

def extract_alter_statements(sql_content: str) -> List[Tuple[str, Optional[str], str]]:
    """
    Returns list of (OBJECT_TYPE, optional SCHEMA, OBJECT_NAME)
    Prioritizes fully-qualified forms; also matches unqualified object names.
    """
    results = []
    fq = re.compile(
        r'ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+(?:["`]?([A-Za-z0-9_]+)["`]?\.)?["`]?([A-Za-z0-9_]+)["`]?',
        re.IGNORECASE,
    )
    for m in fq.finditer(sql_content):
        obj_type = m.group(1).upper()
        schema = m.group(2).upper() if m.group(2) else None
        obj_name = m.group(3).upper()
        results.append((obj_type, schema, obj_name))
    return results

# ---- locate & update file ----
def find_existing_object_file(schema_root: Path, object_name: str, object_type: str) -> Optional[Path]:
    """
    Look for an existing file owning this object under schema_root (search tables/, views/ and root).
    Return Path if found; otherwise return None (do NOT create a new file).
    """
    object_name_lower = object_name.lower()

    # candidate dirs to search (tables, views, schema root)
    candidates_dirs = [schema_root / "tables", schema_root / "views", schema_root]

    for d in candidates_dirs:
        if not d.exists():
            continue
        # priority patterns
        patterns = [
            f"*__{object_name_lower}_table.sql",
            f"*__{object_name_lower}_{object_type.lower()}.sql",
            f"*__{object_name_lower}.sql",
            f"*{object_name_lower}*.sql",
        ]
        matches = []
        for p in patterns:
            matches += list(d.glob(p))
        if matches:
            chosen = sorted(matches, key=lambda p: len(p.name))[0]
            logger.info(f"✅ Will update existing DDL file: {chosen}")
            return chosen
    # no existing file found
    return None

def backup_and_write(target_file: Path, new_content: str, dry_run: bool):
    schema_root = target_file.parent.parent  # e.g. snowflake/hr/tables -> parent.parent = snowflake/hr
    if not schema_root.exists():
        schema_root = target_file.parent
    backup_dir = schema_root / "backup"
    timestamp = int(time.time())
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{target_file.name}.{timestamp}.bak"
    if dry_run:
        logger.info(f"🔍 [DRY RUN] Would backup {target_file} -> {backup_path} and overwrite")
        return
    # copy current content to backup
    try:
        if target_file.exists():
            target_file.replace(target_file)  # no-op but ensures permission
            target_file_content = target_file.read_text(encoding="utf-8")
            backup_path.write_text(target_file_content, encoding="utf-8")
            logger.info(f"📦 Backed up existing file to {backup_path}")
        # overwrite with new content
        target_file.write_text(new_content, encoding="utf-8")
        logger.info(f"✏️ Overwrote {target_file} with refreshed DDL")
    except Exception as e:
        logger.error(f"❌ Failed to backup/write {target_file}: {e}")
        raise

def update_object_file(sf_root: Path, changed_file: Path, conn: snowflake.connector.SnowflakeConnection, dry_run: bool, git_name: str, git_email: str, target_branch: Optional[str]):
    try:
        sql_content = changed_file.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"❌ Could not read {changed_file}: {e}")
        return

    alters = extract_alter_statements(sql_content)
    if not alters:
        logger.info(f"ℹ️ No ALTER statements in {changed_file.name}")
        return

    database = os.getenv("SNOWFLAKE_DATABASE")
    if not database:
        logger.error("❌ SNOWFLAKE_DATABASE not set")
        return

    # find schema root (folder under sf_root that matches schema)
    for obj_type, schema_name, obj_name in alters:
        # if schema in SQL, use it, else derive from path: snowflake/<schema>/tables/...
        if not schema_name:
            try:
                rel = changed_file.relative_to(sf_root)
                parts = rel.parts
                # expected rel: <schema>/tables/<file.sql> or <schema>/<file.sql>
                if len(parts) >= 2 and parts[1].lower() in ("tables", "views"):
                    schema_guess = parts[0]
                else:
                    schema_guess = parts[0]
                schema_name = schema_guess.upper()
            except Exception:
                # fallback: parent.parent
                if changed_file.parent and changed_file.parent.parent:
                    schema_name = changed_file.parent.parent.name.upper()
                else:
                    logger.warning(f"⚠️ Could not derive schema for {changed_file}; skipping")
                    continue

        full_name = f"{database}.{schema_name}.{obj_name}"
        logger.info(f"🔄 Processing {obj_type}: {full_name}")

        ddl = get_current_ddl(conn, obj_type, full_name)
        if not ddl:
            logger.warning(f"⚠️ Skipping {full_name} - could not retrieve DDL")
            continue

        # locate schema root folder under sf_root
        schema_root = None
        for p in sf_root.iterdir():
            if p.is_dir() and p.name.lower() == schema_name.lower():
                schema_root = p
                break
        if not schema_root:
            # fallback
            schema_root = changed_file.parent.parent if changed_file.parent and changed_file.parent.parent else changed_file.parent

        # find existing file — **do not create new**
        target_file = find_existing_object_file(schema_root, obj_name, obj_type)
        if not target_file:
            logger.warning(f"⚠️ No existing DDL file found for {full_name} under {schema_root}; skipping (will not create new file).")
            continue

        # backup + write
        ddl_content = ddl.strip() + "\n"
        backup_and_write(target_file, ddl_content, dry_run)

        # commit & push (only if not dry-run)
        if not dry_run:
            # ensure git identity
            try:
                configure_git_credentials(git_name, git_email)
            except RuntimeError as e:
                logger.error(f"❌ {e}")
            commit_message = f"chore: refresh {obj_type.lower()} DDL for {full_name}"
            git_add_commit_push(target_file, commit_message, target_branch, dry_run)

# ---- main ----
def main():
    parser = argparse.ArgumentParser(description="Refresh object DDL from Snowflake; overwrite only existing DDL files.")
    parser.add_argument("--snowflake-root", required=True, help="Root folder that contains schema subfolders (e.g. 'snowflake')")
    parser.add_argument("--dry-run", action="store_true", help="Do not write or push; only log")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    sf_root = Path(args.snowflake_root)
    if not sf_root.exists():
        logger.error(f"❌ Provided root not found: {sf_root}")
        sys.exit(1)

    logger.info("🚀 Starting DDL synchronization process")
    try:
        altered_files = find_changed_sql_files(str(sf_root))
        if not altered_files:
            logger.info("✅ No ALTER scripts detected; exiting.")
            return
        logger.info(f"🔍 Detected {len(altered_files)} files with ALTER statements")

        conn, key_path = get_snowflake_connection()
        try:
            # git identity / push target
            git_name = os.getenv("GIT_USER_NAME", "DDL Sync Bot")
            git_email = os.getenv("GIT_USER_EMAIL", "ddl-sync@noreply.github.com")
            target_branch = os.getenv("TARGET_PUSH_BRANCH")  # set this in CI to the branch you'd like to update
            logger.debug(f"git_name={git_name}, git_email={git_email}, target_branch={target_branch}")

            for f in altered_files:
                logger.info(f"🔄 Processing: {f}")
                update_object_file(sf_root, f, conn, args.dry_run, git_name, git_email, target_branch)

        finally:
            try:
                conn.close()
            except Exception:
                pass
            try:
                os.remove(key_path)
            except Exception:
                pass
            logger.info("🔒 Cleaned up Snowflake connection")
        logger.info("✅ DDL synchronization completed successfully")
    except Exception as e:
        logger.error(f"❌ DDL synchronization failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
