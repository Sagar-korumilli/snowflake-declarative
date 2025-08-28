#!/usr/bin/env python3
from __future__ import annotations
import argparse
import logging
import os
import re
import sys
import time
import tempfile
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple
import snowflake.connector

# ---------------- logging ----------------
def setup_logging(debug: bool = False) -> logging.Logger:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)

# ---------------- Snowflake ----------------
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

    # write private key to temporary file
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
        raise RuntimeError(f"❌ Failed to connect to Snowflake: {e}")

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

# ---------------- Git helpers ----------------
def configure_git_identity(name: str, email: str):
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

        if target_branch:
            # push the detached HEAD commit to the target branch
            subprocess.run(["git", "push", "origin", f"HEAD:{target_branch}"], check=True)
            logger.info(f"➡️ Pushed commit to origin:{target_branch}")
        else:
            subprocess.run(["git", "push", "origin", "HEAD"], check=True)
            logger.info("➡️ Pushed commit to origin HEAD")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Git operation failed for {file_path}: {e}")

ALTER_RE = re.compile(
    r'''
    ALTER\s+
    (TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)   # object type
    \s+
    (?:
        (?:"([^"]+)"|`([^`]+)`|([A-Za-z0-9_]+))  # optional db (group 2/3/4)
        \.
    )?
    (?:
        (?:"([^"]+)"|`([^`]+)`|([A-Za-z0-9_]+))  # optional schema (group 5/6/7)
        \.
    )?
    (?:
        (?:"([^"]+)"|`([^`]+)`|([A-Za-z0-9_]+))  # object (group 8/9/10)
    )
    ''',
    re.IGNORECASE | re.VERBOSE,
)

def extract_alter_statements(sql_content: str) -> List[Tuple[str, Optional[str], Optional[str], str]]:
    """
    Returns list of tuples: (OBJECT_TYPE, optional DB, optional SCHEMA, OBJECT)
    All returned names are upper-cased and unquoted.
    """
    results: List[Tuple[str, Optional[str], Optional[str], str]] = []
    for m in ALTER_RE.finditer(sql_content):
        obj_type = m.group(1).upper()
        # db can be in group 2,3,4
        db = (m.group(2) or m.group(3) or m.group(4) or None)
        schema = (m.group(5) or m.group(6) or m.group(7) or None)
        obj = (m.group(8) or m.group(9) or m.group(10))
        # normalize
        db_u = db.upper() if db else None
        schema_u = schema.upper() if schema else None
        obj_u = obj.upper()
        results.append((obj_type, db_u, schema_u, obj_u))
    return results

def find_changed_sql_files(sf_root: str) -> List[Path]:
    altered: List[Path] = []
    root = Path(sf_root)
    if not root.exists():
        raise FileNotFoundError(f"❌ Snowflake root not found: {sf_root}")

    for p in root.rglob("*.sql"):
        if any(part.lower() in ("rollback", ".git") for part in p.parts):
            continue
        try:
            txt = p.read_text(encoding="utf-8")
            if re.search(r'ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+', txt, re.IGNORECASE):
                altered.append(p)
                logger.info(f"🔍 Found ALTER statement in: {p}")
        except Exception as e:
            logger.warning(f"⚠️ Could not read {p}: {e}")
    return altered

# ---------------- locate existing files (do not create new) ----------------
def find_existing_object_file(schema_root: Path, object_name: str, object_type: str) -> Optional[Path]:
    """
    Search for an existing DDL file that likely owns the object.
    Return Path or None (do NOT create files).
    Search order: tables/, views/, schema root.
    """
    object_name_lower = object_name.lower()
    candidate_dirs = [schema_root / "tables", schema_root / "views", schema_root]

    for d in candidate_dirs:
        if not d.exists():
            continue
        patterns = [
            f"*__{object_name_lower}_table.sql",
            f"*__{object_name_lower}_{object_type.lower()}.sql",
            f"*__{object_name_lower}.sql",
            f"*{object_name_lower}*.sql",
        ]
        matches = []
        for pat in patterns:
            matches.extend(list(d.glob(pat)))
        if matches:
            chosen = sorted(matches, key=lambda p: len(p.name))[0]
            logger.info(f"✅ Will update existing DDL file: {chosen}")
            return chosen
    return None

def backup_and_overwrite(target_file: Path, new_content: str, dry_run: bool):
    # attempt to determine schema root for backup dir
    schema_root = target_file.parent.parent if target_file.parent and target_file.parent.parent else target_file.parent
    backup_dir = schema_root / "backup"
    timestamp = int(time.time())
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{target_file.name}.{timestamp}.bak"
    if dry_run:
        logger.info(f"🔍 [DRY RUN] Would backup {target_file} -> {backup_path} and overwrite")
        return
    try:
        if target_file.exists():
            content = target_file.read_text(encoding="utf-8")
            backup_path.write_text(content, encoding="utf-8")
            logger.info(f"📦 Backed up existing file to {backup_path}")
        target_file.write_text(new_content, encoding="utf-8")
        logger.info(f"✏️ Overwrote {target_file} with refreshed DDL")
    except Exception as e:
        logger.error(f"❌ Failed to backup/write {target_file}: {e}")
        raise

# ---------------- core processing ----------------
def update_object_file(sf_root: Path, changed_file: Path, conn: snowflake.connector.SnowflakeConnection,
                       dry_run: bool, git_name: str, git_email: str, target_branch: Optional[str]):
    try:
        sql_content = changed_file.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"❌ Could not read {changed_file}: {e}")
        return

    alters = extract_alter_statements(sql_content)
    if not alters:
        logger.info(f"ℹ️ No ALTER statements in {changed_file.name}")
        return

    env_db = os.getenv("SNOWFLAKE_DATABASE")
    if not env_db:
        logger.error("❌ SNOWFLAKE_DATABASE not set")
        return

    for obj_type, stmt_db, stmt_schema, obj_name in alters:
        # Determine DB to use: prefer db from statement else SNOWFLAKE_DATABASE
        use_db = stmt_db or env_db

        # Determine schema: prefer stmt_schema else derive from path
        if stmt_schema:
            use_schema = stmt_schema
        else:
            try:
                rel = changed_file.relative_to(sf_root)
                parts = rel.parts
                if len(parts) >= 2 and parts[1].lower() in ("tables", "views"):
                    schema_guess = parts[0]
                elif len(parts) >= 1:
                    schema_guess = parts[0]
                else:
                    schema_guess = None
                if not schema_guess:
                    # fallback: parent.parent
                    schema_guess = changed_file.parent.parent.name if changed_file.parent and changed_file.parent.parent else None
                if not schema_guess:
                    logger.warning(f"⚠️ Could not derive schema for {changed_file}; skipping {obj_name}")
                    continue
                use_schema = schema_guess.upper()
            except Exception:
                use_schema = changed_file.parent.parent.name.upper() if changed_file.parent and changed_file.parent.parent else None
                if not use_schema:
                    logger.warning(f"⚠️ Could not derive schema for {changed_file}; skipping {obj_name}")
                    continue

        full_name = f"{use_db}.{use_schema}.{obj_name}"
        logger.debug(f"Computed use_db={use_db}, use_schema={use_schema}, obj_name={obj_name}")
        logger.info(f"🔄 Processing {obj_type}: {full_name}")

        ddl = get_current_ddl(conn, obj_type, full_name)
        if not ddl:
            logger.warning(f"⚠️ Skipping {full_name} - could not retrieve DDL")
            continue

        # find the schema root folder under sf_root
        schema_root = None
        for p in sf_root.iterdir():
            if p.is_dir() and p.name.lower() == use_schema.lower():
                schema_root = p
                break
        if not schema_root:
            schema_root = changed_file.parent.parent if changed_file.parent and changed_file.parent.parent else changed_file.parent

        target_file = find_existing_object_file(schema_root, obj_name, obj_type)
        if not target_file:
            logger.warning(f"⚠️ No existing DDL file found for {full_name} under {schema_root}; skipping (will not create new).")
            continue

        ddl_content = ddl.strip() + "\n"
        backup_and_overwrite(target_file, ddl_content, dry_run)

        if not dry_run:
            try:
                configure_git_identity(git_name, git_email)
            except Exception as e:
                logger.error(f"❌ Failed to configure git identity: {e}")
            commit_msg = f"chore: refresh {obj_type.lower()} DDL for {full_name}"
            git_add_commit_push(target_file, commit_msg, target_branch, dry_run)

# ---------------- main ----------------
def main():
    parser = argparse.ArgumentParser(description="Refresh object DDL from Snowflake (overwrite only existing DDL files).")
    parser.add_argument("--snowflake-root", required=True, help="Root folder that contains schema folders (e.g. 'snowflake')")
    parser.add_argument("--dry-run", action="store_true", help="Do not write or push; only log")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    global logger
    logger = setup_logging(debug=args.debug)

    sf_root = Path(args.snowflake_root)
    if not sf_root.exists():
        logger.error(f"❌ Provided snowflake root does not exist: {sf_root}")
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
            git_name = os.getenv("GIT_USER_NAME", "DDL Sync Bot")
            git_email = os.getenv("GIT_USER_EMAIL", "ddl-sync@noreply.github.com")
            target_branch = os.getenv("TARGET_PUSH_BRANCH")  # set in CI: github.event.pull_request.base.ref
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
    logger = setup_logging(False)
    main()
