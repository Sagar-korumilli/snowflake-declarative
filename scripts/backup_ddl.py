#!/usr/bin/env python3
"""
backup_ddl.py

Usage:
    python scripts/backup_ddl.py --snowflake-root snowflake [--dry-run] [--debug]
"""
import argparse
import os
import re
import sys
import tempfile
import logging
from pathlib import Path
import subprocess
from typing import List, Optional, Tuple
import snowflake.connector

# ---- logging ---------------------------------------------------------------
def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)

logger = setup_logging()


# ---- Snowflake connection --------------------------------------------------
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
    for var in required:
        if not os.getenv(var):
            raise RuntimeError(f"❌ Missing environment variable: {var}")

    # write private key to a temp file
    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".pem") as key_file:
        key_file.write(os.getenv("SNOWFLAKE_PRIVATE_KEY"))
        key_path = key_file.name
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
        # cleanup key file on failure
        try:
            os.remove(key_path)
        except Exception:
            pass
        raise RuntimeError(f"❌ Failed to connect to Snowflake: {e}")


def get_current_ddl(conn: snowflake.connector.SnowflakeConnection, object_type: str, full_name: str) -> Optional[str]:
    """
    Calls GET_DDL on Snowflake for the fully qualified name: DB.SCHEMA.OBJECT
    """
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


# ---- Git helper functions --------------------------------------------------
def configure_git_credentials():
    name = os.getenv("GIT_USER_NAME", "DDL Sync Bot")
    email = os.getenv("GIT_USER_EMAIL", "ddl-sync@noreply.github.com")
    token = os.getenv("GIT_PUSH_TOKEN") or os.getenv("GITHUB_TOKEN")

    if not token:
        raise RuntimeError("❌ No authentication token found. Set GIT_PUSH_TOKEN or GITHUB_TOKEN")

    try:
        subprocess.run(["git", "config", "--local", "user.name", name], check=True)
        subprocess.run(["git", "config", "--local", "user.email", email], check=True)

        # find remote url and set tokenized URL
        repo = os.getenv("GITHUB_REPOSITORY")
        if repo:
            auth_url = f"https://{token}@github.com/{repo}.git"
        else:
            url = subprocess.check_output(["git", "config", "--get", "remote.origin.url"], text=True).strip()
            if "github.com" in url:
                if url.startswith("https://"):
                    auth_url = url.replace("https://github.com/", f"https://{token}@github.com/")
                else:
                    # e.g. git@github.com:owner/repo.git
                    repo_path = url.split(":", 1)[1].replace(".git", "")
                    auth_url = f"https://{token}@github.com/{repo_path}.git"
            else:
                raise RuntimeError(f"❌ Unsupported git remote: {url}")
        subprocess.run(["git", "remote", "set-url", "origin", auth_url], check=True)
        logger.info("🔑 Git remote configured with authentication")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ Failed to configure Git credentials: {e}")


def has_changes_to_commit(file_path: Path) -> bool:
    try:
        result = subprocess.run(["git", "status", "--porcelain", str(file_path)], capture_output=True, text=True, check=True)
        return bool(result.stdout.strip())
    except subprocess.CalledProcessError:
        return False


def git_add_commit_push(file_path: Path, message: str, dry_run: bool = False):
    if dry_run:
        logger.info(f"🔍 [DRY RUN] Would commit and push: {file_path}")
        return

    if not has_changes_to_commit(file_path):
        logger.info(f"ℹ️ No changes detected in {file_path}")
        return

    try:
        configure_git_credentials()
        subprocess.run(["git", "add", str(file_path)], check=True)
        # Check if something is staged
        result = subprocess.run(["git", "diff", "--cached", "--exit-code"], capture_output=True)
        if result.returncode == 0:
            logger.info(f"ℹ️ No staged changes for {file_path}")
            return
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)
        logger.info(f"✅ Successfully pushed updated DDL for {file_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Git operation failed for {file_path}: {e}")


# ---- file detection / heuristics -------------------------------------------
def find_changed_sql_files(sf_root: str) -> List[Path]:
    """
    Recursively find *.sql files under sf_root that contain ALTER statements.
    """
    altered: List[Path] = []
    alter_pattern = r"ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+"
    root_path = Path(sf_root)

    if not root_path.exists():
        raise FileNotFoundError(f"❌ Snowflake root directory not found: {sf_root}")

    for f in root_path.rglob("*.sql"):
        # skip rollback or dotfiles
        if any(part.lower() in ("rollback", ".git") for part in f.parts):
            continue
        try:
            text = f.read_text(encoding="utf-8")
            if re.search(alter_pattern, text, re.IGNORECASE):
                altered.append(f)
                logger.info(f"🔍 Found ALTER statement in: {f}")
        except Exception as e:
            logger.warning(f"⚠️ Could not read file {f}: {e}")
    return altered


def extract_alter_statements(sql_content: str) -> List[Tuple[str, Optional[str], str]]:
    """
    Return list of tuples: (OBJECT_TYPE, optional SCHEMA_NAME, OBJECT_NAME)
    Handles both:
      ALTER TABLE SCHEMA.OBJECT ...
      ALTER TABLE OBJECT ...
    Returns object names upper-cased (unquoted).
    """
    results: List[Tuple[str, Optional[str], str]] = []

    # Pattern 1: fully-qualified, accepts optional quoting with " or `
    fq_pattern = re.compile(
        r'ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+'
        r'(?:["`]?([A-Za-z0-9_]+)["`]?\.)\s*["`]?([A-Za-z0-9_]+)["`]?',
        re.IGNORECASE,
    )
    for m in fq_pattern.finditer(sql_content):
        obj_type = m.group(1).upper()
        schema_name = m.group(2).upper()
        obj_name = m.group(3).upper()
        results.append((obj_type, schema_name, obj_name))

    # Pattern 2: unqualified object (no schema). Only accept if not already captured.
    unq_pattern = re.compile(
        r'ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+["`]?([A-Za-z0-9_]+)["`]?',
        re.IGNORECASE,
    )
    for m in unq_pattern.finditer(sql_content):
        obj_type = m.group(1).upper()
        obj_name = m.group(2).upper()
        # to avoid duplicating the fully-qualified matches, skip if we already captured same obj_type+obj_name
        if any(o == obj_type and n == obj_name for (o, s, n) in results):
            continue
        results.append((obj_type, None, obj_name))

    return results


def find_object_file(target_dir: Path, object_name: str, object_type: str) -> Path:
    """
    Try to find an existing file owning this object within target_dir.
    If none found, create a new filename sensible for migrations.
    """
    object_name_lower = object_name.lower()
    candidates: List[Path] = []

    # Priority 1: explicit patterns like *__<object>_table.sql or *__<object>_view.sql
    candidates += list(target_dir.glob(f"*__{object_name_lower}_table.sql"))
    candidates += list(target_dir.glob(f"*__{object_name_lower}_{object_type.lower()}.sql"))

    # Priority 2: *__<object>.sql
    if not candidates:
        candidates += list(target_dir.glob(f"*__{object_name_lower}.sql"))

    # Priority 3: filenames containing __<object> followed by _ or . or end
    if not candidates:
        candidates += [p for p in target_dir.glob(f"*{object_name_lower}*.sql") if re.search(rf"__{object_name_lower}([_.]|$)", p.name)]

    # Priority 4: any containing object name
    if not candidates:
        candidates += list(target_dir.glob(f"*{object_name_lower}*.sql"))

    if candidates:
        chosen = sorted(candidates, key=lambda p: len(p.name))[0]
        logger.info(f"✅ Will update DDL file: {chosen}")
        return chosen

    # fallback new file
    # try to create a migration-like filename V{timestamp}__<object>_<type>.sql
    stamp = subprocess.check_output(["date", "+%s"]).decode().strip() if shutil_available() else str(int(os.times()[4]))
    new_name = f"V{stamp}__{object_name_lower}_{object_type.lower()}.sql"
    new_file = target_dir / new_name
    logger.info(f"ℹ️ Will create new file: {new_file}")
    return new_file


def shutil_available() -> bool:
    # helper to safely check if 'date' command exists; using fallback if not
    try:
        subprocess.check_output(["date", "+%s"])
        return True
    except Exception:
        return False


# ---- core update logic ----------------------------------------------------
def update_object_file(sf_root: Path, changed_file: Path, conn: snowflake.connector.SnowflakeConnection, dry_run: bool = False):
    """
    sf_root: Path to the provided --snowflake-root (used to infer schema root)
    changed_file: the SQL file that contains ALTER statements
    """
    try:
        sql_content = changed_file.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"❌ Could not read changed file {changed_file}: {e}")
        return

    alter_statements = extract_alter_statements(sql_content)
    if not alter_statements:
        logger.info(f"ℹ️ No ALTER statements found in {changed_file.name}")
        return

    db = os.getenv("SNOWFLAKE_DATABASE")
    if not db:
        logger.error("❌ SNOWFLAKE_DATABASE not set")
        return

    for obj_type, schema_name, obj_name in alter_statements:
        # derive schema from path if missing
        if not schema_name:
            # heuristic: we expect changed_file path like <root>/<schema>/<maybe tables|views>/file.sql
            try:
                rel = changed_file.relative_to(sf_root)
            except Exception:
                # fallback to parents heuristic
                rel = Path(*changed_file.parts[-4:])  # last few parts
            parts = rel.parts
            schema_guess = None
            if len(parts) >= 2:
                # if the second part is 'tables' or 'views', schema is first part
                if parts[1].lower() in ("tables", "views"):
                    schema_guess = parts[0]
                else:
                    # else treat first part as schema (works if path is schema/... )
                    schema_guess = parts[0]
            if not schema_guess and changed_file.parent and changed_file.parent.parent:
                schema_guess = changed_file.parent.parent.name
            if not schema_guess:
                logger.warning(f"⚠️ Could not infer schema for {changed_file}; skipping object {obj_name}")
                continue
            schema_name = schema_guess.upper()
            logger.debug(f"ℹ️ Derived schema '{schema_name}' from path {changed_file}")

        full_name = f"{db}.{schema_name}.{obj_name}"
        logger.info(f"🔄 Processing {obj_type}: {full_name}")

        ddl = get_current_ddl(conn, obj_type, full_name)
        if not ddl:
            logger.warning(f"⚠️ Skipping {full_name} - could not retrieve DDL")
            continue

        # find schema root folder (the folder under sf_root that equals schema_name, case-insensitive)
        schema_root = None
        for p in sf_root.iterdir():
            if p.is_dir() and p.name.lower() == schema_name.lower():
                schema_root = p
                break
        if not schema_root:
            # fallback: try parent of changed_file up one level
            schema_root = changed_file.parent.parent if changed_file.parent and changed_file.parent.parent else changed_file.parent

        # choose target folder
        if obj_type == "TABLE":
            target_dir = schema_root / "tables"
        elif obj_type == "VIEW":
            target_dir = schema_root / "views"
        else:
            # for other object types, put in top-level schema folder, you can tweak this
            target_dir = schema_root / "objects"

        target_dir.mkdir(parents=True, exist_ok=True)

        # find best matching file inside target_dir
        target_file = find_object_file(target_dir, obj_name, obj_type)

        if dry_run:
            logger.info(f"🔍 [DRY RUN] Would update: {target_file}")
            continue

        ddl_content = ddl.strip() + "\n"
        try:
            target_file.write_text(ddl_content, encoding="utf-8")
            commit_message = f"chore: refresh {obj_type.lower()} DDL for {full_name}"
            git_add_commit_push(target_file, commit_message, dry_run)
            logger.info(f"✅ Written refreshed DDL to {target_file}")
        except Exception as e:
            logger.error(f"❌ Could not write DDL to {target_file}: {e}")


# ---- main -----------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Refresh object-level DDL in Git from Snowflake after ALTER statements")
    parser.add_argument("--snowflake-root", required=True, help="Path to repo root containing schema folders (e.g. 'snowflake')")
    parser.add_argument("--dry-run", action="store_true", help="Do not write or push changes; just log actions")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    sf_root = Path(args.snowflake_root)
    logger.info("🚀 Starting DDL synchronization process")
    try:
        changed_files = find_changed_sql_files(str(sf_root))
        if not changed_files:
            logger.info("✅ No ALTER scripts detected; exiting.")
            return
        logger.info(f"🔍 Detected {len(changed_files)} files with ALTER statements")
        conn, key_path = get_snowflake_connection()
        try:
            for changed_file in changed_files:
                logger.info(f"🔄 Processing: {changed_file}")
                update_object_file(sf_root, changed_file, conn, args.dry_run)
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
