#!/usr/bin/env python3
import argparse
import os
import re
import sys
import tempfile
import logging
from pathlib import Path
import subprocess
import snowflake.connector
from typing import List, Optional, Tuple

def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def get_snowflake_connection() -> Tuple[snowflake.connector.SnowflakeConnection, str]:
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

    try:
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
        logger.info("✅ Successfully connected to Snowflake")
        return conn, key_path
    except Exception as e:
        os.remove(key_path)
        raise RuntimeError(f"❌ Failed to connect to Snowflake: {e}")

def get_current_ddl(conn: snowflake.connector.SnowflakeConnection, 
                   object_type: str, full_name: str) -> Optional[str]:
    try:
        with conn.cursor() as cur:
            query = f"SELECT GET_DDL('{object_type}', '{full_name}', TRUE)"
            cur.execute(query)
            result = cur.fetchone()
            if result and result[0]:
                logger.info(f"✅ Retrieved DDL for {full_name}")
                return result[0]
            else:
                logger.warning(f"⚠️ No DDL returned for {full_name}")
                return None
    except Exception as e:
        logger.error(f"❌ Failed to get DDL for {full_name}: {e}")
        return None

def configure_git_credentials():
    name = os.getenv('GIT_USER_NAME', 'DDL Sync Bot')
    email = os.getenv('GIT_USER_EMAIL', 'ddl-sync@noreply.github.com')
    token = os.getenv('GIT_PUSH_TOKEN') or os.getenv('GITHUB_TOKEN')

    if not token:
        raise RuntimeError("❌ No authentication token found. Set GIT_PUSH_TOKEN or GITHUB_TOKEN")

    try:
        subprocess.run(["git", "config", "--local", "user.name", name], check=True)
        subprocess.run(["git", "config", "--local", "user.email", email], check=True)

        # Get repository info
        repo = os.getenv('GITHUB_REPOSITORY')
        if repo:
            auth_url = f"https://{token}@github.com/{repo}.git"
        else:
            try:
                url = subprocess.check_output(
                    ["git", "config", "--get", "remote.origin.url"], text=True
                ).strip()
                if 'github.com' in url:
                    if url.startswith('https://'):
                        if '@github.com' in url:
                            url = re.sub(r'https://[^@]+@github.com/', 'https://github.com/', url)
                        auth_url = url.replace('https://github.com/', f'https://{token}@github.com/')
                    else:
                        repo_path = url.split(':')[1].replace('.git', '')
                        auth_url = f"https://{token}@github.com/{repo_path}.git"
                else:
                    raise RuntimeError(f"❌ Unsupported git remote: {url}")
            except subprocess.CalledProcessError:
                raise RuntimeError("❌ Could not determine git remote URL")
        subprocess.run(["git", "remote", "set-url", "origin", auth_url], check=True)
        logger.info("🔑 Git remote configured with authentication")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ Failed to configure Git credentials: {e}")

def has_changes_to_commit(file_path: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", str(file_path)], 
            capture_output=True, text=True, check=True
        )
        return bool(result.stdout.strip())
    except subprocess.CalledProcessError:
        return False

def git_add_commit_push(file_path: Path, message: str, dry_run: bool = False):
    if dry_run:
        logger.info(f"🔍 [DRY RUN] Would commit and push: {file_path.name}")
        return

    if not has_changes_to_commit(file_path):
        logger.info(f"ℹ️ No changes detected in {file_path.name}")
        return

    try:
        configure_git_credentials()
        subprocess.run(["git", "add", str(file_path)], check=True)
        # Check if there's anything staged
        result = subprocess.run(
            ["git", "diff", "--cached", "--exit-code"], 
            capture_output=True
        )
        if result.returncode == 0:
            logger.info(f"ℹ️ No staged changes for {file_path.name}")
            return
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)
        logger.info(f"✅ Successfully pushed updated DDL for {file_path.name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Git operation failed for {file_path.name}: {e}")

def find_changed_sql_files(sf_root: str) -> List[Path]:
    altered = []
    alter_pattern = r'ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+'
    root_path = Path(sf_root)
    if not root_path.exists():
        raise FileNotFoundError(f"❌ Snowflake root directory not found: {sf_root}")
    for schema_dir in root_path.iterdir():
        if not schema_dir.is_dir() or schema_dir.name.lower() in ['rollback', '.git']:
            continue
        for f in schema_dir.glob("*.sql"):
            try:
                text = f.read_text(encoding='utf-8')
                if re.search(alter_pattern, text, re.IGNORECASE):
                    altered.append(f)
                    logger.info(f"🔍 Found ALTER statement in: {f}")
            except Exception as e:
                logger.warning(f"⚠️ Could not read file {f}: {e}")
    return altered

def find_object_file(schema_path: Path, object_name: str, object_type: str) -> Path:
    """
    Stricter matching for correct object file!
    """
    object_name_lower = object_name.lower()

    # Priority 1: Exact migration filename for the table/view
    candidates = list(schema_path.glob(f"*__{object_name_lower}_table.sql")) \
               + list(schema_path.glob(f"*__{object_name_lower}_{object_type.lower()}.sql"))
    # Priority 2: "__objectname.sql"
    if not candidates:
        candidates += list(schema_path.glob(f"*__{object_name_lower}.sql"))
    # Priority 3: filename part equals to object name (for cases like employees.sql, not employees2.sql)
    if not candidates:
        candidates += [f for f in schema_path.glob(f"*{object_name_lower}*.sql")
                       if re.search(rf"__{object_name_lower}([_.]|$)", f.name)]
    # Priority 4: fallback to any containing object_name (least strict)
    if not candidates:
        candidates += list(schema_path.glob(f"*{object_name_lower}*.sql"))
    if candidates:
        # Pick the file with the shortest name (most likely the canonical one)
        chosen = sorted(candidates, key=lambda p: len(p.name))[0]
        logger.info(f"✅ Will update DDL file: {chosen}")
        return chosen
    else:
        new_file = schema_path / f"{object_type.lower()}__{object_name_lower}.sql"
        logger.info(f"ℹ️ Will create new file: {new_file}")
        return new_file

def extract_alter_statements(sql_content: str) -> List[Tuple[str, str, str]]:
    # Handles ALTER <TYPE> schema.object
    pattern = r'ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+(\w+)\.(\w+)'
    matches = re.findall(pattern, sql_content, re.IGNORECASE)
    results = []
    for obj_type, schema_name, obj_name in matches:
        results.append((obj_type.upper(), schema_name, obj_name))
    return results

def update_object_file(schema_path: Path, changed_file: Path, 
                      conn: snowflake.connector.SnowflakeConnection, 
                      dry_run: bool = False):
    try:
        sql_content = changed_file.read_text(encoding='utf-8')
        alter_statements = extract_alter_statements(sql_content)
        if not alter_statements:
            logger.info(f"ℹ️ No ALTER statements found in {changed_file.name}")
            return
        db = os.getenv('SNOWFLAKE_DATABASE')
        for obj_type, schema_name, obj_name in alter_statements:
            full_name = f"{db}.{schema_name}.{obj_name}"
            logger.info(f"🔄 Processing {obj_type}: {full_name}")
            ddl = get_current_ddl(conn, obj_type, full_name)
            if not ddl:
                logger.warning(f"⚠️ Skipping {full_name} - could not retrieve DDL")
                continue
            target_file = find_object_file(schema_path, obj_name, obj_type)
            if dry_run:
                logger.info(f"🔍 [DRY RUN] Would update: {target_file}")
                continue
            ddl_content = ddl.strip() + "\n"
            target_file.write_text(ddl_content, encoding='utf-8')
            commit_message = f"chore: refresh {obj_type.lower()} DDL for {full_name}"
            git_add_commit_push(target_file, commit_message, dry_run)
    except Exception as e:
        logger.error(f"❌ Error updating object file {changed_file}: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Refresh object-level DDL in Git from Snowflake after ALTER statements"
    )
    parser.add_argument('--snowflake-root', required=True)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("🚀 Starting DDL synchronization process")
    try:
        changed_files = find_changed_sql_files(args.snowflake_root)
        if not changed_files:
            logger.info("✅ No ALTER scripts detected; exiting.")
            return
        logger.info(f"🔍 Detected {len(changed_files)} files with ALTER statements")
        conn, key_path = get_snowflake_connection()
        try:
            for changed_file in changed_files:
                logger.info(f"🔄 Processing: {changed_file}")
                update_object_file(changed_file.parent, changed_file, conn, args.dry_run)
        finally:
            conn.close()
            os.remove(key_path)
            logger.info("🔒 Cleaned up Snowflake connection")
        logger.info("✅ DDL synchronization completed successfully")
    except Exception as e:
        logger.error(f"❌ DDL synchronization failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
