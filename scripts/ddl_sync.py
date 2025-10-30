#!/usr/bin/env python3
"""
scripts/ddl_sync.py

Behavior:
 - By default (no --execute) the script will NOT execute SQL files; it only scans them for ALTER statements
   and then calls GET_DDL to refresh files under snowflake/.
 - If you pass --execute, it will first execute the SQL statements (same as before), then refresh DDL.
 - Accepts both db.schema.object and schema.object and unqualified object names (with optional SNOWFLAKE_SCHEMA env).
 - No backup copies (overwrites target files). Commits per-file using GIT_PUSH_TOKEN.

Usage examples:
  # default: only parse and refresh DDL (safe after your deploy step)
  python scripts/ddl_sync.py --inputs "deploy/5678-project2.sql" --snowflake-root snowflake

  # execute the SQL file(s) first, then refresh
  python scripts/ddl_sync.py --inputs "deploy/5678-project2.sql" --snowflake-root snowflake --execute

  # dry run
  python scripts/ddl_sync.py --inputs "deploy/" --snowflake-root snowflake --dry-run
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

# ---------- logging ----------
def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ---------- Snowflake connection ----------
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
        logger.info("✅ Connected to Snowflake")
        return conn, key_path
    except Exception as e:
        os.remove(key_path)
        raise RuntimeError(f"❌ Failed to connect to Snowflake: {e}")

# ---------- helpers ----------
def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r'[^0-9a-z]+', '_', n)
    return n.strip('_')

def configure_git_credentials():
    name = os.getenv('GIT_USER_NAME', 'DDL Sync Bot')
    email = os.getenv('GIT_USER_EMAIL', 'ddl-sync@noreply.github.com')
    token = os.getenv('GIT_PUSH_TOKEN')  # expected token name

    if not token:
        raise RuntimeError("❌ No authentication token found. Set GIT_PUSH_TOKEN")

    try:
        subprocess.run(["git", "config", "--local", "user.name", name], check=True)
        subprocess.run(["git", "config", "--local", "user.email", email], check=True)

        repo = os.getenv('GITHUB_REPOSITORY')
        if repo:
            auth_url = f"https://{token}@github.com/{repo}.git"
        else:
            url = subprocess.check_output(["git", "config", "--get", "remote.origin.url"], text=True).strip()
            if 'github.com' in url:
                if url.startswith('https://'):
                    if '@github.com' in url:
                        url = re.sub(r'https://[^@]+@github.com/', 'https://github.com/', url)
                    auth_url = url.replace('https://github.com/', f'https://{token}@github.com/')
                else:
                    repo_path = url.split(':',1)[1].replace('.git','')
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
        logger.info(f"🔍 [DRY RUN] Would commit and push: {file_path.name}")
        return

    if not has_changes_to_commit(file_path):
        logger.info(f"ℹ️ No changes detected in {file_path.name}")
        return

    try:
        configure_git_credentials()
        subprocess.run(["git", "add", str(file_path)], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--exit-code"], capture_output=True)
        if result.returncode == 0:
            logger.info(f"ℹ️ No staged changes for {file_path.name}")
            return
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)
        logger.info(f"✅ Successfully pushed updated DDL for {file_path.name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Git operation failed for {file_path.name}: {e}")

# ---------- find file to update ----------
def find_object_file(schema_path: Path, object_name: str, object_type: str) -> Path:
    object_name_lower = object_name.lower()
    candidates = []
    candidates += list(schema_path.glob(f"*__{object_name_lower}_table.sql"))
    candidates += list(schema_path.glob(f"*__{object_name_lower}_{object_type.lower()}.sql"))
    if not candidates:
        candidates += list(schema_path.glob(f"*__{object_name_lower}.sql"))
    if not candidates:
        candidates += [f for f in schema_path.glob(f"*{object_name_lower}*.sql") if re.search(rf"__{object_name_lower}([_.]|$)", f.name)]
    if not candidates:
        candidates += list(schema_path.glob(f"*{object_name_lower}*.sql"))
    if candidates:
        chosen = sorted(candidates, key=lambda p: len(p.name))[0]
        logger.info(f"✅ Will update DDL file: {chosen}")
        return chosen
    else:
        new_file = schema_path / f"{object_type.lower()}__{object_name_lower}.sql"
        logger.info(f"ℹ️ Will create new file: {new_file}")
        return new_file

# ---------- SQL parsing helpers ----------
def split_sql_statements(sql_text: str) -> List[str]:
    parts = re.split(r';\s*(?:\n|$)', sql_text)
    return [p.strip() for p in parts if p.strip()]

def strip_identifier(token: str) -> str:
    token = token.strip()
    if token.startswith('"') and token.endswith('"'):
        return token[1:-1]
    if token.startswith('`') and token.endswith('`'):
        return token[1:-1]
    return token

def extract_alter_statements(sql_content: str) -> List[Tuple[str, Optional[str], Optional[str], str]]:
    """
    Return list of tuples: (OBJECT_TYPE, opt_db, opt_schema, OBJECT_NAME)
    Accepts:
      - db.schema.object
      - schema.object
      - object
    Handles quoted identifiers.
    """
    results = []
    matches = re.findall(r'ALTER\s+(TABLE|VIEW|FUNCTION|PROCEDURE|STAGE|STREAM|TASK|SEQUENCE)\s+([^;,\n]+)', sql_content, flags=re.IGNORECASE)
    for obj_type, target in matches:
        target = target.strip()
        m = re.match(r'([A-Za-z0-9_"`\.]+)', target)
        if not m:
            continue
        id_chunk = m.group(1)
        parts = [strip_identifier(p) for p in id_chunk.split(".")]
        parts = [p for p in parts if p]
        if len(parts) == 3:
            db_name, schema_name, obj_name = parts
            results.append((obj_type.upper(), db_name, schema_name, obj_name))
        elif len(parts) == 2:
            schema_name, obj_name = parts
            results.append((obj_type.upper(), None, schema_name, obj_name))
        elif len(parts) == 1:
            obj_name = parts[0]
            results.append((obj_type.upper(), None, None, obj_name))
        else:
            schema_name, obj_name = parts[-2], parts[-1]
            results.append((obj_type.upper(), None, schema_name, obj_name))
    return results

# ---------- GET_DDL helpers ----------
def quote_ident(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    safe = name.replace('"', '""')
    return f'"{safe}"'

def build_quoted_fullname_candidates(db_env: Optional[str], parsed_db: Optional[str], parsed_schema: Optional[str], obj_name: str) -> List[str]:
    env_db = db_env
    env_schema = os.getenv("SNOWFLAKE_SCHEMA")
    candidates = []

    def q(*parts):
        return ".".join(quote_ident(p) for p in parts if p is not None)

    # 1) parsed db + schema + obj
    if parsed_db and parsed_schema:
        candidates.append(q(parsed_db, parsed_schema, obj_name))
    # 2) parsed schema + obj with env db
    if parsed_schema and env_db:
        candidates.append(q(env_db, parsed_schema, obj_name))
    # 3) parsed_db + env_schema + obj (unlikely but try)
    if parsed_db and env_schema:
        candidates.append(q(parsed_db, env_schema, obj_name))
    # 4) env_db + env_schema + obj (when ALTER used unqualified name)
    if env_db and env_schema:
        candidates.append(q(env_db, env_schema, obj_name))
    # 5) schema.obj (without db) if parsed_schema present
    if parsed_schema:
        candidates.append(q(parsed_schema, obj_name))
    # 6) just object
    candidates.append(q(obj_name))
    # unique preserve order
    seen = set(); uniq = []
    for c in candidates:
        if c not in seen:
            uniq.append(c); seen.add(c)
    return uniq

def get_current_ddl_with_fallback(conn: snowflake.connector.SnowflakeConnection,
                                  obj_type: str,
                                  parsed_db: Optional[str],
                                  parsed_schema: Optional[str],
                                  obj_name: str) -> Optional[str]:
    env_db = os.getenv('SNOWFLAKE_DATABASE')
    candidates = build_quoted_fullname_candidates(env_db, parsed_db, parsed_schema, obj_name)
    last_err = None
    for cname in candidates:
        sql = f"SELECT GET_DDL('{obj_type}', '{cname.replace(\"'\",\"''\")}', TRUE)"
        try:
            with conn.cursor() as cur:
                logger.info(f"➡️ Trying GET_DDL for {obj_type} using identifier: {cname}")
                cur.execute(sql)
                res = cur.fetchone()
                if res and res[0]:
                    logger.info(f"✅ GET_DDL succeeded for identifier: {cname}")
                    return res[0]
                else:
                    logger.debug(f"GET_DDL returned empty for identifier: {cname}")
        except Exception as e:
            last_err = e
            logger.debug(f"GET_DDL attempt failed for {cname}: {e}")
            continue
    if last_err:
        logger.error(f"❌ All GET_DDL attempts failed for {obj_name}. Last error: {last_err}")
    return None

# ---------- path helpers ----------
def gather_input_files(inputs: List[str]) -> List[Path]:
    files: List[Path] = []
    for token in inputs:
        token = token.strip()
        p = Path(token)
        if p.exists():
            if p.is_file():
                files.append(p.resolve())
            elif p.is_dir():
                files.extend(sorted([f.resolve() for f in p.rglob("*.sql")]))
            continue
        repo_root = Path.cwd()
        matches = []
        norm = token.replace("\\","/").lstrip("./").lower()
        for f in repo_root.rglob("*.sql"):
            rel = str(f.relative_to(repo_root)).replace("\\","/").lower()
            if rel == norm or rel.endswith("/"+norm) or norm.endswith("/"+Path(rel).name):
                matches.append(f.resolve())
        if matches:
            files.extend(sorted(matches))
            continue
        globbed = list(Path('.').glob(token))
        for g in globbed:
            if g.is_file():
                files.append(g.resolve())
            elif g.is_dir():
                files.extend(sorted([f.resolve() for f in g.rglob("*.sql")]))
    unique = sorted(list(dict.fromkeys(files)), key=lambda p: str(p).lower())
    return unique

# ---------- execute file and update corresponding DDLs (NO BACKUPS) ----------
def process_sql_file(file_path: Path, conn: snowflake.connector.SnowflakeConnection,
                     snowflake_root: Path, dry_run: bool = False, execute: bool = False):
    logger.info(f"▶ Processing SQL file: {file_path} (execute={execute})")
    try:
        text = file_path.read_text(encoding='utf-8', errors='ignore')
    except Exception as e:
        logger.error(f"❌ Could not read {file_path}: {e}")
        return

    if execute:
        statements = split_sql_statements(text)
        logger.info(f"ℹ️ Found {len(statements)} statements (split by semicolons). Executing...")
        exec_errors = []
        with conn.cursor() as cur:
            for idx, stmt in enumerate(statements, start=1):
                if not stmt:
                    continue
                try:
                    logger.debug(f"Executing statement {idx}: {stmt[:120].replace('\\n',' ')}...")
                    cur.execute(stmt)
                except Exception as e:
                    logger.error(f"❌ Execution failed for statement {idx} in {file_path.name}: {e}")
                    exec_errors.append((idx, str(e)))
        if exec_errors:
            logger.warning(f"⚠️ {len(exec_errors)} statements failed in {file_path.name} (see logs).")
    else:
        logger.info("ℹ️ Skipping execution of SQL file (parse-only mode).")

    alters = extract_alter_statements(text)
    if not alters:
        logger.info(f"ℹ️ No ALTER statements found in {file_path.name}; nothing to refresh.")
        return

    logger.info(f"🔍 Detected {len(alters)} ALTER targets in {file_path.name}")

    for obj_type, parsed_db, parsed_schema, obj_name in alters:
        # if parsed_schema missing, try SNOWFLAKE_SCHEMA env
        schema_to_use = parsed_schema or os.getenv('SNOWFLAKE_SCHEMA')
        if not schema_to_use:
            logger.warning(f"⚠️ ALTER target {obj_name} has no schema qualifier and SNOWFLAKE_SCHEMA not set; skipping.")
            continue

        ddl = get_current_ddl_with_fallback(conn, obj_type, parsed_db, parsed_schema, obj_name)
        if not ddl:
            logger.warning(f"⚠️ Could not retrieve DDL for {obj_type} {parsed_db or ''}.{parsed_schema or ''}.{obj_name}; skipping file update.")
            continue

        schema_name_to_find = parsed_schema or os.getenv('SNOWFLAKE_SCHEMA')
        schema_folder = None
        for cand in sorted(snowflake_root.iterdir(), key=lambda p: p.name.lower()):
            if cand.is_dir() and cand.name.lower() == schema_name_to_find.lower():
                schema_folder = cand
                break
        if not schema_folder:
            norm_target = normalize_name(schema_name_to_find)
            for cand in sorted(snowflake_root.iterdir(), key=lambda p: p.name.lower()):
                if cand.is_dir() and normalize_name(cand.name) == norm_target:
                    schema_folder = cand
                    break
        if not schema_folder:
            logger.warning(f"⚠️ Schema folder for {schema_name_to_find} not found under {snowflake_root}; skipping update for {obj_name}")
            continue

        target_file = find_object_file(schema_folder, obj_name, obj_type)
        if dry_run:
            logger.info(f"🔍 [DRY RUN] Would update {target_file} with DDL for {obj_name}")
            continue

        try:
            target_file.write_text(ddl.strip() + "\n", encoding='utf-8')
            commit_msg = f"chore: refresh {obj_type.lower()} DDL for {obj_name}"
            git_add_commit_push(target_file, commit_msg, dry_run=False)
            logger.info(f"✅ Updated {target_file}")
        except Exception as e:
            logger.error(f"❌ Failed to write {target_file}: {e}")

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Execute deploy/rollback SQL files and refresh altered object DDL into repo (no backups)")
    parser.add_argument('--inputs', required=True, help="Comma-separated files or directories to execute (deploy/ or specific file).")
    parser.add_argument('--snowflake-root', required=True, help="Path to your snowflake/ folder in the repo")
    parser.add_argument('--dry-run', action='store_true', help="Don't write files or push git commits")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    parser.add_argument('--execute', action='store_true', help="Execute SQL before fetching DDL (default: parse-only)")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    inputs = [t for t in args.inputs.split(",") if t.strip()]
    files_to_process = gather_input_files(inputs)
    if not files_to_process:
        logger.error("❌ No SQL files found for provided inputs.")
        sys.exit(2)

    snowflake_root = Path(args.snowflake_root)
    if not snowflake_root.exists() or not snowflake_root.is_dir():
        logger.error(f"❌ Provided snowflake root not found or not a directory: {snowflake_root}")
        sys.exit(2)

    logger.info(f"🚀 Will process {len(files_to_process)} SQL files (dry_run={args.dry_run}, execute={args.execute})")
    conn, key_path = get_snowflake_connection()
    try:
        for f in files_to_process:
            process_sql_file(f, conn, snowflake_root, dry_run=args.dry_run, execute=args.execute)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            if os.path.exists(key_path):
                os.remove(key_path)
        except Exception:
            pass

    logger.info("✅ DDL sync run complete")

if __name__ == "__main__":
    main()
