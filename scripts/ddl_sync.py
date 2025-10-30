#!/usr/bin/env python3
"""
scripts/ddl_sync.py

Executes SQL files (deploy/rollback) or parses them for ALTER statements,
fetches current DDL from Snowflake using GET_DDL and overwrites the
corresponding file under snowflake/ (no backup copies). Commits & pushes
changes using GIT_PUSH_TOKEN.

Defaults to parse-only (won't execute SQL). Use --execute to run SQL first.

Main improvement over previous: object file resolution now searches *inside*
schema subfolders (e.g. snowflake/hr/Tables) and creates new files inside
the appropriate subfolder when needed.
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
    token = os.getenv('GIT_PUSH_TOKEN')
    if not token:
        raise RuntimeError("❌ No authentication token found. Set GIT_PUSH_TOKEN")

    repo_env = os.getenv('GITHUB_REPOSITORY')
    if repo_env:
        owner_repo = repo_env
    else:
        try:
            url = subprocess.check_output(["git", "config", "--get", "remote.origin.url"], text=True).strip()
            if url.startswith("git@github.com:"):
                owner_repo = url.split(":", 1)[1].replace(".git", "")
            elif "github.com" in url:
                owner_repo = url.split("github.com/")[-1].replace(".git", "")
            else:
                raise RuntimeError(f"Unsupported remote: {url}")
        except Exception as e:
            raise RuntimeError(f"❌ Could not determine repo location for remote origin: {e}")

    auth_url = f"https://{token}@github.com/{owner_repo}.git"
    try:
        subprocess.run(["git", "remote", "set-url", "origin", auth_url], check=True)
        os.environ["GIT_TERMINAL_PROMPT"] = "0"
        logger.info("🔑 Git remote configured with authentication (token-based)")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ Failed to set git remote URL: {e}")

def has_changes_to_commit(file_path: Path) -> bool:
    try:
        result = subprocess.run(["git", "status", "--porcelain", str(file_path)], capture_output=True, text=True, check=True)
        return bool(result.stdout.strip())
    except subprocess.CalledProcessError:
        return False

def current_branch() -> str:
    try:
        b = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True).strip()
        return b
    except Exception:
        return "HEAD"

def git_add_commit_push(file_path: Path, message: str, dry_run: bool = False):
    if dry_run:
        logger.info(f"🔍 [DRY RUN] Would commit and push: {file_path.name}")
        return

    if not has_changes_to_commit(file_path):
        logger.info(f"ℹ️ No changes detected in {file_path.name}")
        return

    try:
        git_user = os.getenv('GIT_USER_NAME', None)
        git_email = os.getenv('GIT_USER_EMAIL', None)
        if git_user:
            subprocess.run(["git", "config", "--local", "user.name", git_user], check=True)
        if git_email:
            subprocess.run(["git", "config", "--local", "user.email", git_email], check=True)

        configure_git_credentials()

        subprocess.run(["git", "add", str(file_path)], check=True)
        diff = subprocess.run(["git", "diff", "--cached", "--name-only"], capture_output=True, text=True)
        if not diff.stdout.strip():
            logger.info(f"ℹ️ Nothing staged to commit for {file_path.name}")
            return
        subprocess.run(["git", "commit", "-m", message], check=True)
        branch = current_branch()
        subprocess.run(["git", "push", "origin", f"HEAD:{branch}"], check=True)
        logger.info(f"✅ Successfully pushed updated DDL for {file_path.name} to {branch}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Git operation failed for {file_path.name}: {e}")

# ---------- object-type -> folder mapping ----------
OBJECT_TYPE_FOLDERS = {
    "TABLE": ["tables"],
    "VIEW": ["views"],
    "FUNCTION": ["functions"],
    "PROCEDURE": ["stored_procedures", "procedures", "stored-procedures", "storedprocedures"],
    "STORED_PROCEDURE": ["stored_procedures", "procedures"],
    "SEQUENCE": ["sequences"],
    "FILE_FORMAT": ["file_formats", "file-formats"],
    "STREAM": ["streams"],
    "TASK": ["tasks"],
    # fallback generic
    "DEFAULT": ["tables", "views", "functions", "stored_procedures"]
}

# ---------- find file to update (searches inside type subfolders first) ----------
def find_object_file(schema_path: Path, object_name: str, object_type: str) -> Path:
    """
    Search order:
      1. preferred type subfolders under schema (case-insensitive), exact & improved heuristics
      2. entire schema folder (all subfolders)
      3. fallback: create new file inside preferred type subfolder (create it if necessary)
    """
    object_name_lower = object_name.lower()
    object_type_key = object_type.upper()
    # choose candidate folders
    preferred_folders = OBJECT_TYPE_FOLDERS.get(object_type_key, OBJECT_TYPE_FOLDERS["DEFAULT"])

    # helper to search a directory's files with heuristics
    def search_files_in(dir_path: Path) -> Optional[Path]:
        files = [p for p in dir_path.rglob("*.sql")]
        # priority A: exact canonical names
        exact_candidates = []
        for p in files:
            n = p.name.lower()
            if n == f"{object_name_lower}_table.sql" or n == f"{object_name_lower}.sql" or n == f"{object_name_lower}_tbl.sql":
                exact_candidates.append(p)
        if exact_candidates:
            return sorted(exact_candidates, key=lambda p: len(p.name))[0]
        # priority B: contains object name and type token
        token = "table" if object_type_key == "TABLE" else object_type_key.lower()
        contains_candidates = []
        for p in files:
            n = p.name.lower()
            if object_name_lower in n and (token in n or f"_{token}" in n or f"-{token}" in n):
                contains_candidates.append(p)
        if contains_candidates:
            return sorted(contains_candidates, key=lambda p: len(p.name))[0]
        # priority C: fuzzy filename containing object name
        fuzzy = [p for p in files if object_name_lower in p.name.lower()]
        if fuzzy:
            return sorted(fuzzy, key=lambda p: len(p.name))[0]
        # priority D: previous __ patterns
        candidates = [p for p in files if re.search(rf"__{re.escape(object_name_lower)}(_|\.|-|$)", p.name.lower())]
        if candidates:
            return sorted(candidates, key=lambda p: len(p.name))[0]
        return None

    # 1) Try preferred subfolders (case-insensitive)
    for pref in preferred_folders:
        # find matching folder names under schema_path
        matched = None
        for child in [c for c in schema_path.iterdir() if c.is_dir()]:
            if normalize_name(child.name) == normalize_name(pref) or pref.lower() in child.name.lower():
                matched = child
                break
        if matched:
            found = search_files_in(matched)
            if found:
                logger.info(f"✅ Found file in preferred folder '{matched}': {found}")
                return found

    # 2) Try any subfolder under schema_path (search all)
    found_any = None
    for child in [c for c in schema_path.iterdir() if c.is_dir()]:
        found = search_files_in(child)
        if found:
            # pick the best (shortest name) among candidates encountered
            if not found_any or len(found.name) < len(found_any.name):
                found_any = found
    if found_any:
        logger.info(f"✅ Found file in schema subfolders: {found_any}")
        return found_any

    # 3) Try searching directly under schema_path (top-level SQL files)
    found_top = search_files_in(schema_path)
    if found_top:
        logger.info(f"✅ Found file in schema root: {found_top}")
        return found_top

    # 4) Fallback: create new file inside preferred folder (first preferred if exists, else create first preferred)
    chosen_folder = None
    for pref in preferred_folders:
        for child in [c for c in schema_path.iterdir() if c.is_dir()]:
            if normalize_name(child.name) == normalize_name(pref) or pref.lower() in child.name.lower():
                chosen_folder = child
                break
        if chosen_folder:
            break
    if not chosen_folder:
        # create first preferred folder under schema_path
        chosen_folder = schema_path / preferred_folders[0]
        try:
            chosen_folder.mkdir(parents=True, exist_ok=True)
            logger.info(f"ℹ️ Created folder for object type under schema: {chosen_folder}")
        except Exception:
            # fallback to schema root
            chosen_folder = schema_path

    token = object_type.lower() if object_type else "table"
    new_file = chosen_folder / f"{token}__{object_name_lower}.sql"
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
    if parsed_db and parsed_schema:
        candidates.append(q(parsed_db, parsed_schema, obj_name))
    if parsed_schema and env_db:
        candidates.append(q(env_db, parsed_schema, obj_name))
    if parsed_db and env_schema:
        candidates.append(q(parsed_db, env_schema, obj_name))
    if env_db and env_schema:
        candidates.append(q(env_db, env_schema, obj_name))
    if parsed_schema:
        candidates.append(q(parsed_schema, obj_name))
    candidates.append(q(obj_name))
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
        escaped_cname = cname.replace("'", "''")
        sql = f"SELECT GET_DDL('{obj_type}', '{escaped_cname}', TRUE)"
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

# ---------- execute file and update corresponding DDLs ----------
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
        schema_to_use = parsed_schema or os.getenv('SNOWFLAKE_SCHEMA')
        if not schema_to_use:
            logger.warning(f"⚠️ ALTER target {obj_name} has no schema qualifier and SNOWFLAKE_SCHEMA not set; skipping.")
            continue

        ddl = get_current_ddl_with_fallback(conn, obj_type, parsed_db, parsed_schema, obj_name)
        if not ddl:
            logger.warning(f"⚠️ Could not retrieve DDL for {obj_type} {parsed_db or ''}.{parsed_schema or ''}.{obj_name}; skipping file update.")
            continue

        # locate schema folder under snowflake_root
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

        # find or create object file (now searches in subfolders like Tables)
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
