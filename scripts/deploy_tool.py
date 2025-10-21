#!/usr/bin/env python3
"""
scripts/deploy_tool.py

Usage examples:
  Interactive:
    python scripts/deploy_tool.py --mode initial_setup --schemas hr
  Non-interactive (CI):
    python scripts/deploy_tool.py --mode initial_setup --schemas hr --non-interactive
    python scripts/deploy_tool.py --mode deploy --files deploy/JIRA123.sql --non-interactive
    python scripts/deploy_tool.py --mode deploy --schemas hr --object-types Views,Functions --non-interactive

Environment variables used (exact names):
  SNOWFLAKE_ACCOUNT
  SNOWFLAKE_USER
  SNOWFLAKE_PRIVATE_KEY
  SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (optional)
  SNOWFLAKE_ROLE (optional)
  SNOWFLAKE_WAREHOUSE (optional)
  SNOWFLAKE_DATABASE (optional)
"""
import os
import sys
import re
import argparse
import subprocess
import tempfile
import shlex
import shutil
from pathlib import Path

# ---------- repository root detection ----------
def find_repo_root():
    """
    Walk up from this script until we find a folder that contains a repo marker:
    - a top-level 'snowflake' folder, or 'deploy' folder, or a .git folder.
    Falls back to the parent of the script folder if nothing found.
    """
    cur = Path(__file__).resolve().parent
    for anc in [cur] + list(cur.parents):
        if (anc / "snowflake").exists() or (anc / "deploy").exists() or (anc / ".git").exists():
            return anc
    return cur.parent

REPO_ROOT = find_repo_root()
SNOWFLAKE_DIR = REPO_ROOT / "snowflake"
DEPLOY_DIR = REPO_ROOT / "deploy"
ROLLBACK_DIR = REPO_ROOT / "rollback"

# ---------- configuration ----------
OBJECT_ORDER = [
    "tables",
    "views",
    "sequences",
    "functions",
    "stored_procedures",
    "procedures",
    "file_formats"
]

# ---------- utilities ----------
def normalize_name(name: str) -> str:
    """Normalize folder or token names: 'Stored Procedures' -> 'stored_procedures'"""
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r'[^0-9a-z]+', '_', n)
    n = n.strip('_')
    return n

def ensure_env_vars():
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    if not account or not user:
        print("ERROR: SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set.", file=sys.stderr)
        sys.exit(2)
    return account, user

def write_private_key():
    key_text = os.environ.get("SNOWFLAKE_PRIVATE_KEY")
    if not key_text:
        print("ERROR: SNOWFLAKE_PRIVATE_KEY not set.", file=sys.stderr)
        sys.exit(2)
    tf = tempfile.NamedTemporaryFile(delete=False, prefix="snowkey_", suffix=".p8", mode="w", encoding="utf-8")
    tf.write(key_text)
    tf.flush()
    tf.close()
    try:
        os.chmod(tf.name, 0o600)
    except Exception:
        pass
    return Path(tf.name)

def choose_client(preferred=None):
    """Return the first available client binary (preferred, snowsql, snow) found in PATH, or None."""
    for c in (preferred, "snowsql", "snow"):
        if not c:
            continue
        if shutil.which(c):
            return c
    return None

def make_wrapper_sql(original_path: Path, role: str, warehouse: str, database: str):
    """
    Create a temporary SQL file that prefixes USE ROLE/WAREHOUSE/DATABASE statements.
    Returns (path_to_file, created_bool).
    """
    header_lines = []
    if role:
        header_lines.append(f"USE ROLE {role};")
    if warehouse:
        header_lines.append(f"USE WAREHOUSE {warehouse};")
    if database:
        header_lines.append(f"USE DATABASE {database};")
    if not header_lines:
        return original_path, False
    tf = tempfile.NamedTemporaryFile(delete=False, prefix="wrap_", suffix=".sql", mode="w", encoding="utf-8")
    tf.write("\n".join(header_lines) + "\n\n")
    with open(original_path, "r", encoding="utf-8") as rf:
        tf.write(rf.read())
    tf.flush()
    tf.close()
    return Path(tf.name), True

def run_sql_file(client_bin: str, account: str, user: str, keypath: Path, sqlfile: Path):
    """
    Execute a single SQL file using the detected client (snowsql or snow).
    Uses key-pair auth and injects PRIVATE_KEY_PASSPHRASE env var if present.
    """
    role = os.environ.get("SNOWFLAKE_ROLE")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    wrapper_path, created = make_wrapper_sql(sqlfile, role, warehouse, database)

    if client_bin and client_bin.endswith("snowsql"):
        cmd = [
            client_bin,
            "-a", account,
            "-u", user,
            "--authenticator", "SNOWFLAKE_JWT",
            "--private-key-path", str(keypath),
            "-f", str(wrapper_path),
            "-o", "exit_on_error=true"
        ]
    elif client_bin and client_bin.endswith("snow"):
        # the snow CLI command may vary by version; this is common form
        cmd = [
            client_bin, "sql", "execute",
            "--account", account,
            "--username", user,
            "--private-key-path", str(keypath),
            "--file", str(wrapper_path),
            "--exit-on-error"
        ]
    else:
        if created:
            try:
                os.remove(wrapper_path)
            except Exception:
                pass
        print("ERROR: No Snow client found (snowsql or snow). Install one or set --snowsql path.", file=sys.stderr)
        sys.exit(10)

    print("Running:", " ".join(shlex.quote(c) for c in cmd))
    env = os.environ.copy()
    if passphrase:
        env["PRIVATE_KEY_PASSPHRASE"] = passphrase
    proc = subprocess.run(cmd, env=env)
    if created:
        try:
            os.remove(wrapper_path)
        except Exception:
            pass
    if proc.returncode != 0:
        print(f"ERROR: execution failed for {sqlfile} (rc={proc.returncode})", file=sys.stderr)
        sys.exit(proc.returncode)

# ---------- file discovery helpers ----------
def resolve_files_arg(files_arg: str):
    """Resolve user-supplied comma-separated file list to Path objects (repo-relative or absolute)."""
    if not files_arg:
        return []
    out = []
    for raw in [x.strip() for x in files_arg.split(",") if x.strip()]:
        p = Path(raw)
        if not p.is_absolute():
            cand = (REPO_ROOT / raw).resolve()
            if cand.exists():
                p = cand
            else:
                cand2 = DEPLOY_DIR / raw
                cand3 = SNOWFLAKE_DIR / raw
                if cand2.exists():
                    p = cand2
                elif cand3.exists():
                    p = cand3
                else:
                    matches = list((REPO_ROOT).glob(raw))
                    if matches:
                        p = matches[0]
                    else:
                        print(f"ERROR: file {raw} not found.", file=sys.stderr)
                        sys.exit(3)
        if not p.exists():
            print(f"ERROR: resolved path {p} does not exist.", file=sys.stderr)
            sys.exit(3)
        out.append(p)
    return out

def find_jira_files():
    if not DEPLOY_DIR.exists():
        return []
    return sorted(DEPLOY_DIR.glob("jira*.sql")) + sorted(DEPLOY_DIR.glob("JIRA*.sql"))

def find_explicit_setup_files_for_schema(schema_folder: Path):
    files = []
    for p in schema_folder.rglob("*.sql"):
        name = p.name.lower()
        if "setup" in name or name.startswith("v001") or "full" in name:
            files.append(p)
    return sorted(files, key=lambda p: str(p).lower())

def find_ordered_objects_for_schema(schema_folder: Path):
    """Collect files following OBJECT_ORDER using case-insensitive tolerant matching."""
    candidates = []
    if not schema_folder.exists():
        return candidates

    # map normalized child folder name -> Path
    children_map = {}
    for child in [c for c in schema_folder.iterdir() if c.is_dir()]:
        children_map[normalize_name(child.name)] = child

    # Collect in order
    for obj in OBJECT_ORDER:
        norm_obj = normalize_name(obj)
        # find child folders matching this object token
        matches = [p for key, p in children_map.items() if (key == norm_obj or norm_obj in key or key in norm_obj)]
        for match_folder in matches:
            for p in sorted(match_folder.rglob("*.sql"), key=lambda x: str(x).lower()):
                if "/backup/" in str(p).replace("\\", "/"):
                    continue
                if p not in candidates:
                    candidates.append(p)

    # Add remaining .sql files not yet included (maintain deterministic order)
    for p in sorted(schema_folder.rglob("*.sql"), key=lambda x: str(x).lower()):
        if "/backup/" in str(p).replace("\\", "/"):
            continue
        if p not in candidates:
            candidates.append(p)

    return candidates

def find_snowflake_objects(schemas=None, object_types=None):
    """Search snowflake/<schema>/ subtree for matching object types (case-insensitive)."""
    found = []
    if not SNOWFLAKE_DIR.exists():
        return found

    schema_list = schemas or [p.name for p in SNOWFLAKE_DIR.iterdir() if p.is_dir()]
    obj_tokens = [normalize_name(t) for t in object_types] if object_types else ["all"]

    for s in schema_list:
        sf = SNOWFLAKE_DIR / s
        # try case-insensitive locate if folder not present exactly
        if not sf.exists():
            norm_target = normalize_name(s)
            matches = [p for p in SNOWFLAKE_DIR.iterdir() if p.is_dir() and normalize_name(p.name) == norm_target]
            if matches:
                sf = matches[0]
        if not sf.exists():
            continue

        if "all" in obj_tokens:
            for p in sorted(sf.rglob("*.sql"), key=lambda x: str(x).lower()):
                if "/backup/" in str(p).replace("\\", "/"):
                    continue
                found.append(p)
            continue

        children_map = {normalize_name(c.name): c for c in [c for c in sf.iterdir() if c.is_dir()]}

        for tok in obj_tokens:
            for child_norm, child_path in children_map.items():
                if child_norm == tok or tok in child_norm or child_norm in tok:
                    for p in sorted(child_path.rglob("*.sql"), key=lambda x: str(x).lower()):
                        if "/backup/" in str(p).replace("\\", "/"):
                            continue
                        if p not in found:
                            found.append(p)

    return sorted(found, key=lambda p: str(p).lower())

# ---------- interactive picker ----------
def interactive_pick(files):
    if not files:
        print("No candidate files.")
        return []
    print("Candidate files:")
    for i, f in enumerate(files, 1):
        try:
            rel = f.relative_to(REPO_ROOT)
        except Exception:
            rel = f
        print(f" {i:3d}) {rel}")
    s = input("Enter comma-separated indexes or 'all': ").strip()
    if not s:
        print("No selection; exiting.")
        sys.exit(0)
    if s.lower() == "all":
        return files
    picks = []
    for part in s.split(","):
        try:
            idx = int(part.strip())
            picks.append(files[idx - 1])
        except Exception:
            pass
    return picks

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["initial_setup", "deploy", "rollback"])
    parser.add_argument("--files", help="comma-separated files (repo-relative or absolute)", default="")
    parser.add_argument("--schemas", help="comma-separated schema names (for searching snowflake/)", default="")
    parser.add_argument("--object-types", help="comma-separated object types (views,functions,procedures,tables,all)", default="all")
    parser.add_argument("--snowsql", help="preferred client binary (path or name).", default=None)
    parser.add_argument("--non-interactive", action="store_true", help="Don't prompt; run all candidates.")
    args = parser.parse_args()

    if not args.mode:
        print("Pick mode: 1) initial_setup  2) deploy  3) rollback")
        choice = input("Choice (1/2/3): ").strip()
        args.mode = {"1": "initial_setup", "2": "deploy", "3": "rollback"}.get(choice)
        if not args.mode:
            print("Invalid selection")
            sys.exit(1)

    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    if not account or not user:
        print("ERROR: SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set.", file=sys.stderr)
        sys.exit(2)

    # ensure the snowflake folder exists when needed and give friendly message
    if not SNOWFLAKE_DIR.exists():
        # if only deploy mode and files provided explicitly, that's ok
        if args.mode == "deploy" and args.files:
            pass
        else:
            # warn but continue — discovery functions handle empty dirs gracefully
            print(f"Warning: expected top-level folder not found: {SNOWFLAKE_DIR} (repo root: {REPO_ROOT})", file=sys.stderr)

    keyfile = write_private_key()
    client = choose_client(args.snowsql)
    try:
        files_to_run = []

        if args.mode == "initial_setup":
            schemas = [s.strip() for s in args.schemas.split(",") if s.strip()] if args.schemas else None
            candidate_files = []

            # build schema folder list (case-insensitive)
            schema_folders = []
            if schemas:
                for s in schemas:
                    sf = SNOWFLAKE_DIR / s
                    if not sf.exists():
                        norm_target = normalize_name(s)
                        matches = [p for p in SNOWFLAKE_DIR.iterdir() if p.is_dir() and normalize_name(p.name) == norm_target]
                        if matches:
                            sf = matches[0]
                    if sf.exists() and sf.is_dir():
                        schema_folders.append(sf)
                    else:
                        print(f"Warning: schema folder {s} not found, skipping.", file=sys.stderr)
            else:
                if SNOWFLAKE_DIR.exists():
                    schema_folders = [p for p in SNOWFLAKE_DIR.iterdir() if p.is_dir()]
                else:
                    schema_folders = []

            for schema_folder in schema_folders:
                explicit = find_explicit_setup_files_for_schema(schema_folder)
                if explicit:
                    candidate_files += explicit
                else:
                    candidate_files += find_ordered_objects_for_schema(schema_folder)

            if not candidate_files:
                print("No initial setup files.")
                sys.exit(0)

            files_to_run = candidate_files if args.non_interactive else interactive_pick(candidate_files)

        elif args.mode == "deploy":
            if args.files:
                files_to_run = resolve_files_arg(args.files)
            else:
                candidates = []
                candidates += find_jira_files()
                obj_types_raw = [o.strip() for o in args.object_types.split(",")] if args.object_types else ["all"]
                obj_types = [normalize_name(o) for o in obj_types_raw]
                schemas = [s.strip() for s in args.schemas.split(",") if s.strip()] if args.schemas else None
                candidates += find_snowflake_objects(schemas=schemas, object_types=obj_types)
                candidates = sorted(set(candidates), key=lambda p: str(p).lower())
                if not candidates:
                    print("No deploy candidates found")
                    sys.exit(0)
                files_to_run = candidates if args.non_interactive else interactive_pick(candidates)

        elif args.mode == "rollback":
            if args.files:
                files_to_run = resolve_files_arg(args.files)
            else:
                cand = sorted(ROLLBACK_DIR.glob("*.sql")) if ROLLBACK_DIR.exists() else []
                if not cand:
                    print("No rollback scripts found.")
                    sys.exit(0)
                files_to_run = cand if args.non_interactive else interactive_pick(cand)

        # final safety: if no files selected, exit
        if not files_to_run:
            print("No files selected; exiting.")
            sys.exit(0)

        # Execute each file in order
        for f in files_to_run:
            run_sql_file(client, account, user, keyfile, f)

        print("Completed.")
    finally:
        try:
            if keyfile and keyfile.exists():
                keyfile.unlink()
        except Exception:
            pass

if __name__ == "__main__":
    main()
