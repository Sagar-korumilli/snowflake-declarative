#!/usr/bin/env python3
"""
deploy_tool.py - case-insensitive folder matching and robust discovery.

Uses these env vars (exact names):
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY,
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, SNOWFLAKE_ROLE,
SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE

Modes: initial_setup, deploy, rollback
"""
import os, sys, argparse, subprocess, tempfile, shlex, shutil, re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DEPLOY_DIR = REPO_ROOT / "deploy"
SNOWFLAKE_DIR = REPO_ROOT / "snowflake"
ROLLBACK_DIR = REPO_ROOT / "rollback"

# fallback object order for initial setup
OBJECT_ORDER = ["tables", "views", "functions", "stored_procedures", "procedures", "sequences", "file_formats"]

def normalize_name(name: str) -> str:
    """Normalize a folder/name like 'Stored Procedures' -> 'stored_procedures' for robust matching."""
    if not name:
        return ""
    # lowercase, replace any sequence of non-alnum with single underscore
    n = name.lower()
    n = re.sub(r'[^0-9a-z]+', '_', n)
    n = n.strip('_')
    return n

# ---------------- env & client helpers ----------------
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
    tf = tempfile.NamedTemporaryFile(delete=False, prefix="snowkey_", suffix=".p8")
    tf.write(key_text.encode())
    tf.flush(); tf.close()
    os.chmod(tf.name, 0o600)
    return tf.name

def choose_client(preferred=None):
    for c in (preferred, "snowsql", "snow"):
        if not c: continue
        if shutil.which(c):
            return c
    return None

def make_wrapper_sql(original_path, role, warehouse, database):
    header = []
    if role:
        header.append(f"USE ROLE {role};")
    if warehouse:
        header.append(f"USE WAREHOUSE {warehouse};")
    if database:
        header.append(f"USE DATABASE {database};")
    if not header:
        return original_path, False
    tf = tempfile.NamedTemporaryFile(delete=False, prefix="wrap_", suffix=".sql", mode="w", encoding="utf-8")
    tf.write("\n".join(header) + "\n\n")
    with open(original_path, "r", encoding="utf-8") as f:
        tf.write(f.read())
    tf.flush(); tf.close()
    return Path(tf.name), True

def run_sql_file(client_bin, account, user, keypath, sqlfile):
    role = os.environ.get("SNOWFLAKE_ROLE")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    wrapper_path, created = make_wrapper_sql(sqlfile, role, warehouse, database)

    if client_bin and client_bin.endswith("snowsql"):
        cmd = [client_bin, "-a", account, "-u", user,
               "--authenticator", "SNOWFLAKE_JWT",
               "--private-key-path", keypath,
               "-f", str(wrapper_path),
               "-o", "exit_on_error=true"]
    elif client_bin and client_bin.endswith("snow"):
        cmd = [client_bin, "sql", "execute", "--account", account, "--username", user,
               "--private-key-path", keypath, "--file", str(wrapper_path), "--exit-on-error"]
    else:
        print("ERROR: No Snow client found (snowsql or snow).", file=sys.stderr)
        if created:
            os.remove(wrapper_path)
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

# ---------------- file discovery ----------------
def resolve_files_arg(files_arg):
    if not files_arg:
        return []
    out = []
    for f in [x.strip() for x in files_arg.split(",") if x.strip()]:
        p = Path(f)
        if not p.is_absolute():
            cand = (REPO_ROOT / f).resolve()
            if cand.exists():
                p = cand
            else:
                cand2 = DEPLOY_DIR / f
                cand3 = SNOWFLAKE_DIR / f
                if cand2.exists(): p = cand2
                elif cand3.exists(): p = cand3
                else:
                    # try glob expansion inside repo root
                    matches = list((REPO_ROOT).glob(f))
                    if matches:
                        p = matches[0]
                    else:
                        print(f"ERROR: file {f} not found.", file=sys.stderr); sys.exit(3)
        if not p.exists():
            print(f"ERROR: resolved path {p} does not exist.", file=sys.stderr); sys.exit(3)
        out.append(p)
    return out

def find_jira_files():
    return sorted(DEPLOY_DIR.glob("jira*.sql")) + sorted(DEPLOY_DIR.glob("JIRA*.sql"))

def find_explicit_setup_files_for_schema(schema_folder: Path):
    files = []
    for p in schema_folder.rglob("*.sql"):
        name = p.name.lower()
        if "setup" in name or name.startswith("v001") or "full" in name:
            files.append(p)
    return sorted(files, key=lambda p: str(p).lower())

def find_ordered_objects_for_schema(schema_folder: Path):
    """
    Collect files in OBJECT_ORDER order, matching folder names case-insensitively and tolerant
    to spaces/dashes/underscores.
    """
    candidates = []
    # map normalized folder name -> Path for direct children only
    children_map = {}
    for child in [c for c in schema_folder.iterdir() if c.is_dir()]:
        children_map[normalize_name(child.name)] = child

    # for each object name in desired order, attempt to find matching folder(s)
    for obj in OBJECT_ORDER:
        norm_obj = normalize_name(obj)
        # find children whose normalized name contains norm_obj or equals it
        matches = [p for key, p in children_map.items() if (key == norm_obj or norm_obj in key or key in norm_obj)]
        for match_folder in matches:
            # gather .sql files from this folder
            for p in sorted(match_folder.rglob("*.sql"), key=lambda x: str(x).lower()):
                if "/backup/" in str(p).replace("\\","/"): continue
                if p not in candidates:
                    candidates.append(p)

    # finally, add any remaining .sql under schema folder not already included
    for p in sorted(schema_folder.rglob("*.sql"), key=lambda x: str(x).lower()):
        if "/backup/" in str(p).replace("\\","/"): continue
        if p not in candidates:
            candidates.append(p)
    return candidates

def find_snowflake_objects(schemas=None, object_types=None):
    """
    Search snowflake/<schema>/ for files matching object_types (case-insensitive).
    object_types: list like ['views','tables'] or ['all'].
    """
    found = []
    if not SNOWFLAKE_DIR.exists():
        return found

    schema_list = schemas or [p.name for p in SNOWFLAKE_DIR.iterdir() if p.is_dir()]
    # normalize requested object type tokens
    obj_tokens = [normalize_name(t) for t in object_types] if object_types else ["all"]

    for s in schema_list:
        sf = SNOWFLAKE_DIR / s
        if not sf.exists(): continue

        # if 'all' requested, include everything
        if "all" in obj_tokens:
            for p in sorted(sf.rglob("*.sql"), key=lambda x: str(x).lower()):
                if "/backup/" in str(p).replace("\\","/"): continue
                found.append(p)
            continue

        # build mapping of child folders normalized name -> Path
        children_map = {normalize_name(c.name): c for c in [c for c in sf.iterdir() if c.is_dir()]}

        # for each requested token, find candidate child folders by normalized matching
        for tok in obj_tokens:
            for child_norm, child_path in children_map.items():
                if child_norm == tok or tok in child_norm or child_norm in tok:
                    # collect .sql files from child_path
                    for p in sorted(child_path.rglob("*.sql"), key=lambda x: str(x).lower()):
                        if "/backup/" in str(p).replace("\\","/"): continue
                        if p not in found:
                            found.append(p)
                else:
                    # sometimes files might be directly under child folder that doesn't match exactly,
                    # but the folder may contain the token in its name (covered by tok in child_norm).
                    pass

    return sorted(found, key=lambda p: str(p).lower())

# ---------------- interactive picker ----------------
def interactive_pick(files):
    if not files:
        print("No candidate files.")
        return []
    print("Candidate files:")
    for i,f in enumerate(files,1):
        try:
            print(f" {i:3d}) {f.relative_to(REPO_ROOT)}")
        except Exception:
            print(f" {i:3d}) {f}")
    s = input("Enter comma-separated indexes or 'all': ").strip()
    if not s:
        print("No selection; exiting."); sys.exit(0)
    if s.lower() == "all":
        return files
    picks=[]
    for part in s.split(","):
        try:
            idx=int(part.strip()); picks.append(files[idx-1])
        except Exception:
            pass
    return picks

# ---------------- main ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["initial_setup","deploy","rollback"])
    parser.add_argument("--files", help="comma-separated files")
    parser.add_argument("--schemas", help="comma-separated schemas (for searching snowflake/)")
    parser.add_argument("--object-types", help="comma-separated object types when searching snowflake/ (views,functions,procedures,tables,all)", default="all")
    parser.add_argument("--snowsql", help="preferred client binary (snowsql or snow)", default=None)
    parser.add_argument("--non-interactive", action="store_true")
    args = parser.parse_args()

    if not args.mode:
        print("Pick mode: 1) initial_setup  2) deploy  3) rollback")
        choice = input("Choice (1/2/3): ").strip()
        args.mode = {"1":"initial_setup","2":"deploy","3":"rollback"}.get(choice)
        if not args.mode:
            print("Invalid selection"); sys.exit(1)

    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    if not account or not user:
        print("ERROR: SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set.", file=sys.stderr); sys.exit(2)

    keyfile = write_private_key()
    client = choose_client(args.snowsql)

    try:
        files_to_run = []
        if args.mode == "initial_setup":
            schemas = [s.strip() for s in args.schemas.split(",")] if args.schemas else None
            candidate_files = []
            # build schema folder list
            schema_folders = []
            if schemas:
                for s in schemas:
                    sf = SNOWFLAKE_DIR / s
                    # try case-insensitive find if sf doesn't exist exactly
                    if not sf.exists():
                        # search for matching folder ignoring case/normalize
                        norm_target = normalize_name(s)
                        matches = [p for p in SNOWFLAKE_DIR.iterdir() if p.is_dir() and normalize_name(p.name) == norm_target]
                        if matches:
                            sf = matches[0]
                    if sf.exists() and sf.is_dir():
                        schema_folders.append(sf)
                    else:
                        print(f"Warning: schema folder {s} not found, skipping.")
            else:
                schema_folders = [p for p in SNOWFLAKE_DIR.iterdir() if p.is_dir()]

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
                if DEPLOY_DIR.exists():
                    candidates += find_jira_files()
                # normalize object-types requested
                obj_types_raw = [o.strip() for o in args.object_types.split(",")] if args.object_types else ["all"]
                obj_types = [normalize_name(o) for o in obj_types_raw]
                schemas = [s.strip() for s in args.schemas.split(",")] if args.schemas else None
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

        for f in files_to_run:
            run_sql_file(client, account, user, keyfile, f)

        print("Completed.")
    finally:
        try:
            os.remove(keyfile)
        except Exception:
            pass

if __name__ == "__main__":
    main()
