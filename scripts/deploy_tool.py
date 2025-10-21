#!/usr/bin/env python3
"""
deploy_tool.py - uses these env vars (exact names):
SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY,
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, SNOWFLAKE_ROLE,
SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE

Supports modes: initial_setup, deploy, rollback
"""
import os, sys, argparse, subprocess, tempfile, shlex, shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DEPLOY_DIR = REPO_ROOT / "deploy"
SNOWFLAKE_DIR = REPO_ROOT / "snowflake"
ROLLBACK_DIR = REPO_ROOT / "rollback"

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
    """Create a temporary SQL file that prefixes USE statements to original SQL."""
    header = []
    if role:
        header.append(f"USE ROLE {role};")
    if warehouse:
        header.append(f"USE WAREHOUSE {warehouse};")
    if database:
        header.append(f"USE DATABASE {database};")
    # if no header, return original path to avoid extra file
    if not header:
        return original_path, False
    tf = tempfile.NamedTemporaryFile(delete=False, prefix="wrap_", suffix=".sql", mode="w", encoding="utf-8")
    # write header then original content
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

    # build command (snowsql preferred)
    if client_bin and client_bin.endswith("snowsql"):
        cmd = [client_bin, "-a", account, "-u", user,
               "--authenticator", "SNOWFLAKE_JWT",
               "--private-key-path", keypath,
               "-f", str(wrapper_path),
               "-o", "exit_on_error=true"]
    elif client_bin and client_bin.endswith("snow"):
        # snow CLI: `snow sql execute --file <file>` (some installs may differ)
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

# --- basic file discovery/resolution (kept simple) ---
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
                    print(f"ERROR: file {f} not found.", file=sys.stderr); sys.exit(3)
        if not p.exists():
            print(f"ERROR: resolved path {p} does not exist.", file=sys.stderr); sys.exit(3)
        out.append(p)
    return out

def find_jira_files():
    return sorted(DEPLOY_DIR.glob("jira*.sql")) + sorted(DEPLOY_DIR.glob("JIRA*.sql"))

def find_snowflake_objects(schemas=None, object_types=None):
    candidates = []
    if not SNOWFLAKE_DIR.exists():
        return candidates
    schemas_list = schemas or [p.name for p in SNOWFLAKE_DIR.iterdir() if p.is_dir()]
    for s in schemas_list:
        sf = SNOWFLAKE_DIR / s
        if not sf.exists(): continue
        if not object_types or "all" in object_types:
            for p in sf.rglob("*.sql"):
                if "/backup/" in str(p).replace("\\","/"): continue
                candidates.append(p)
        else:
            for ot in object_types:
                # common folder names: views, functions, stored procedures, tables
                for sub in sf.rglob(ot):
                    if sub.is_file() and sub.suffix.lower() == ".sql":
                        candidates.append(sub)
                    elif sub.is_dir():
                        for p in sub.rglob("*.sql"):
                            if "/backup/" in str(p).replace("\\","/"): continue
                            candidates.append(p)
    return sorted(set(candidates), key=lambda p: str(p).lower())

# --- main CLI (kept compact) ---
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
        if not args.mode: print("Invalid"); sys.exit(1)

    account, user = ensure_env_vars()
    keyfile = write_private_key()
    client = choose_client(args.snowsql)
    try:
        files_to_run = []
        if args.mode == "initial_setup":
            schemas = [s.strip() for s in args.schemas.split(",")] if args.schemas else None
            candidates = []
            if schemas:
                for s in schemas:
                    p = SNOWFLAKE_DIR / s
                    if not p.exists(): continue
                    for f in p.rglob("*.sql"):
                        n = f.name.lower()
                        if "setup" in n or n.startswith("v001") or "full" in n:
                            candidates.append(f)
            else:
                for p in SNOWFLAKE_DIR.rglob("*.sql"):
                    n = p.name.lower()
                    if "setup" in n or n.startswith("v001") or "full" in n:
                        candidates.append(p)
            if not candidates: print("No initial setup files."); sys.exit(0)
            files_to_run = candidates if args.non_interactive else interactive_pick(candidates)

        elif args.mode == "deploy":
            if args.files:
                files_to_run = resolve_files_arg(args.files)
            else:
                candidates = []
                candidates += find_jira_files() if (DEPLOY_DIR.exists()) else []
                obj_types = [x.strip().lower() for x in args.object_types.split(",")] if args.object_types else ["all"]
                schemas = [s.strip() for s in args.schemas.split(",")] if args.schemas else None
                candidates += find_snowflake_objects(schemas=schemas, object_types=obj_types)
                if not candidates: print("No deploy candidates found"); sys.exit(0)
                files_to_run = candidates if args.non_interactive else interactive_pick(candidates)

        elif args.mode == "rollback":
            if args.files:
                files_to_run = resolve_files_arg(args.files)
            else:
                cand = sorted(ROLLBACK_DIR.glob("*.sql")) if ROLLBACK_DIR.exists() else []
                if not cand: print("No rollback scripts found."); sys.exit(0)
                files_to_run = cand if args.non_interactive else interactive_pick(cand)

        for f in files_to_run:
            run_sql_file(client, account, user, keyfile, f)
        print("Completed.")
    finally:
        try: os.remove(keyfile)
        except Exception: pass

def interactive_pick(files):
    print("Candidate files:")
    for i,f in enumerate(files,1):
        print(f" {i:3d}) {f.relative_to(REPO_ROOT)}")
    s = input("Enter comma-separated indexes or 'all': ").strip()
    if s.lower() == "all": return files
    picks=[]
    for part in s.split(","):
        try:
            idx=int(part.strip()); picks.append(files[idx-1])
        except Exception: pass
    return picks

if __name__ == "__main__":
    main()
