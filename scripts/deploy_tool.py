#!/usr/bin/env python3

import os
import sys
import re
import argparse
import subprocess
import tempfile
import shlex
import shutil
from pathlib import Path

# ---------- repo root detection ----------
def find_repo_root():
    cur = Path(__file__).resolve().parent
    for anc in [cur] + list(cur.parents):
        if (anc / "snowflake").exists() or (anc / "deploy").exists() or (anc / ".git").exists():
            return anc
    return cur.parent

REPO_ROOT = find_repo_root()
SNOWFLAKE_DIR = REPO_ROOT / "snowflake"
DEPLOY_DIR = REPO_ROOT / "deploy"
ROLLBACK_DIR = REPO_ROOT / "rollback"

OBJECT_ORDER = [
    "tables",
    "views",
    "sequences",
    "functions",
    "stored_procedures",
    "procedures",
    "file_formats"
]

# ---------- helpers ----------
def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r'[^0-9a-z]+', '_', n)
    n = n.strip('_')
    return n

def write_private_key():
    key_text = os.environ.get("SNOWFLAKE_PRIVATE_KEY")
    if not key_text:
        print("ERROR: SNOWFLAKE_PRIVATE_KEY not set.", file=sys.stderr)
        sys.exit(2)
    tf = tempfile.NamedTemporaryFile(delete=False, prefix="snowkey_", suffix=".p8", mode="w", encoding="utf-8")
    tf.write(key_text)
    tf.flush(); tf.close()
    try:
        os.chmod(tf.name, 0o600)
    except Exception:
        pass
    return Path(tf.name)

def choose_client(preferred=None):
    # return full path if available
    if preferred:
        resolved = shutil.which(preferred) or (preferred if Path(preferred).is_file() and os.access(preferred, os.X_OK) else None)
        if resolved:
            return resolved
    for name in ("snowsql", "snow"):
        p = shutil.which(name)
        if p:
            return p
    return None

def make_wrapper_sql(original_path: Path, role: str, warehouse: str, database: str):
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
    tf.flush(); tf.close()
    return Path(tf.name), True

# ---------------- masked printing helper ----------------
def masked_cmd_str(cmd):
   
    out = []
    i = 0
    while i < len(cmd):
        tok = cmd[i]
        if tok in mask_next and i + 1 < len(cmd):
            out.append(tok)
            out.append("<REDACTED>")
            i += 2
            continue
        replaced = tok
        # defensive mask if env values appear as tokens
        for envk in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"):
            v = os.environ.get(envk)
            if v and tok == v:
                replaced = "<REDACTED>"
                break
        out.append(replaced)
        i += 1
    return " ".join(shlex.quote(x) for x in out)

# ---------- core execution (snow/snowsql) ----------
def run_sql_file(client_bin: str, account: str, user: str, keypath: Path, sqlfile: Path):
  
    role = os.environ.get("SNOWFLAKE_ROLE")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    schema = os.environ.get("SNOWFLAKE_SCHEMA")
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    if not client_bin:
        print("ERROR: No Snow client found in PATH (snowsql or snow). Install one or set --snowsql.", file=sys.stderr)
        sys.exit(10)

    client_name = Path(client_bin).name.lower()

    # SNOW CLI path: use connection flags directly (no wrapper)
    if "snow" == client_name or client_name.startswith("snow"):
        cmd = [
            client_bin, "sql", "-x",
            "--account", account,
            "--username", user,
            "--authenticator", "SNOWFLAKE_JWT",
            "--private-key-file", str(keypath),
            "--filename", str(sqlfile),
        ]
        if role:
            cmd += ["--role", role]
        if warehouse:
            cmd += ["--warehouse", warehouse]
        if database:
            cmd += ["--database", database]
        if schema:
            cmd += ["--schema", schema]
    else:
        wrapper_path, created = make_wrapper_sql(sqlfile, role, warehouse, database)
        cmd = [
            client_bin,
            "-a", account,
            "-u", user,
            "--authenticator", "SNOWFLAKE_JWT",
            "--private-key-path", str(keypath),
            "-f", str(wrapper_path),
            "-o", "exit_on_error=true"
        ]

    # masked print to avoid leaking secrets
    try:
        print("Running:", masked_cmd_str(cmd))
    except Exception:
        print("Running:", " ".join(shlex.quote(c) for c in cmd))

    env = os.environ.copy()
    if passphrase:
        env["PRIVATE_KEY_PASSPHRASE"] = passphrase

    proc = subprocess.run(cmd, env=env)

    # cleanup wrapper if created (snowsql branch)
    if not (("snow" == client_name or client_name.startswith("snow"))):
        try:
            if wrapper_path and wrapper_path.exists():
                wrapper_path.unlink()
        except Exception:
            pass

    if proc.returncode != 0:
        print(f"ERROR: execution failed for {sqlfile} (rc={proc.returncode})", file=sys.stderr)
        if "snow" in client_name:
            print("Hint: 'snow' failed. If you still see problems, paste the 'Running:' line and the full CLI error here and I'll adapt.", file=sys.stderr)
        sys.exit(proc.returncode)

# ---------- file discovery helpers (case-insensitive resolve_files_arg) ----------
def resolve_files_arg(files_arg: str):
    """
    Resolve user-supplied comma-separated file list to Path objects (repo-relative or absolute),
    with case-insensitive matching fallback.
    """
    if not files_arg:
        return []
    out = []

    # build a map of repo-relative lowercased paths -> actual Path
    repo_files_map = {}
    for p in sorted(REPO_ROOT.rglob("*")):
        if p.is_file():
            try:
                rel = str(p.relative_to(REPO_ROOT)).replace("\\", "/")
            except Exception:
                rel = str(p)
            repo_files_map[rel.lower()] = p

    for raw in [x.strip() for x in files_arg.split(",") if x.strip()]:
        p = Path(raw)
        resolved_path = None

        # 1) exact absolute path
        if p.is_absolute() and p.exists():
            resolved_path = p.resolve()
        else:
            # 2) check REPO_ROOT / raw (case-sensitive)
            cand = (REPO_ROOT / raw)
            if cand.exists():
                resolved_path = cand.resolve()

        if resolved_path is None:
            # 3) try case-insensitive match against repo-relative paths
            norm = str(raw).replace("\\", "/").lstrip("./").lower()
            if norm in repo_files_map:
                resolved_path = repo_files_map[norm]

        if resolved_path is None:
            # 4) match by filename only (case-insensitive)
            basename = Path(raw).name.lower()
            matches = [p for rel, p in repo_files_map.items() if Path(rel).name.lower() == basename]
            if len(matches) == 1:
                resolved_path = matches[0]
            elif len(matches) > 1:
                matches = sorted(matches, key=lambda x: str(x).lower())
                resolved_path = matches[0]

        if resolved_path is None:
            # helpful error showing partial matches
            norm = str(raw).replace("\\", "/").lstrip("./").lower()
            partial_matches = [p for rel, p in repo_files_map.items() if norm in rel]
            sample = "\n".join("  " + str(p) for p in (partial_matches[:10] if partial_matches else []))
            print(f"ERROR: file {raw} not found.", file=sys.stderr)
            if sample:
                print("Partial matches (similar paths) found:", file=sys.stderr)
                print(sample, file=sys.stderr)
            else:
                print(f"No similar files found under repo root {REPO_ROOT}", file=sys.stderr)
            sys.exit(3)

        out.append(resolved_path.resolve())
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
    candidates = []
    if not schema_folder.exists():
        return candidates
    children_map = {}
    for child in [c for c in schema_folder.iterdir() if c.is_dir()]:
        children_map[normalize_name(child.name)] = child
    for obj in OBJECT_ORDER:
        norm_obj = normalize_name(obj)
        matches = [p for key, p in children_map.items() if (key == norm_obj or norm_obj in key or key in norm_obj)]
        for match_folder in matches:
            for p in sorted(match_folder.rglob("*.sql"), key=lambda x: str(x).lower()):
                if "/backup/" in str(p).replace("\\", "/"):
                    continue
                if p not in candidates:
                    candidates.append(p)
    for p in sorted(schema_folder.rglob("*.sql"), key=lambda x: str(x).lower()):
        if "/backup/" in str(p).replace("\\", "/"):
            continue
        if p not in candidates:
            candidates.append(p)
    return candidates

def find_snowflake_objects(schemas=None, object_types=None):
    found = []
    if not SNOWFLAKE_DIR.exists():
        return found
    schema_list = schemas or [p.name for p in SNOWFLAKE_DIR.iterdir() if p.is_dir()]
    obj_tokens = [normalize_name(t) for t in object_types] if object_types else ["all"]
    for s in schema_list:
        sf = SNOWFLAKE_DIR / s
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
            idx = int(part.strip()); picks.append(files[idx - 1])
        except Exception:
            pass
    return picks

# ---------- schema input normalization helper ----------
def normalize_schema_tokens(raw_schema_arg: str):
    """
    Accept a comma-separated string of schemas or schema paths.
    Returns a list of schema folder names (basename), or None if no input.
    """
    if not raw_schema_arg:
        return None
    out = []
    for token in [t.strip() for t in raw_schema_arg.split(",") if t.strip()]:
        p = Path(token)
        if p.exists():
            try:
                rel = p.resolve().relative_to(SNOWFLAKE_DIR.resolve())
                if len(rel.parts) >= 1:
                    out.append(rel.parts[0])
                    continue
            except Exception:
                out.append(p.name)
                continue
        if "/" in token or "\\" in token:
            out.append(Path(token).name)
        else:
            out.append(token)
    return out or None

# ---------- git helpers for rollback commit/push ----------
def configure_git_credentials_for_push():
    """
    Configure 'origin' remote URL to include the token so subsequent pushes use the PAT.
    Expects GIT_PUSH_TOKEN to be set (your PERSONAL_ACCESS_TOKEN secret value).
    """
    token = os.environ.get('GIT_PUSH_TOKEN')
    if not token:
        raise RuntimeError("❌ No authentication token found. Set GIT_PUSH_TOKEN")

    # prefer explicitly provided GITHUB_REPOSITORY if present
    repo_env = os.environ.get('GITHUB_REPOSITORY')
    if repo_env:
        owner_repo = repo_env
    else:
        # try to infer from existing remote
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
        subprocess.run(["git", "remote", "set-url", "origin", auth_url], check=True, cwd=REPO_ROOT)
        os.environ["GIT_TERMINAL_PROMPT"] = "0"
        subprocess.run(["git", "config", "--local", "core.autocrlf", "false"], check=False, cwd=REPO_ROOT)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ Failed to set git remote URL: {e}")

def current_branch():
    try:
        b = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True, cwd=REPO_ROOT).strip()
        return b
    except Exception:
        return "HEAD"

def commit_and_push_files(file_paths, message):
    """
    Stages given repo-paths (Path objects or strings relative to REPO_ROOT),
    commits them and pushes a single commit to the current branch.
    """
    if not file_paths:
        return True, None
    # ensure paths are relative to repo root for git
    rels = []
    for p in file_paths:
        if isinstance(p, Path):
            rels.append(str(p.relative_to(REPO_ROOT)))
        else:
            rels.append(str(Path(p).relative_to(REPO_ROOT)))

    # configure git remote to use token
    configure_git_credentials_for_push()

    # set git user if provided
    git_user = os.environ.get('GIT_USER_NAME')
    git_email = os.environ.get('GIT_USER_EMAIL')
    if git_user:
        subprocess.run(["git", "config", "--local", "user.name", git_user], check=False, cwd=REPO_ROOT)
    if git_email:
        subprocess.run(["git", "config", "--local", "user.email", git_email], check=False, cwd=REPO_ROOT)

    try:
        subprocess.run(["git", "add", "--"] + rels, check=True, cwd=REPO_ROOT)
        # check if anything staged
        diff = subprocess.run(["git", "diff", "--cached", "--name-only"], capture_output=True, text=True, cwd=REPO_ROOT)
        if not diff.stdout.strip():
            return True, None
        subprocess.run(["git", "commit", "-m", message], check=True, cwd=REPO_ROOT)
        branch = current_branch()
        subprocess.run(["git", "push", "origin", f"HEAD:{branch}"], check=True, cwd=REPO_ROOT)
        return True, None
    except subprocess.CalledProcessError as e:
        return False, e

# ---------- helper: get previous committed content for a repo file ----------
def get_previous_file_content(repo_path: Path):
    """
    Return previous version of a file as text if available, else None.
    Uses: git rev-list -n 2 HEAD -- <path>  and git show <commit>:<path>
    Requires that the repository has history (fetch-depth: 0).
    """
    try:
        rel = str(repo_path.relative_to(REPO_ROOT)).replace("\\", "/")
    except Exception:
        rel = str(repo_path)
    try:
        out = subprocess.check_output(["git", "rev-list", "-n", "2", "HEAD", "--", rel], cwd=REPO_ROOT, text=True)
        commits = [c.strip() for c in out.splitlines() if c.strip()]
        if len(commits) < 2:
            # no previous version (file may be newly added)
            return None
        prev_commit = commits[1]
        content = subprocess.check_output(["git", "show", f"{prev_commit}:{rel}"], cwd=REPO_ROOT, text=True, errors='ignore')
        return content
    except subprocess.CalledProcessError:
        return None
    except Exception:
        return None

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["initial_setup", "deploy", "rollback"], required=True)
    parser.add_argument("--files", help="comma-separated files (repo-relative or absolute)", default="")
    parser.add_argument("--schemas", help="comma-separated schema names (for searching snowflake/)", default="")
    parser.add_argument("--object-types", help="comma-separated object types (views,functions,procedures,tables,all)", default="all")
    parser.add_argument("--snowsql", help="preferred client binary (path or name).", default=None)
    parser.add_argument("--non-interactive", action="store_true", help="Don't prompt; run all candidates.")
    args = parser.parse_args()

    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    if not account or not user:
        print("ERROR: SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set.", file=sys.stderr)
        sys.exit(2)

    if not SNOWFLAKE_DIR.exists():
        if not (args.mode == "deploy" and args.files):
            print(f"Warning: expected top-level folder not found: {SNOWFLAKE_DIR} (repo root: {REPO_ROOT})", file=sys.stderr)

    keyfile = write_private_key()
    client = choose_client(args.snowsql)
    try:
        files_to_run = []

        if args.mode == "initial_setup":
            schemas = normalize_schema_tokens(args.schemas)
            candidate_files = []
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
                schemas = normalize_schema_tokens(args.schemas)
                candidates += find_snowflake_objects(schemas=schemas, object_types=obj_types)
                candidates = sorted(set(candidates), key=lambda p: str(p).lower())
                if not candidates:
                    print("No deploy candidates found")
                    sys.exit(0)
                files_to_run = candidates if args.non_interactive else interactive_pick(candidates)

        elif args.mode == "rollback":
           
            if not args.files:
                print("No files provided for rollback; skipping rollback.")
                sys.exit(0)

            provided = resolve_files_arg(args.files)
            if not provided:
                print("No rollback files resolved; exiting.")
                sys.exit(0)

            # partition: rollback_sqls (paths under rollback/) vs snowflake_paths
            rollback_sqls = []
            snowflake_paths = []
            for p in provided:
                try:
                    rel = p.relative_to(REPO_ROOT)
                except Exception:
                    rel = p
                rel_str = str(rel).replace("\\", "/")
                if rel_str.startswith("rollback/"):
                    rollback_sqls.append(p)
                elif rel_str.startswith("snowflake/"):
                    snowflake_paths.append(p)
                else:
                    # accept other explicit files as rollback SQLs (execute as-is)
                    rollback_sqls.append(p)

            any_failures = False
            restored_files = []

            # 1) Execute rollback/* SQLs directly
            for f in rollback_sqls:
                if not f.exists():
                    print(f"ERROR: rollback SQL file not found: {f}", file=sys.stderr)
                    any_failures = True
                    continue
                try:
                    print(f"Executing rollback SQL: {f}")
                    run_sql_file(client, account, user, keyfile, f)
                except SystemExit as se:
                    print(f"ERROR: execution failed for rollback SQL {f}: {se}", file=sys.stderr)
                    any_failures = True
                except Exception as e:
                    print(f"ERROR: unexpected error executing rollback SQL {f}: {e}", file=sys.stderr)
                    any_failures = True

            # 2) Process snowflake/* non-table files: restore previous commit, execute, overwrite and collect for commit
            for f in snowflake_paths:
                # skip table folder files
                try:
                    rel = f.relative_to(SNOWFLAKE_DIR)
                    parts = [p.lower() for p in rel.parts]
                    if any("table" in p for p in parts):
                        print(f"Skipping table-related file for rollback (tables are not auto-rolled back): {f}", file=sys.stderr)
                        continue
                except Exception:
                    # not under snowflake, still process as snowflake path if user provided
                    pass

                if not f.exists():
                    print(f"ERROR: file not found: {f}", file=sys.stderr)
                    any_failures = True
                    continue

                prev_content = get_previous_file_content(f)
                if prev_content is None:
                    print(f"INFO: No previous commit version found for {f} (possibly newly added). Skipping.", file=sys.stderr)
                    continue

                # write prev_content to temp file and execute it
                tmp = tempfile.NamedTemporaryFile(delete=False, prefix="rb_prev_", suffix=".sql", mode="w", encoding="utf-8")
                tmp.write(prev_content)
                tmp.flush(); tmp.close()
                tmp_path = Path(tmp.name)
                try:
                    print(f"Executing previous SQL for {f} on Snowflake (temp: {tmp_path})")
                    run_sql_file(client, account, user, keyfile, tmp_path)
                except SystemExit as se:
                    print(f"ERROR: execution failed for previous version of {f}: {se}", file=sys.stderr)
                    any_failures = True
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
                    continue
                except Exception as e:
                    print(f"ERROR: unexpected exception executing previous version of {f}: {e}", file=sys.stderr)
                    any_failures = True
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
                    continue

                # if execution succeeded, overwrite repo file with prev_content
                try:
                    f.write_text(prev_content, encoding='utf-8')
                    restored_files.append(f)
                    print(f"INFO: Overwrote {f} with previous committed content.")
                except Exception as e:
                    print(f"ERROR: failed to write previous content back to {f}: {e}", file=sys.stderr)
                    any_failures = True
                finally:
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass

            # commit & push a single commit if we restored any files
            if restored_files:
                commit_msg = f"rollback: restore previous versions for {len(restored_files)} file(s)"
                print(f"Committing and pushing {len(restored_files)} restored file(s) with message: {commit_msg}")
                try:
                    succ, err = commit_and_push_files(restored_files, commit_msg)
                    if not succ:
                        print(f"ERROR: git commit/push failed: {err}", file=sys.stderr)
                        any_failures = True
                    else:
                        print("INFO: Successfully committed & pushed restored files.")
                except Exception as e:
                    print(f"ERROR: commit_and_push_files failed: {e}", file=sys.stderr)
                    any_failures = True
            else:
                print("No files restored from previous commits; nothing to commit.")

            if any_failures:
                print("Rollback completed with errors.", file=sys.stderr)
                sys.exit(1)
            else:
                print("Rollback completed successfully.")
                sys.exit(0)

        # continuing for initial_setup / deploy modes:
        if not files_to_run:
            print("No files selected; exiting.")
            sys.exit(0)

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
