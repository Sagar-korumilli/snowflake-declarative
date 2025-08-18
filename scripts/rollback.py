#!/usr/bin/env python3
"""
rollback_tables_then_non_table.py

Behavior:
- First pass: revert TABLE/INSERT changes in Snowflake for all files in the PR.
- Second pass: revert non-table repeatable objects (VIEW, MATERIALIZED VIEW, STAGE, FUNCTION, PROCEDURE).
  - For files with a prior version in git, apply previous SQL to Snowflake and stage the previous content.
  - For files without prior git content (but not 'added'), attempt clone+GET_DDL fallback and stage reconstructed DDL.
  - For files that were newly 'added' in the PR -> skip (do nothing).
- After both passes, create a single commit on the PR target branch updating all staged files (unless dry-run).

Usage:
  export GH_TOKEN=...
  export SNOWFLAKE_USER=...
  export SNOWFLAKE_ACCOUNT=...
  export SNOWFLAKE_PRIVATE_KEY='-----BEGIN PRIVATE KEY-----\n...'
  export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='optional'
  export SNOWFLAKE_DATABASE=MYDB

  python rollback_tables_then_non_table.py --repo owner/repo --token "$GH_TOKEN" --pr 123 [--path snowflake/] [--dry-run]
"""
import os
import argparse
import tempfile
import requests
import re
import time
import base64
from datetime import datetime, timezone
import snowflake.connector
from typing import Dict, List, Optional

# -----------------------------
# GitHub helper (PR inspection + git-data commit)
# -----------------------------
class GitHubHelper:
    def __init__(self, repo: str, token: str, path_filter='snowflake/'):
        parts = repo.split('/')
        if len(parts) != 2:
            raise ValueError('repo must be owner/repo')
        self.owner, self.repo = parts
        self.token = token
        self.path_filter = path_filter.rstrip('/') + '/'
        self.base_url = f'https://api.github.com/repos/{self.owner}/{self.repo}'

    def _headers(self, accept: str = 'application/vnd.github.v3+json'):
        return {'Authorization': f'token {self.token}', 'Accept': accept}

    def get_latest_closed_pr_number(self) -> int:
        resp = requests.get(f"{self.base_url}/pulls", headers=self._headers(), params={'state': 'closed', 'sort': 'updated', 'direction': 'desc', 'per_page': 1})
        resp.raise_for_status()
        prs = resp.json()
        if not prs:
            raise Exception('no closed PRs')
        return prs[0]['number']

    def get_pr(self, pr_number: int) -> dict:
        resp = requests.get(f"{self.base_url}/pulls/{pr_number}", headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def get_pr_files(self, pr_number: int) -> List[dict]:
        files = []
        page = 1
        while True:
            resp = requests.get(f"{self.base_url}/pulls/{pr_number}/files", headers=self._headers(), params={'page': page, 'per_page': 100})
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            files.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return files

    def get_file_at_ref_raw(self, path: str, ref: str) -> Optional[str]:
        resp = requests.get(f"{self.base_url}/contents/{path}", headers=self._headers('application/vnd.github.v3.raw'), params={'ref': ref})
        if resp.status_code == 200:
            return resp.text
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return None

    # ---------- Git Data API helpers for single commit with multiple files ----------
    def create_blobs(self, files: Dict[str, str]) -> Dict[str, str]:
        blob_shas = {}
        for path, content in files.items():
            payload = {"content": content, "encoding": "utf-8"}
            resp = requests.post(f"{self.base_url}/git/blobs", headers=self._headers(), json=payload)
            resp.raise_for_status()
            blob_shas[path] = resp.json()['sha']
        return blob_shas

    def get_ref(self, branch: str) -> dict:
        resp = requests.get(f"{self.base_url}/git/refs/heads/{branch}", headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def get_commit(self, commit_sha: str) -> dict:
        resp = requests.get(f"{self.base_url}/git/commits/{commit_sha}", headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def create_tree(self, tree_entries: List[dict], base_tree_sha: str) -> str:
        payload = {"tree": tree_entries, "base_tree": base_tree_sha}
        resp = requests.post(f"{self.base_url}/git/trees", headers=self._headers(), json=payload)
        resp.raise_for_status()
        return resp.json()['sha']

    def create_commit(self, message: str, tree_sha: str, parents: List[str]) -> str:
        payload = {"message": message, "tree": tree_sha, "parents": parents}
        resp = requests.post(f"{self.base_url}/git/commits", headers=self._headers(), json=payload)
        resp.raise_for_status()
        return resp.json()['sha']

    def update_ref(self, branch: str, new_commit_sha: str):
        resp = requests.patch(f"{self.base_url}/git/refs/heads/{branch}", headers=self._headers(), json={"sha": new_commit_sha})
        resp.raise_for_status()
        return resp.json()

    def commit_files_single_commit(self, files: Dict[str, str], branch: str, message: str, dry_run=False) -> bool:
        if dry_run:
            print("[DRY-RUN] Would commit files to branch:", branch)
            for p in files:
                print("[DRY-RUN]  -", p)
            return True

        if not files:
            return True

        ref = self.get_ref(branch)
        base_commit_sha = ref['object']['sha']
        base_commit = self.get_commit(base_commit_sha)
        base_tree_sha = base_commit['tree']['sha']

        blob_shas = self.create_blobs(files)

        tree_entries = []
        for path, blob_sha in blob_shas.items():
            tree_entries.append({
                "path": path,
                "mode": "100644",
                "type": "blob",
                "sha": blob_sha
            })

        new_tree_sha = self.create_tree(tree_entries, base_tree_sha)
        new_commit_sha = self.create_commit(message, new_tree_sha, [base_commit_sha])
        self.update_ref(branch, new_commit_sha)
        print(f"[INFO] Created commit {new_commit_sha} on {branch} updating {len(files)} file(s).")
        return True

# -----------------------------
# Snowflake helpers (private-key auth)
# -----------------------------
def write_temp_key(pem_text: str) -> str:
    fd, path = tempfile.mkstemp(prefix="snow_pk_", suffix=".pem")
    os.write(fd, pem_text.encode('utf-8'))
    os.close(fd)
    os.chmod(path, 0o600)
    return path

def get_snowflake_conn_from_env():
    required = ['SNOWFLAKE_USER', 'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_PRIVATE_KEY']
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing Snowflake env: {missing}")
    key_text = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    key_pwd = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
    keyfile = write_temp_key(key_text)
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'), account=os.getenv('SNOWFLAKE_ACCOUNT'),
        role=os.getenv('SNOWFLAKE_ROLE'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'), private_key_file=keyfile,
        private_key_file_pwd=key_pwd
    )
    return conn, keyfile

def execute_sql_statements(cur, sql_text, dry_run=False):
    sql_text = sql_text.strip()
    if not sql_text:
        return
    if dry_run:
        print('[DRY-RUN] Would execute SQL (head):')
        print(sql_text[:800])
        return
    try:
        cur.execute(sql_text)
    except Exception:
        parts = [s.strip() for s in re.split(r';\s*\n', sql_text) if s.strip()]
        for p in parts:
            try:
                cur.execute(p)
            except Exception as e2:
                print('[ERROR] statement failed:', e2, 'statement head:', p[:200])

def restore_from_clone_getddl(cur, db, sch, name, obj_type, ts, dry_run=False):
    clone_schema = f"{sch}_rb_clone_{int(time.time())}"
    ddl_to_apply = None
    try:
        sql_create = f"CREATE SCHEMA {db}.{clone_schema} CLONE {db}.{sch} AT (TIMESTAMP=>'{ts}');"
        print('[INFO]', sql_create)
        if not dry_run:
            cur.execute(sql_create)
        obj_id_clone = f"{db}.{clone_schema}.{name}"
        try:
            cur.execute(f"SELECT GET_DDL('{obj_type}', '{obj_id_clone}');")
            row = cur.fetchone()
            if row and row[0]:
                ddl = row[0]
                ddl_to_apply = ddl.replace(f"{db}.{clone_schema}", f"{db}.{sch}")
                execute_sql_statements(cur, ddl_to_apply, dry_run=dry_run)
            else:
                print('[WARN] GET_DDL returned nothing for', obj_id_clone)
        except Exception as e:
            print('[WARN] GET_DDL error for', obj_id_clone, e)
    finally:
        try:
            sql_drop = f"DROP SCHEMA IF EXISTS {db}.{clone_schema};"
            print('[INFO]', sql_drop)
            if not dry_run:
                cur.execute(sql_drop)
        except Exception as e:
            print('[WARN] Cleanup drop failed:', e)
    return ddl_to_apply

# -----------------------------
# SQL parsing helpers
# -----------------------------
def parse_non_table_metadata(text: str) -> dict:
    if not text:
        return {}
    m = re.search(r"\b(CREATE|ALTER|DROP)\s+(?:OR\s+REPLACE\s+)?(VIEW|MATERIALIZED\s+VIEW|STAGE|FUNCTION|PROCEDURE)\s+((?:[\w]+\.){0,2}[\w]+)", text, re.IGNORECASE)
    if not m:
        return {}
    obj = m.group(2).upper()
    parts = m.group(3).split('.')
    db = schema = None
    name = parts[-1]
    if len(parts) == 3:
        db, schema, name = parts
    elif len(parts) == 2:
        schema, name = parts
    return {'database': db, 'schema': schema, 'object_type': obj, 'object_name': name}

def parse_table_metadata(text: str) -> dict:
    if not text:
        return {}
    ins = re.search(r"\bINSERT\s+INTO\s+((?:[\w]+\.){0,2}[\w]+)", text, re.IGNORECASE)
    if ins:
        parts = ins.group(1).split('.')
        db = schema = None
        name = parts[-1]
        if len(parts) == 3:
            db, schema, name = parts
        elif len(parts) == 2:
            schema, name = parts
        return {'database': db, 'schema': schema, 'object_type': 'INSERT', 'object_name': name}
    m = re.search(r"\b(CREATE|ALTER)\s+(?:OR\s+REPLACE\s+)?(TABLE)\s+((?:[\w]+\.){0,2}[\w]+)", text, re.IGNORECASE)
    if not m:
        return {}
    parts = m.group(3).split('.')
    db = schema = None
    name = parts[-1]
    if len(parts) == 3:
        db, schema, name = parts
    elif len(parts) == 2:
        schema, name = parts
    return {'database': db, 'schema': schema, 'object_type': 'TABLE', 'object_name': name}

# -----------------------------
# Table rollback helpers
# -----------------------------
def get_column_names(cur, db, sch, tbl):
    cur.execute(f"SHOW COLUMNS IN TABLE {db}.{sch}.{tbl};")
    return [r[2].upper() for r in cur.fetchall()]

def clone_table_at_point(cur, db, sch, tbl, ts, tag):
    name = f"{tbl}_backup_{tag}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    sql = f"CREATE TABLE {db}.{sch}.{name} CLONE {db}.{sch}.{tbl} AT (TIMESTAMP=>'{ts}');"
    print(f"[BACKUP] {sql}")
    cur.execute(sql)
    return name

def rollback_data_inplace(cur, db, sch, tbl, ts):
    sql = f"INSERT OVERWRITE INTO {db}.{sch}.{tbl} SELECT * FROM {db}.{sch}.{tbl} AT (TIMESTAMP=>'{ts}');"
    print(f"[ROLLBACK DATA] {sql}")
    cur.execute(sql)

def rollback_added_columns(cur, db, sch, tbl, ts):
    b = clone_table_at_point(cur, db, sch, tbl, ts, 'schema')
    curr = set(get_column_names(cur, db, sch, tbl))
    old = set(get_column_names(cur, db, sch, b))
    for c in curr - old:
        sql = f"ALTER TABLE {db}.{sch}.{tbl} DROP COLUMN {c};"
        print(f"[ROLLBACK SCHEMA] {sql}")
        cur.execute(sql)

def rollback_dropped_columns(cur, db, sch, tbl, ts):
    print("⚠️ Cannot auto-restore dropped columns; backup clone created.")
    clone_table_at_point(cur, db, sch, tbl, ts, 'recover')

# -----------------------------
# Main flow: TABLES first, then NON-TABLES
# -----------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--repo', required=True, help='owner/repo')
    p.add_argument('--token', help='GitHub token (or set GH_TOKEN env var)')
    p.add_argument('--pr', type=int, default=None, help='PR number (default: latest closed PR)')
    p.add_argument('--path', default='snowflake/', help='path filter to SQL files')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    token = args.token or os.getenv('GH_TOKEN') or os.getenv('GITHUB_TOKEN')
    if not token:
        raise SystemExit("GitHub token required via --token or GH_TOKEN env var")

    gh = GitHubHelper(args.repo, token, path_filter=args.path)

    # pick PR
    pr_num = args.pr
    if not pr_num:
        pr_num = gh.get_latest_closed_pr_number()
        print('[INFO] using latest closed PR', pr_num)
    pr = gh.get_pr(pr_num)
    base_sha = pr['base']['sha']
    target_branch = pr['base']['ref']
    head_sha = pr['head']['sha']
    merged_at = pr.get('merged_at')
    merged_ts = None
    if merged_at:
        merged_ts = datetime.strptime(merged_at, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    files = gh.get_pr_files(pr_num)

    # connect to Snowflake
    conn, keyfile = get_snowflake_conn_from_env()
    cur = conn.cursor()

    now_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    allowed_non_table = set(['VIEW', 'MATERIALIZED VIEW', 'STAGE', 'FUNCTION', 'PROCEDURE'])

    # This will hold path->content for files we need to commit back into target branch
    files_to_commit: Dict[str, str] = {}

    # ---------- PASS 1: TABLE ROLLBACK for all files ----------
    print("\n[PASS 1] Table rollback pass (all files)")
    for f in files:
        path = f.get('filename')
        if not path or not path.startswith(args.path):
            continue
        status = f.get('status')
        print(f"[TABLE PASS] {path} status={status}")

        # get HEAD content to inspect table DDL/INSERT
        try:
            head_content = gh.get_file_at_ref_raw(path, head_sha)
        except Exception:
            head_content = None

        table_meta = parse_table_metadata(head_content or '')
        if not table_meta:
            print(f"[TABLE PASS] no table-related SQL found in {path}; skipping table actions.")
            continue

        typ = table_meta.get('object_type').upper()
        db = table_meta.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        sch = table_meta.get('schema') or os.getenv('SNOWFLAKE_SCHEMA') or 'PUBLIC'
        tbl = table_meta.get('object_name')

        if not ((sch and tbl and typ == 'TABLE') or typ == 'INSERT'):
            print(f"[TABLE PASS] not a table/insert for {path}; skipping.")
            continue

        print(f"[TABLE PASS] processing {db}.{sch}.{tbl} ({typ}) change={status}")
        # pre-backup
        try:
            clone_table_at_point(cur, db, sch, tbl, now_ts, 'pre')
        except Exception as e:
            print("[ERROR] pre-backup failed:", e)
            continue

        if not merged_ts:
            print("[WARN] No PR merged timestamp; skipping time-travel based table restores.")
            continue

        try:
            if typ == 'INSERT' or status in ('modified', 'changed', 'updated'):
                rollback_data_inplace(cur, db, sch, tbl, merged_ts)
            elif status == 'added':
                rollback_added_columns(cur, db, sch, tbl, merged_ts)
            elif status == 'removed':
                rollback_dropped_columns(cur, db, sch, tbl, merged_ts)
        except Exception as e:
            print("[ERROR] Table rollback operation failed:", e)

    # ---------- PASS 2: NON-TABLE ROLLBACK for all files ----------
    print("\n[PASS 2] Non-table rollback pass (all files)")
    for f in files:
        path = f.get('filename')
        if not path or not path.startswith(args.path):
            continue
        status = f.get('status')
        print(f"\n[NON-TABLE PASS] {path} status={status}")

        # try to get previous content at base sha (pre-PR)
        prev_content = None
        try:
            prev_content = gh.get_file_at_ref_raw(path, base_sha)
        except Exception as e:
            print('[WARN] could not fetch previous file content for', path, e)
            prev_content = None

        # detect non-table from prev_content or head
        meta_non_table = parse_non_table_metadata(prev_content or '')
        if not meta_non_table:
            try:
                cur_content = gh.get_file_at_ref_raw(path, head_sha)
            except Exception:
                cur_content = None
            meta_non_table = parse_non_table_metadata(cur_content or '')

        if not meta_non_table:
            print(f"[NON-TABLE PASS] no repeatable non-table object detected in {path}; skipping non-table actions.")
            continue

        typ = meta_non_table.get('object_type')
        if typ not in allowed_non_table:
            print(f"[NON-TABLE PASS] object type {typ} not in allowed repeatables for {path}; skipping.")
            continue

        db = meta_non_table.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        sch = meta_non_table.get('schema') or os.getenv('SNOWFLAKE_SCHEMA') or 'PUBLIC'
        name = meta_non_table.get('object_name')

        # If prev_content exists -> apply and stage for commit to target_branch
        if prev_content:
            print(f"[NON-TABLE PASS] applying previous SQL for {sch}.{name} ({typ})")
            try:
                execute_sql_statements(cur, prev_content, dry_run=args.dry_run)
                files_to_commit[path] = prev_content
            except Exception as e:
                print('[ERROR] applying previous blob failed:', e)
                # fallback clone+GET_DDL if merged_ts available
                if merged_ts:
                    print('[FALLBACK] attempting clone+GET_DDL')
                    ddl_applied = restore_from_clone_getddl(cur, db, sch, name, typ, merged_ts, dry_run=args.dry_run)
                    if ddl_applied:
                        files_to_commit[path] = ddl_applied
        else:
            # If file was newly added in PR -> skip (do nothing)
            if status == 'added':
                print(f"[NON-TABLE PASS] {path} was newly added in PR; skipping (per policy).")
                continue
            # else try clone+GET_DDL fallback
            if merged_ts:
                print(f"[NON-TABLE PASS] no prev_content; using clone+GET_DDL for {db}.{sch}.{name} at {merged_ts}")
                try:
                    ddl_applied = restore_from_clone_getddl(cur, db, sch, name, typ, merged_ts, dry_run=args.dry_run)
                    if ddl_applied:
                        files_to_commit[path] = ddl_applied
                    else:
                        print(f"[NON-TABLE PASS] clone+GET_DDL returned nothing for {path}")
                except Exception as e:
                    print('[ERROR] clone fallback failed:', e)
            else:
                print(f"[NON-TABLE PASS] no prev_content and no merged timestamp; manual restore needed for {path}")

    # ---------- Single commit for restored files ----------
    if files_to_commit:
        commit_message = f"Rollback PR #{pr_num}: restore {len(files_to_commit)} file(s) to pre-PR state"
        print(f"\n[INFO] Preparing single commit on branch {target_branch} for {len(files_to_commit)} file(s).")
        if args.dry_run:
            for p, c in files_to_commit.items():
                print(f"[DRY-RUN] Would commit {p} (size {len(c)} bytes)")
        else:
            try:
                gh.commit_files_single_commit(files_to_commit, target_branch, commit_message, dry_run=args.dry_run)
            except Exception as e:
                print("[ERROR] Failed to commit files in single commit:", e)
    else:
        print("\n[INFO] No repository file updates needed (nothing to commit).")

    # cleanup
    cur.close()
    conn.close()
    try:
        os.remove(keyfile)
    except Exception:
        pass

    print("\n[DONE]")

if __name__ == '__main__':
    main()
