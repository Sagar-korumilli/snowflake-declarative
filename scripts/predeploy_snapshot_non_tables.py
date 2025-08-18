#!/usr/bin/env python3
"""
rollback_non_table_only.py

Rollback non-table Snowflake objects (stages, views, materialized views,
functions, procedures) using PR history as the source of truth.

Behavior:
- Inspect a GitHub PR (or the latest closed PR) to find SQL files changed in the
  PR under a path (default `snowflake/`).
- For each changed file that parses as a non-table repeatable (VIEW, MATERIALIZED VIEW,
  STAGE, FUNCTION, PROCEDURE), attempt to restore the *previous file contents* from
  the PR base commit (i.e. the repo state before the PR) and apply that SQL to
  Snowflake.
- If the file had no prior version (it was added in the PR), do nothing.
- If previous content cannot be fetched but the file did exist before,
  fall back to cloning the schema at the PR merge timestamp and
  using GET_DDL to recreate the prior object definition.
- Does NOT touch tables or time-travel logic (table handling should remain in your
  existing script).

Usage:
  export GH_TOKEN=...                         # GitHub token (repo read access)
  export SNOWFLAKE_USER=...
  export SNOWFLAKE_ACCOUNT=...
  export SNOWFLAKE_PRIVATE_KEY='-----BEGIN PRIVATE KEY-----\n...'
  export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='optional'
  export SNOWFLAKE_DATABASE=MYDB

  python rollback_non_table_only.py --repo myorg/myrepo --pr 123

Notes:
- Runner must have network access for GitHub API and Snowflake.
- This script will execute DDL on Snowflake; test in dev first and use --dry-run.

"""

import os
import argparse
import tempfile
import requests
import re
import time
from datetime import datetime, timezone
import snowflake.connector

# -----------------------------
# Simple GitHub PR inspector
# -----------------------------
class GitHubPRInspector:
    def __init__(self, repo, token, path_filter='snowflake/'):
        parts = repo.split('/')
        if len(parts) != 2:
            raise ValueError('repo must be owner/repo')
        self.owner, self.repo = parts
        self.token = token
        self.path_filter = path_filter.rstrip('/') + '/'
        self.base_url = f'https://api.github.com/repos/{self.owner}/{self.repo}'

    def _get(self, endpoint, params=None):
        headers = {
            'Authorization': f'token {self.token}',
            'Accept': 'application/vnd.github.v3.raw+json'
        }
        resp = requests.get(f"{self.base_url}{endpoint}", headers=headers, params=params or {})
        if resp.status_code not in (200, 201):
            raise Exception(f"GitHub API {resp.status_code}: {resp.text}")
        return resp.json()

    def get_latest_closed_pr_number(self):
        prs = self._get('/pulls', {'state': 'closed', 'sort': 'updated', 'direction': 'desc', 'per_page': 1})
        if not prs:
            raise Exception('no closed PRs')
        return prs[0]['number']

    def get_pr(self, pr_number):
        return self._get(f'/pulls/{pr_number}')

    def get_pr_files(self, pr_number):
        files = []
        page = 1
        while True:
            batch = self._get(f'/pulls/{pr_number}/files', {'page': page, 'per_page': 100})
            if not batch:
                break
            files.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return files

    def get_file_at_ref(self, path, ref):
        resp = requests.get(
            f"{self.base_url}/contents/{path}",
            headers={'Authorization': f'token {self.token}', 'Accept': 'application/vnd.github.v3.raw'},
            params={'ref': ref}
        )
        if resp.status_code == 200:
            return resp.text
        return None

# -----------------------------
# Snowflake helpers
# -----------------------------

def write_temp_key(pem_text: str):
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
        print('[DRY-RUN] Would execute SQL:')
        print(sql_text[:1000])
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

# -----------------------------
# SQL parsing helper
# -----------------------------

def parse_sql_metadata(text):
    if not text:
        return {}
    m = re.search(
        r"\b(CREATE|ALTER|DROP)\s+(?:OR\s+REPLACE\s+)?(VIEW|MATERIALIZED\s+VIEW|STAGE|FUNCTION|PROCEDURE)\s+((?:[\w]+\.){0,2}[\w]+)",
        text, re.IGNORECASE
    )
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

# -----------------------------
# Main
# -----------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--repo', required=True, help='owner/repo')
    p.add_argument('--token', required=True, help='GitHub token')
    p.add_argument('--pr', type=int, default=None, help='PR number (default: latest closed PR)')
    p.add_argument('--path', default='snowflake/', help='path filter to SQL files')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    gh = GitHubPRInspector(args.repo, args.token, path_filter=args.path)

    pr_num = args.pr
    if not pr_num:
        pr_num = gh.get_latest_closed_pr_number()
        print('[INFO] using latest closed PR', pr_num)
    pr = gh.get_pr(pr_num)
    base_sha = pr['base']['sha']
    merged_at = pr.get('merged_at')
    merged_ts = None
    if merged_at:
        merged_ts = datetime.strptime(merged_at, '%Y-%m-%dT%H:%M:%SZ').replace(
            tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    files = gh.get_pr_files(pr_num)

    candidate_files = []
    for f in files:
        fname = f['filename']
        if not fname.startswith(args.path):
            continue
        if not fname.lower().endswith('.sql'):
            continue
        candidate_files.append(f)

    if not candidate_files:
        print('[INFO] no SQL files changed in PR under', args.path)
        return

    conn, keyfile = get_snowflake_conn_from_env()
    cur = conn.cursor()

    allowed_types = set(['VIEW', 'MATERIALIZED VIEW', 'STAGE', 'FUNCTION', 'PROCEDURE'])

    for f in candidate_files:
        path = f['filename']
        status = f.get('status')
        print('\n[PROCESS FILE]', path, 'status=', status)

        prev_content = None
        try:
            prev_content = gh.get_file_at_ref(path, base_sha)
        except Exception as e:
            print('[WARN] could not fetch previous file content for', path, e)

        meta = parse_sql_metadata(prev_content or '')
        if not meta:
            try:
                cur_content = gh.get_file_at_ref(path, pr['head']['sha'])
            except Exception:
                cur_content = None
            meta = parse_sql_metadata(cur_content or '')

        if not meta:
            print('[SKIP] could not detect non-table object in', path)
            continue

        typ = meta.get('object_type')
        if typ not in allowed_types:
            print(f"[SKIP] object type {typ} not in allowed repeatables")
            continue

        db = meta.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        sch = meta.get('schema') or os.getenv('SNOWFLAKE_SCHEMA') or 'PUBLIC'
        name = meta.get('object_name')

        if prev_content:
            print(f"[APPLY PREV FILE] applying previous SQL blob for {sch}.{name} ({typ})")
            try:
                execute_sql_statements(cur, prev_content, dry_run=args.dry_run)
            except Exception as e:
                print('[ERROR] applying previous blob failed:', e)
                if merged_ts:
                    print('[FALLBACK] attempting clone+GET_DDL')
                    restore_from_clone_getddl(cur, db, sch, name, typ, merged_ts, dry_run=args.dry_run)
            continue

        if status == "added":
            print(f"[SKIP] {path} was newly added in PR; nothing to rollback.")
            continue

        if merged_ts:
            print(f"[FALLBACK CLONE] no previous blob; using clone+GET_DDL for {db}.{sch}.{name} at {merged_ts}")
            try:
                restore_from_clone_getddl(cur, db, sch, name, typ, merged_ts, dry_run=args.dry_run)
            except Exception as e:
                print('[ERROR] clone fallback failed:', e)
        else:
            print('[WARN] no previous blob and no PR merged timestamp; manual restore needed for', path)

    cur.close()
    conn.close()
    try:
        os.remove(keyfile)
    except Exception:
        pass

    print('\n[DONE]')

if __name__ == '__main__':
    main()
