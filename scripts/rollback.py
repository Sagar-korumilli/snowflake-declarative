#!/usr/bin/env python3
import argparse
import os
import sys
import tempfile
import base64
import requests
import re
import time
from datetime import datetime, timezone
import snowflake.connector

# -----------------------------
# GitHub helper + repo inspector
# -----------------------------
class GitHubPRAnalyzer:
    def __init__(self, repo, token, branch=None, path_filter='snowflake/'):
        self.owner, self.repo = self.parse_repo_url(repo)
        self.token = token
        self.branch = branch
        self.path_filter = path_filter.rstrip('/') + '/'
        self.base_url = f'https://api.github.com/repos/{self.owner}/{self.repo}'

    def parse_repo_url(self, url):
        if url.startswith('https://github.com/'):
            parts = url.replace('https://github.com/', '').rstrip('.git').split('/')
            return parts[0], parts[1]
        elif '/' in url:
            parts = url.split('/')
            return parts[0], parts[1]
        raise ValueError('Invalid GitHub repo format')

    def make_request(self, endpoint, params=None):
        headers = {'Authorization': f'token {self.token}', 'Accept': 'application/vnd.github.v3+json'}
        resp = requests.get(f"{self.base_url}{endpoint}", headers=headers, params=params or {})
        if resp.status_code not in (200, 201):
            raise Exception(f"GitHub API error {resp.status_code}: {resp.text}")
        return resp.json()

    def get_latest_pr_number(self):
        prs = self.make_request('/pulls', {'state':'closed','sort':'updated','direction':'desc','per_page':1})
        if not prs:
            raise Exception('No pull requests found')
        return prs[0]['number']

    def get_pr_files(self, pr_number):
        files, page = [], 1
        while True:
            batch = self.make_request(f'/pulls/{pr_number}/files', {'page':page,'per_page':100})
            if not batch:
                break
            files.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return files

    def fetch_file_content(self, path, ref=None):
        params = {'ref': ref or self.branch} if (ref or self.branch) else {}
        data = self.make_request(f'/contents/{path}', params)
        content = data.get('content')
        return base64.b64decode(content).decode('utf-8',errors='ignore') if content else None

    def list_dir(self, path, ref=None):
        """List directory contents via GitHub contents API. Returns list of dicts or empty list."""
        try:
            params = {'ref': ref or self.branch} if (ref or self.branch) else {}
            return self.make_request(f'/contents/{path}', params)
        except Exception as e:
            print(f"[GITHUB] list_dir error for {path}: {e}")
            return []

    def get_pr_merged_time(self, pr_number):
        pr = self.make_request(f'/pulls/{pr_number}')
        merged = pr.get('merged_at')
        if not merged:
            raise Exception('PR not merged yet')
        dt = datetime.strptime(merged,'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    def parse_sql_metadata(self, text):
        if not text:
            return {}
        # INSERT detection
        ins = re.search(r"\bINSERT\s+INTO\s+((?:[\w]+\.){0,2}[\w]+)", text, re.IGNORECASE)
        if ins:
            parts = ins.group(1).split('.')
            db = schema = None
            name = parts[-1]
            if len(parts)==3:
                db,schema,name = parts
            elif len(parts)==2:
                schema,name = parts
            return {'database':db,'schema':schema,'object_type':'INSERT','object_name':name}
        # basic detection for common objects
        m = re.search(r"\b(CREATE|ALTER|DROP)\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW|MATERIALIZED\s+VIEW|SEQUENCE|STAGE|FILE\s+FORMAT|PIPE|TASK|FUNCTION|PROCEDURE|ROLE|GRANT)\s+((?:[\w]+\.){0,2}[\w]+)", text, re.IGNORECASE)
        if not m:
            return {}
        obj = m.group(2).upper()
        parts = m.group(3).split('.')
        db = schema = None; name = parts[-1]
        if len(parts)==3:
            db,schema,name = parts
        elif len(parts)==2:
            schema,name = parts
        return {'database':db,'schema':schema,'object_type':obj,'object_name':name}

    def enrich_pr_files(self, files):
        objs=[]
        for f in files:
            if not f['filename'].startswith(self.path_filter):
                continue
            status = f['status']
            content = None if status=='removed' else self.fetch_file_content(f['filename'])
            meta = self.parse_sql_metadata(content)
            if not meta:
                continue
            objs.append({**meta,'change_type':status})
        return objs

# -----------------------------
# Snowflake connection helper
# -----------------------------
def get_snowflake_connection():
    creds = {k:os.getenv(k) for k in ['SNOWFLAKE_USER','SNOWFLAKE_ACCOUNT','SNOWFLAKE_ROLE','SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE','SNOWFLAKE_PRIVATE_KEY','SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']}
    if not all(creds.values()):
        missing=[k for k,v in creds.items() if not v]
        raise EnvironmentError(f"Missing Snowflake vars: {missing}")
    with tempfile.NamedTemporaryFile('w+',delete=False,suffix='.pem') as f:
        f.write(creds['SNOWFLAKE_PRIVATE_KEY'])
        keypath=f.name
    return snowflake.connector.connect(
        user=creds['SNOWFLAKE_USER'], account=creds['SNOWFLAKE_ACCOUNT'], role=creds['SNOWFLAKE_ROLE'],
        warehouse=creds['SNOWFLAKE_WAREHOUSE'], database=creds['SNOWFLAKE_DATABASE'],
        private_key_file=keypath, private_key_file_pwd=creds['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
    )

# -----------------------------
# Table/time-travel helpers (kept from your original script)
# -----------------------------
def get_column_names(cur,db,sch,tbl):
    cur.execute(f"SHOW COLUMNS IN TABLE {db}.{sch}.{tbl};")
    return [r[2].upper() for r in cur.fetchall()]

def clone_table_at_point(cur,db,sch,tbl,ts,tag):
    name=f"{tbl}_backup_{tag}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    sql=f"CREATE TABLE {db}.{sch}.{name} CLONE {db}.{sch}.{tbl} AT (TIMESTAMP=>'{ts}');"
    print(f"[BACKUP] {sql}")
    cur.execute(sql)
    return name

def rollback_data_inplace(cur,db,sch,tbl,ts):
    sql=f"INSERT OVERWRITE INTO {db}.{sch}.{tbl} SELECT * FROM {db}.{sch}.{tbl} AT (TIMESTAMP=>'{ts}');"
    print(f"[ROLLBACK DATA] {sql}")
    cur.execute(sql)

def rollback_added_columns(cur,db,sch,tbl,ts):
    b=clone_table_at_point(cur,db,sch,tbl,ts,'schema')
    curr=set(get_column_names(cur,db,sch,tbl))
    old=set(get_column_names(cur,db,sch,b))
    for c in curr-old:
        sql=f"ALTER TABLE {db}.{sch}.{tbl} DROP COLUMN {c};"
        print(f"[ROLLBACK SCHEMA] {sql}")
        cur.execute(sql)

def rollback_dropped_columns(cur,db,sch,tbl,ts):
    print("⚠️ Cannot auto-restore dropped columns; backup clone created.")
    clone_table_at_point(cur,db,sch,tbl,ts,'recover')

# -----------------------------
# Non-table restore helpers
# -----------------------------
def execute_sql_statements(cur, sql_text):
    """
    Attempt to execute SQL text. Snowflake connector expects statements one at a time.
    We try to execute the whole block; on failure we split by semicolon newline.
    """
    sql_text = sql_text.strip()
    if not sql_text:
        return
    try:
        print("[EXECUTE DDL]")
        cur.execute(sql_text)
        return
    except Exception as e:
        # fallback: naive split
        print("[WARN] Single execute failed, trying split; err:", e)
        parts = [s.strip() for s in re.split(r';\s*\n', sql_text) if s.strip()]
        for p in parts:
            try:
                print("[EXECUTE PART]", p[:200].replace("\n"," "))
                cur.execute(p)
            except Exception as e2:
                print("[ERROR] statement failed:", e2, "statement:", p[:300])
                # continue with others

def apply_snapshot_sql(cur, snapshot_content):
    """
    Apply a snapshot SQL text (GET_DDL output) to restore object.
    We attempt to run as-is; GET_DDL often returns a single CREATE ... statement.
    """
    execute_sql_statements(cur, snapshot_content)

def restore_from_clone_getddl(cur, db, sch, name, obj_type, ts):
    """
    Create a temporary schema clone, GET_DDL for the object, apply it, and remove clone.
    """
    clone_schema = f"{sch}_rb_clone_{int(time.time())}"
    try:
        cur.execute(f"CREATE SCHEMA {db}.{clone_schema} CLONE {db}.{sch} AT (TIMESTAMP=>'{ts}');")
        obj_id_clone = f"{db}.{clone_schema}.{name}"
        # For GET_DDL: some object types use different keys, but we try the obj_type value
        try:
            cur.execute(f"SELECT GET_DDL('{obj_type}', '{obj_id_clone}');")
            ddl = cur.fetchone()[0]
            if ddl:
                ddl_to_apply = ddl.replace(f"{db}.{clone_schema}", f"{db}.{sch}")
                apply_snapshot_sql(cur, ddl_to_apply)
        except Exception as e:
            print(f"[GET_DDL ERROR] {obj_type} {obj_id_clone}: {e}")
    finally:
        try:
            cur.execute(f"DROP SCHEMA IF EXISTS {db}.{clone_schema};")
        except Exception as e:
            print("[CLEANUP] drop clone failed:", e)

# -----------------------------
# Main rollback flow
# -----------------------------
def main():
    p=argparse.ArgumentParser()
    p.add_argument('--repo',required=True)
    p.add_argument('--token',required=True)
    p.add_argument('--branch', default='main', help='branch that contains rollback/ snapshots (e.g. rollback/pr-123)')
    p.add_argument('--path',default='snowflake/')
    args=p.parse_args()

    gh=GitHubPRAnalyzer(args.repo,args.token,args.branch,args.path)

    # 1) discover PR and merge time (used for table rollback clone)
    try:
        pr = gh.get_latest_pr_number()
    except Exception as e:
        print("[WARN] Could not find latest PR:", e)
        pr = None

    pr_ts = None
    if pr:
        try:
            pr_ts = gh.get_pr_merged_time(pr)
            print(f"[INFO] PR #{pr} merged at {pr_ts}")
        except Exception as e:
            print("[WARN] Could not get PR merged time:", e)
            pr_ts = None

    # 2) connect to Snowflake
    conn = get_snowflake_connection(); cur = conn.cursor()

    # 3) Handle tables using existing time-travel logic (from original script)
    # We'll still process the PR file list to perform table rollbacks as before
    try:
        if pr:
            files = gh.get_pr_files(pr)
            objs = gh.enrich_pr_files(files)
            print(f"[INFO] Found {len(objs)} objects changed in PR")
        else:
            objs = []
    except Exception as e:
        print("[WARN] Could not list PR files:", e)
        objs = []

    now_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    for o in objs:
        db=o.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        sch=o.get('schema'); tbl=o.get('object_name'); typ=o.get('object_type','').upper(); ch=o.get('change_type')
        if not ((sch and tbl and typ=='TABLE') or typ=='INSERT'):
            print(f"[SKIP TABLE HANDLER] {sch}.{tbl} ({typ})")
            continue
        print(f"[PROCESS TABLE] {db}.{sch}.{tbl} ({ch})")
        # Pre-backup
        try:
            clone_table_at_point(cur,db,sch,tbl,now_ts,'pre')
        except Exception as e:
            print("[WARN] pre-clone failed:", e)
        # Do rollbacks for table/insert
        try:
            if typ=='INSERT' or ch in ('modified','changed','updated'):
                rollback_data_inplace(cur,db,sch,tbl,pr_ts or now_ts)
            elif ch=='added':
                rollback_added_columns(cur,db,sch,tbl,pr_ts or now_ts)
            elif ch=='removed':
                rollback_dropped_columns(cur,db,sch,tbl,pr_ts or now_ts)
        except Exception as e:
            print("[ERROR] table rollback op failed:", e)

    # 4) Restore non-table objects from rollback snapshot files (from branch)
    # We expect rollback snapshots to be saved under top-level "rollback/pr-<run_id>/" in the specified branch.
    # If args.branch is 'main' and there are no snapshots, we fall back to trying clone+GET_DDL per object.
    print("[INFO] Looking for rollback snapshots under top-level 'rollback/' in branch:", args.branch)
    top_level = gh.list_dir('rollback', ref=args.branch)
    if not top_level:
        print("[INFO] No rollback folder found in branch; will attempt clone-based restoration for non-table objects if possible.")
        snapshot_dirs = []
    else:
        # top_level may be a list of items (files/dirs). find directories like pr-<tag>
        snapshot_dirs = [item['path'] for item in top_level if item.get('type')=='dir']

    # Flatten snapshot files mapping: { 'schema.object.objtype.sql' -> path }
    snapshot_files = {}
    for snap_dir in snapshot_dirs:
        contents = gh.list_dir(snap_dir, ref=args.branch)
        for item in contents:
            if item.get('type') != 'file':
                continue
            path = item['path']  # e.g. rollback/pr-123/hr.employees.VIEW.sql
            name = os.path.basename(path)
            snapshot_files[name] = path

    # apply snapshots in dependency-safe order
    # preferred order: FILE FORMAT / STAGE -> SEQUENCE -> TABLE (we skip because handled) -> VIEW / MATERIALIZED VIEW -> FUNCTION/PROCEDURE -> PIPE/TASK -> GRANTS
    order_patterns = [
        ('FILE FORMAT', r'\.FILEFORMAT\.sql$'),
        ('STAGE', r'\.STAGE\.sql$'),
        ('SEQUENCE', r'\.SEQUENCE\.sql$'),
        ('VIEW', r'\.VIEW\.sql$'),
        ('MATERIALIZED VIEW', r'\.MATERIALIZED VIEW\.sql$'),  # unlikely filename; handle explicit token below
        ('FUNCTION/PROC', r'\.(FUNCTION|PROCEDURE)\.sql$'),
        ('PIPE/TASK', r'\.(PIPE|TASK)\.sql$'),
        ('OTHER', r'.*\.sql$'),
    ]

    applied = []
    skipped = []

    # Helper to find snapshot by object tokens
    def find_snapshot_for(schema, name, obj_type):
        # two common filename formats we use: "{schema}.{name}.{OBJTYPE}.sql" (OBJTYPE may have space removed)
        candidates = []
        for fname, path in snapshot_files.items():
            # simple parse
            # normalize tokens
            norm = fname.replace(' ', '').upper()
            token = f"{schema.upper()}.{name.upper()}.{obj_type.replace(' ','').upper()}"
            if norm.startswith(token) or (f".{obj_type.replace(' ','').upper()}.sql" in norm and f"{schema.upper()}.{name.upper()}" in norm):
                candidates.append(path)
        # fallback: any file that contains .{name}.{objtype}.sql
        if candidates:
            return candidates[0]
        return None

    # iterate objects of interest: first gather non-table objects changed in PR (if available),
    # else try to discover from snapshot files
    non_table_objs = []
    # gather from PR objects (if we had them)
    for o in objs:
        typ = o.get('object_type','').upper()
        if typ and typ not in ('TABLE','INSERT'):
            non_table_objs.append({'database': o.get('database'), 'schema':o.get('schema'), 'name':o.get('object_name'), 'type':typ})

    # if no PR object list (or to supplement), derive from snapshot filenames
    if not non_table_objs and snapshot_files:
        for fname in snapshot_files:
            # parse "hr.my_view.VIEW.sql" or "hr.my_stage.STAGE.sql"
            parts = fname.rsplit('.', 3)
            # safe parse: try splitting by dots
            p = fname.split('.')
            if len(p) >= 3:
                sch = p[0]
                nm = p[1]
                typ = p[2].replace('.sql','')
                non_table_objs.append({'database': os.getenv('SNOWFLAKE_DATABASE'), 'schema':sch, 'name':nm, 'type': typ.upper()})

    # now apply in order patterns
    for label, pat in order_patterns:
        regex = re.compile(pat, re.IGNORECASE)
        for fname, path in list(snapshot_files.items()):
            if not regex.search(fname):
                continue
            print(f"[SNAPSHOT APPLY] {fname} -> {path}")
            try:
                content = gh.fetch_file_content(path, ref=args.branch)
                if not content:
                    print(f"[WARN] snapshot file empty: {path}")
                    skipped.append(path); continue
                # apply the DDL content
                apply_snapshot_sql(cur, content)
                applied.append(path)
            except Exception as e:
                print(f"[ERROR] applying snapshot {path}: {e}")
                skipped.append(path)
            # remove applied from snapshot_files so that OTHER step does not double-apply
            snapshot_files.pop(fname, None)

    # After snapshots, if any non-table objects remain in PR list and not handled, try clone-getddl fallback
    for o in non_table_objs:
        db = o.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        sch = o.get('schema'); name = o.get('name'); typ = o.get('type')
        # skip if table
        if not (sch and name and typ and typ not in ('TABLE','INSERT')):
            continue
        # check if a snapshot exists (maybe different filename); try to find
        snap = find_snapshot_for(sch, name, typ)
        if snap:
            print(f"[ALREADY HANDLED BY SNAPSHOT] {sch}.{name} ({typ}) -> {snap}")
            continue
        # fallback to clone-getddl if PR merge time available
        if pr_ts:
            print(f"[FALLBACK CLONE] Attempting clone+GET_DDL for {db}.{sch}.{name} at {pr_ts}")
            try:
                restore_from_clone_getddl(cur, db, sch, name, typ, pr_ts)
            except Exception as e:
                print(f"[ERROR] clone fallback failed for {sch}.{name}: {e}")
        else:
            print(f"[WARN] No snapshot and no PR timestamp available for {sch}.{name}; manual restore required.")
    # done
    cur.close(); conn.close()
    print(f"[DONE] Applied {len(applied)} snapshot files; skipped {len(skipped)} files (see logs).")

if __name__=='__main__':
    main()
