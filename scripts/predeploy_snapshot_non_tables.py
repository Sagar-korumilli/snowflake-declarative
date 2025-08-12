#!/usr/bin/env python3
# tools/predeploy_snapshot_non_tables.py
import argparse
import os
import re
import subprocess
import tempfile
from datetime import datetime, timezone

import snowflake.connector

def run_shell(cmd):
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nSTDOUT: {p.stdout}\nSTDERR: {p.stderr}")
    return p.stdout.strip()

def get_changed_files(github_event_before, github_sha):
    if not github_event_before or not github_sha:
        print("[WARN] Missing GITHUB_EVENT_BEFORE or GITHUB_SHA, will check ALL files under snowflake/")
        all_files = run_shell("git ls-files 'snowflake/*' || true").splitlines()
        return [p for p in all_files if p]
    cmd = f"git diff --name-only {github_event_before} {github_sha} || true"
    out = run_shell(cmd)
    files = [l for l in out.splitlines() if l.strip()]
    return files

def parse_sql_metadata_from_file(path):
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as fh:
            text = fh.read()
    except Exception:
        return {}
    if not text:
        return {}
    ins = re.search(r"\bINSERT\s+INTO\s+((?:[\w]+\.){0,2}[\w]+)", text, re.IGNORECASE)
    if ins:
        parts = ins.group(1).split('.')
        name = parts[-1]
        schema = parts[-2] if len(parts) >= 2 else None
        return {'object_type': 'INSERT', 'object_name': name, 'schema': schema}
    m = re.search(
        r"\b(CREATE|ALTER|DROP)\s+(?:OR\s+REPLACE\s+)?(VIEW|MATERIALIZED\s+VIEW|SEQUENCE|STAGE|FILE\s+FORMAT|PIPE|TASK|FUNCTION|PROCEDURE|ROLE|GRANT|TABLE)\s+((?:[\w]+\.){0,2}[\w]+)",
        text, re.IGNORECASE
    )
    if not m:
        return {}
    action = m.group(1).upper()
    obj = m.group(2).upper().replace('  ', ' ')
    parts = m.group(3).split('.')
    db = schema = None
    name = parts[-1]
    if len(parts) == 3:
        db, schema, name = parts
    elif len(parts) == 2:
        schema, name = parts
    return {'database': db, 'schema': schema, 'object_type': obj, 'object_name': name, 'action': action.lower()}

def get_snowflake_connection_from_env():
    creds = {k: os.getenv(k) for k in ['SNOWFLAKE_USER','SNOWFLAKE_ACCOUNT','SNOWFLAKE_ROLE','SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE','SNOWFLAKE_PRIVATE_KEY','SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']}
    missing=[k for k,v in creds.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing Snowflake vars: {missing}")
    with tempfile.NamedTemporaryFile('w+', delete=False, suffix='.pem') as f:
        f.write(creds['SNOWFLAKE_PRIVATE_KEY'])
        keypath=f.name
    return snowflake.connector.connect(
        user=creds['SNOWFLAKE_USER'],
        account=creds['SNOWFLAKE_ACCOUNT'],
        role=creds['SNOWFLAKE_ROLE'],
        warehouse=creds['SNOWFLAKE_WAREHOUSE'],
        database=creds['SNOWFLAKE_DATABASE'],
        private_key_file=keypath,
        private_key_file_pwd=creds['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
    )

def save_file(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(content)

def snapshot_get_ddl(cur, db, sch, obj_type, name, outdir, tag):
    identifier = f"{db}.{sch}.{name}" if db else f"{sch}.{name}"
    try:
        cur.execute(f"SELECT GET_DDL('{obj_type}', '{identifier}');")
        row = cur.fetchone()
        ddl = row[0] if row else None
    except Exception as e:
        print(f"[GET_DDL ERROR] {identifier}: {e}")
        ddl = None
    if ddl:
        fname = f"{outdir}/pr-{tag}/{sch}.{name}.{obj_type}.sql"
        header = f"-- snapshot pre-deploy (non-table) for run {tag}\n-- object: {identifier}\n-- captured_at: {datetime.now(timezone.utc).isoformat()}\n\n"
        save_file(fname, header + ddl)
        print(f"[SNAPSHOT] {fname}")
        return fname
    else:
        print(f"[SKIP GET_DDL] no DDL for {identifier}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default='rollback', help='output root folder (top-level rollback/)')
    parser.add_argument('--tag', default=None, help='tag for naming e.g. github run id or pr number')
    parser.add_argument('--files', default=None, help='comma-separated list of changed files (overrides git diff)')
    args = parser.parse_args()

    github_event_before = os.getenv('GITHUB_EVENT_BEFORE')
    github_sha = os.getenv('GITHUB_SHA')
    run_tag = args.tag or os.getenv('GITHUB_RUN_ID') or datetime.now().strftime('%Y%m%d%H%M%S')

    if args.files:
        changed_files = [p.strip() for p in args.files.split(',') if p.strip()]
    else:
        changed_files = get_changed_files(github_event_before, github_sha)

    changed_sf = [p for p in changed_files if p.startswith('snowflake/') and '/rollback/' not in p]
    if not changed_sf:
        print("[INFO] No changed snowflake files detected.")
        return

    print(f"[INFO] Detected {len(changed_sf)} changed snowflake files (will snapshot non-tables):")
    for p in changed_sf:
        print("  -", p)

    conn = get_snowflake_connection_from_env()
    cur = conn.cursor()

    created = []
    for p in changed_sf:
        meta = parse_sql_metadata_from_file(p)
        if not meta:
            print(f"[NO META] Skipping {p} (no recognizable CREATE/ALTER/INSERT)")
            continue
        obj_type = meta.get('object_type')
        if not obj_type or obj_type.upper() in ('TABLE','INSERT'):
            print(f"[SKIP TABLE/INSERT] {p}")
            continue
        db = meta.get('database') or os.getenv('SNOWFLAKE_DATABASE') or ''
        sch = meta.get('schema') or (p.split('/')[1] if len(p.split('/'))>1 else '')
        name = meta.get('object_name')
        if not (sch and name):
            print(f"[SKIP] Could not infer schema/object for {p}")
            continue
        s = snapshot_get_ddl(cur, db, sch, obj_type, name, args.out, run_tag)
        if s:
            created.append(s)

    cur.close(); conn.close()
    if created:
        print(f"[DONE] Created {len(created)} snapshot files under {args.out}/pr-{run_tag}/")
    else:
        print("[DONE] No non-table snapshots created.")

if __name__ == '__main__':
    main()
