#!/usr/bin/env python3
# validation_checker.py
# ---------------------
# Checks for destructive SQL operations (DROP, TRUNCATE, ALTER DROP COLUMN) across object-level
# migration files under each schema folder, and then queries Snowflake for downstream dependencies.

import os
import re
import sys
import json
import argparse
import snowflake.connector
from pathlib import Path 
from snowflake.connector.errors import ProgrammingError
from git import Repo

# Patterns for destructive operations
DESTRUCTIVE_PATTERNS = {
    'DROP_TABLE': [
        re.compile(r'\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE),
        re.compile(r'\bTRUNCATE\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    'ALTER_DROP_COLUMN': [
        re.compile(r'\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s]+)\s+DROP\s+COLUMN\s+([^\s;]+)', re.IGNORECASE)
    ]
}

# Parse fully qualified name
def parse_fqn(name, default_db, default_schema):
    parts = name.replace('"','').split('.')
    if len(parts)==3:
        return parts
    if len(parts)==2:
        return default_db, parts[0], parts[1]
    if len(parts)==1 and default_schema:
        return default_db, default_schema, parts[0]
    return None, None, None

# Validate required env vars
def validate_env():
    req = [
        'SNOWFLAKE_ACCOUNT','SNOWFLAKE_USER','SNOWFLAKE_ROLE',
        'SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE',
        'SNOWFLAKE_PRIVATE_KEY','SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]
    missing = [v for v in req if not os.getenv(v)]
    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

# Main
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--snowflake-root', required=True,
                        help='Root directory containing schema subfolders')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report dependencies without exiting error')
    args = parser.parse_args()

    validate_env()
    target_db = os.getenv('SNOWFLAKE_DATABASE')

    # Determine changed SQL files via Git diff
    repo = Repo('.')
    commit = repo.head.commit
    parent = commit.parents[0] if commit.parents else None
    diffs = commit.diff(parent) if parent else []

    changed = []
    for d in diffs:
        path = d.b_path
        if not path or not path.endswith('.sql'): continue
        if not path.startswith(f"{args.snowflake_root}/"): continue
        if '/backup/' in path or '/rollback/' in path: continue
        changed.append(path)

    if not changed:
        print("✅ No changed SQL files to scan.")
        return
    print("🔍 Scanning files:", changed)

    # Write key file
    with open('key.pem','w') as f: f.write(os.getenv('SNOWFLAKE_PRIVATE_KEY'))
    os.chmod('key.pem',0o600)

    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=target_db,
        authenticator='snowflake_jwt',
        private_key_file='key.pem',
        private_key_file_pwd=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
    )
    cur = ctx.cursor()

    impacted = {}
    # Scan each changed file for destructive patterns
    for fp in changed:
        default_schema = Path(fp).parts[1].upper()
        sql = open(fp,'r',encoding='utf-8').read()
        for domain, patterns in DESTRUCTIVE_PATTERNS.items():
            for rx in patterns:
                for m in rx.finditer(sql):
                    literal = m.group(1)
                    db, sch, name = parse_fqn(literal, target_db.upper(), default_schema)
                    if not all([db,sch,name]):
                        print(f"❌ Could not parse identifier {literal} in {fp}")
                        sys.exit(1)
                    key = (domain, db, sch, name)
                    impacted.setdefault(key, []).append(fp)

    if not impacted:
        print("✅ No destructive operations found.")
        cur.close(); ctx.close()
        return

    print(f"\n🔗 Checking dependencies for {len(impacted)} object(s)...")
    blockers = []
    for (dom, db, sch, nm), files in impacted.items():
        print(f"Checking {dom} {db}.{sch}.{nm} (from {files})")
        q = f"""
            SELECT REFERENCING_DATABASE,
                   REFERENCING_SCHEMA,
                   REFERENCING_OBJECT_NAME,
                   REFERENCING_OBJECT_DOMAIN
              FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
             WHERE REFERENCED_DATABASE = '{db}'
               AND REFERENCED_SCHEMA   = '{sch}'
               AND REFERENCED_OBJECT_NAME   = '{nm}'
               AND REFERENCED_OBJECT_DOMAIN = '{dom}'
               AND REFERENCING_OBJECT_ID != REFERENCED_OBJECT_ID
        """
        try:
            cur.execute(q)
            for row in cur.fetchall():
                blockers.append({
                    'dropped': f"{db}.{sch}.{nm}",
                    'dependent': f"{row[0]}.{row[1]}.{row[2]}",
                    'domain': row[3]
                })
        except ProgrammingError as e:
            if 'does not exist' in str(e).lower(): continue
            print(f"❌ Query failed: {e}")
            sys.exit(1)

    cur.close(); ctx.close()

    # Filter same-change dependencies
    dropped = {f"{db}.{sch}.{nm}" for (dom,db,sch,nm) in impacted}
    final = [b for b in blockers if b['dependent'] not in dropped]

    with open('blocking_dependencies.json','w') as out:
        json.dump(final, out, indent=2)

    if final:
        print("\n❌ Blocking dependencies found:")
        for b in final:
            print(f" • {b['dependent']} ({b['domain']}) depends on {b['dropped']}")
        if not args.dry_run:
            sys.exit(1)
        else:
            print("⚠️ Dry-run — not exiting error.")
    else:
        print("✅ No blocking dependencies.")

if __name__ == '__main__':
    main()
