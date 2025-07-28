# validation_checker.py
# ---------------------
# Checks for destructive SQL operations (DROP, TRUNCATE, ALTER DROP COLUMN) across schema-level folders
# under a top-level Snowflake directory, and then queries Snowflake for downstream dependencies.

import os
import re
import sys
import json
import argparse
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from git import Repo

# Patterns for destructive operations
DESTRUCTIVE_PATTERNS = {
    'TABLE': [
        re.compile(r'\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE),
        re.compile(r'\bTRUNCATE\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    'ALTER_TABLE_DROP_COLUMN': [
        re.compile(r'\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s]+)\s+DROP\s+COLUMN\s+([^\s;]+)', re.IGNORECASE)
    ]
}

# Parse fully qualified names
def parse_fqn(name, default_db, default_schema):
    parts = name.replace('"', '').split('.')
    if len(parts) == 3:
        return parts
    if len(parts) == 2:
        return default_db, parts[0], parts[1]
    if len(parts) == 1 and default_schema:
        return default_db, default_schema, parts[0]
    return None, None, None

# Validate required env vars
def validate_env():
    req = [
        'SNOWFLAKE_ACCOUNT','SNOWFLAKE_USER','SNOWFLAKE_ROLE',
        'SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE','SNOWFLAKE_PRIVATE_KEY',
        'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]
    missing = [v for v in req if not os.getenv(v)]
    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

# Main logic
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--snowflake-root', required=True,
                        help='Top-level directory containing schema-named subfolders')
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
    # Walk schema folders
    for schema_dir in os.listdir(args.snowflake_root):
        if schema_dir.lower() in ('backup', 'rollback'): continue
        schema_path = os.path.join(args.snowflake_root, schema_dir)
        if not os.path.isdir(schema_path): continue
        for diff in diffs:
            path = diff.b_path
            if path and path.startswith(f"{args.snowflake_root}/{schema_dir}/") and path.endswith('.sql'):
                # ignore backup subfolders
                rel = path.replace(f"{args.snowflake_root}/{schema_dir}/", '')
                if not rel.startswith('backup/'):
                    changed.append(path)

    if not changed:
        print("✅ No changed SQL files to scan.")
        return
    print("🔍 Scanning files:", changed)

    # Scan for destructive operations
    impacted = {}
    for fp in changed:
        # default schema from folder name
        folder = fp.split('/')[1]
        default_schema = folder.upper()
        sql = open(fp, 'r', encoding='utf-8').read()
        for domain, patterns in DESTRUCTIVE_PATTERNS.items():
            for rx in patterns:
                for m in rx.finditer(sql):
                    literal = m.group(1)
                    db, sch, name = parse_fqn(literal, target_db, default_schema)
                    if not all((db, sch, name)):
                        print(f"❌ Could not parse identifier {literal} in {fp}")
                        sys.exit(1)
                    key = (domain, db, sch, name)
                    impacted.setdefault(key, []).append(fp)

    if not impacted:
        print("✅ No destructive operations found.")
        return

    print(f"\n🔗 Checking dependencies for {len(impacted)} operations...")

    # Setup Snowflake connection
    key = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    with open('key.pem','w') as f: f.write(key)
    os.chmod('key.pem', 0o600)
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

    blockers = []
    for (dom, db, sch, nm), files in impacted.items():
        print(f"Checking {dom} {db}.{sch}.{nm} (from {files})")
        q = f"""
            SELECT REFERENCING_DATABASE, REFERENCING_SCHEMA,
                   REFERENCING_OBJECT_NAME, REFERENCING_OBJECT_DOMAIN
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

    cur.close()
    ctx.close()

    if not blockers:
        print("✅ No blocking dependencies.")
        return

    # Filter out changes within same commit
    dropped = {f for (_,f1,f2,f3) in impacted.keys() for f in [f1+"."+f2+"."+f3]}
    final = [b for b in blockers if b['dependent'] not in dropped]

    with open('blocking_dependencies.json','w') as out:
        json.dump(final, out, indent=2)

    print("\n❌ Blocking dependencies found:")
    for b in final:
        print(f" • {b['dependent']} ({b['domain']}) depends on {b['dropped']}")
    if not args.dry_run:
        sys.exit(1)
    else:
        print("⚠️ Dry-run; not exiting error.")

if __name__ == '__main__':
    main()
