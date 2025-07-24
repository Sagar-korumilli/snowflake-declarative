import os
import re
import sys
import json
import argparse
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# --- Configuration ---
SQL_FOLDERS = ['snowflake/setup', 'snowflake/migrations']
SCHEMA_FROM_FILENAME_RE = re.compile(r'__([a-zA-Z0-9_]+)__', re.IGNORECASE)
DESTRUCTIVE_PATTERNS = {
    'TABLE': [
        re.compile(r'\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE),
        re.compile(r'\bTRUNCATE\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    # ... (other patterns unchanged) ...
    'ALTER_TABLE_DROP_COLUMN': [
        re.compile(r'\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s]+)\s+DROP\s+COLUMN\s+([^\s;]+)', re.IGNORECASE)
    ]
}

def parse_fully_qualified_name(name_str, default_db, default_schema):
    parts = name_str.upper().replace('"', '').split('.')
    if len(parts) == 3:
        return parts
    elif len(parts) == 2:
        return default_db, parts[0], parts[1]
    elif len(parts) == 1 and default_schema:
        return default_db, default_schema, parts[0]
    return (None, None, None)

def validate_env_vars():
    required = [
        'SNOWFLAKE_USER', 'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_ROLE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"❌ Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help="Only report dependencies, don't exit with error.")
    args = parser.parse_args()

    validate_env_vars()
    target_db = os.environ['SNOWFLAKE_DATABASE']
    impacted_objects = {}

    print("🔍 Step 1: Scanning SQL files...")
    for folder in SQL_FOLDERS:
        if not os.path.isdir(folder):
            continue
        for fname in os.listdir(folder):
            if not fname.lower().endswith('.sql'):
                continue

            schema_match = SCHEMA_FROM_FILENAME_RE.search(fname)
            default_schema = schema_match.group(1).upper() if schema_match else None

            content = open(os.path.join(folder, fname), 'r', encoding='utf-8').read()
            for domain, regex_list in DESTRUCTIVE_PATTERNS.items():
                for rx in regex_list:
                    for m in rx.finditer(content):
                        dom = 'TABLE' if domain == 'ALTER_TABLE_DROP_COLUMN' else domain
                        name_literal = m.group(1)
                        db, sch, name = parse_fully_qualified_name(name_literal, target_db.upper(), default_schema)
                        if not all([db, sch, name]):
                            print(f"❌ Could not parse {name_literal} in {fname}")
                            sys.exit(1)
                        impacted_objects.setdefault((dom, db, sch, name), []).append(fname)

    if not impacted_objects:
        print("✅ No destructive operations found.")
        return

    print(f"\n🔍 Step 2: Checking downstream dependencies for {len(impacted_objects)} object(s)...")

    # Write out the key file
    key = os.environ['SNOWFLAKE_PRIVATE_KEY']
    with open('key.pem', 'w') as f:
        f.write(key)
    os.chmod('key.pem', 0o600)

    # Connect via key-pair auth
    ctx = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        role=os.environ['SNOWFLAKE_ROLE'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=target_db,
        authenticator='snowflake_jwt',
        private_key_file='key.pem',
        private_key_file_pwd=os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
    )
    cur = ctx.cursor()

    blocking = []
    for (dom, db, sch, nm), files in impacted_objects.items():
        print(f"🔗 Checking: {dom} {db}.{sch}.{nm} (from {', '.join(files)})")
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
                blocking.append({
                    "dropped_domain": dom,
                    "dropped_name": f"{db}.{sch}.{nm}",
                    "dependent_domain": row[3],
                    "dependent_name": f"{row[0]}.{row[1]}.{row[2]}"
                })
        except ProgrammingError as e:
            msg = str(e).lower()
            if "does not exist" in msg:
                print(f"ℹ️ {db}.{sch}.{nm} not found; skipping.")
                continue
            print(f"❌ Query failed: {e}")
            sys.exit(1)

    cur.close()
    ctx.close()

    # Filter out cases where both dropped and dependent are in the same change set
    dropped_keys = {f"{d}.{db}.{sch}.{nm}" for (d, db, sch, nm) in impacted_objects}
    final_blockers = [b for b in blocking if b['dependent_name'].upper() not in dropped_keys]

    with open("blocking_dependencies.json", "w") as f:
        json.dump(final_blockers, f, indent=2)

    if final_blockers:
        print("\n❌ Blocking dependencies found:")
        for b in final_blockers:
            print(f" • {b['dependent_name']} ({b['dependent_domain']}) depends on {b['dropped_name']}")
        if not args.dry_run:
            print("🚫 Exiting due to dependency violations.")
            sys.exit(1)
        else:
            print("⚠️ Dry-run — not exiting with error.")
    else:
        print("✅ No blocking dependencies. Safe to proceed.")

if __name__ == "__main__":
    main()
