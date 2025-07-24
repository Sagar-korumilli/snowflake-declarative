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
    'VIEW': [
        re.compile(r'\bDROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    'FUNCTION': [
        re.compile(r'\bDROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?([^\s(;]+)', re.IGNORECASE)
    ],
    'PROCEDURE': [
        re.compile(r'\bDROP\s+PROCEDURE\s+(?:IF\s+EXISTS\s+)?([^\s(;]+)', re.IGNORECASE)
    ],
    'STAGE': [
        re.compile(r'\bDROP\s+STAGE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    'FILE FORMAT': [
        re.compile(r'\bDROP\s+FILE\s+FORMAT\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    'SEQUENCE': [
        re.compile(r'\bDROP\s+SEQUENCE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    'ALTER_TABLE_DROP_COLUMN': [
        re.compile(r'\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s]+)\s+DROP\s+COLUMN\s+([^\s;]+)', re.IGNORECASE)
    ]
}


def parse_fully_qualified_name(name_str, default_db, default_schema):
    parts = name_str.upper().replace('"', '').split('.')
    if len(parts) == 3:
        return (parts[0], parts[1], parts[2])
    elif len(parts) == 2:
        return (default_db, parts[0], parts[1])
    elif len(parts) == 1 and default_schema:
        return (default_db, default_schema, parts[0])
    return (None, None, None)


def validate_env_vars():
    required = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER',
        'SNOWFLAKE_ROLE', 'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_PRIVATE_KEY',
        'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
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

            file_path = os.path.join(folder, fname)
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            for domain, regex_list in DESTRUCTIVE_PATTERNS.items():
                for regex in regex_list:
                    for match in regex.finditer(sql_content):
                        object_domain = 'TABLE' if domain == 'ALTER_TABLE_DROP_COLUMN' else domain
                        unparsed_name = match.group(1)

                        db, schema, name = parse_fully_qualified_name(
                            unparsed_name,
                            target_db.upper(),
                            default_schema
                        )
                        if not all([db, schema, name]):
                            print(f"❌ Could not parse object: {unparsed_name} in file {fname}")
                            sys.exit(1)

                        key = (object_domain, db, schema, name)
                        impacted_objects.setdefault(key, []).append(fname)

    if not impacted_objects:
        print("✅ No destructive operations found.")
        return

    print(f"\n🔍 Step 2: Checking downstream dependencies for {len(impacted_objects)} object(s)...")

    # Connect using key-pair auth
    try:
        ctx = snowflake.connector.connect(
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            user=os.environ['SNOWFLAKE_USER'],
            authenticator='snowflake_jwt',
            private_key_file='key.pem',
            private_key_file_pwd=os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
            role=os.environ['SNOWFLAKE_ROLE'],
            database=target_db
        )
        cur = ctx.cursor()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)

    blocking_dependencies = []

    for (domain, db, schema, name), files in impacted_objects.items():
        print(f"🔗 Checking: {domain} {db}.{schema}.{name} (from {', '.join(files)})")
        query = f"""
            SELECT
                REFERENCING_DATABASE,
                REFERENCING_SCHEMA,
                REFERENCING_OBJECT_NAME,
                REFERENCING_OBJECT_DOMAIN
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
            WHERE REFERENCED_DATABASE = '{db}'
              AND REFERENCED_SCHEMA = '{schema}'
              AND REFERENCED_OBJECT_NAME = '{name}'
              AND REFERENCED_OBJECT_DOMAIN = '{domain}'
              AND REFERENCING_OBJECT_ID != REFERENCED_OBJECT_ID
        """
        try:
            cur.execute(query)
            for row in cur.fetchall():
                blocking_dependencies.append({
                    "dropped_domain": domain,
                    "dropped_name": f"{db}.{schema}.{name}",
                    "dependent_domain": row[3],
                    "dependent_name": f"{row[0]}.{row[1]}.{row[2]}"
                })
        except ProgrammingError as e:
            if "does not exist" in str(e):
                print(f"ℹ️ Object {db}.{schema}.{name} does not exist. Skipping.")
                continue
            print(f"❌ Query failed: {e}")
            sys.exit(1)

    cur.close()
    ctx.close()

    dropped_keys = {f"{d}.{db}.{schema}.{name}" for (d, db, schema, name) in impacted_objects}
    truly_blocking = [
        dep for dep in blocking_dependencies
        if f"{dep['dependent_domain']}.{dep['dependent_name'].upper()}" not in dropped_keys
    ]

    with open("blocking_dependencies.json", "w") as f:
        json.dump(truly_blocking, f, indent=2)

    if truly_blocking:
        print("\n❌ Blocking dependencies found:")
        for dep in truly_blocking:
            print(f" • {dep['dependent_name']} ({dep['dependent_domain']}) depends on {dep['dropped_name']}")
        if not args.dry_run:
            print("🚫 Exiting due to dependency violations.")
            sys.exit(1)
        else:
            print("⚠️ Dry run mode active — not exiting with error.")
    else:
        print("✅ No blocking dependencies. Safe to proceed.")

if __name__ == "__main__":
    main()
