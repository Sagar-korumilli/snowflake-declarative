import os
import re
import sys
import json
import argparse
import snowflake.connector

# --- Configuration ---
SQL_FOLDERS = ['snowflake/setup', 'snowflake/migrations']
SCHEMA_FROM_FILENAME_RE = re.compile(r'__([A-Za-z0-9_]+)__', re.IGNORECASE)
METADATA_TABLE = os.getenv('METADATA_TABLE', 'PUBLIC.DEPENDENCY_METADATA')

# Patterns for destructive operations
DESTRUCTIVE_PATTERNS = {
    'TABLE': [
        re.compile(r'\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE),
        re.compile(r'\bTRUNCATE\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    'VIEW': [
        re.compile(r'\bDROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?([^\s;]+)', re.IGNORECASE)
    ],
    # add other domains if needed...
}


def parse_fq(name, default_db, default_schema):
    parts = name.upper().replace('"', '').split('.')
    if len(parts) == 3:
        return parts
    if len(parts) == 2:
        return (default_db, parts[0], parts[1])
    if len(parts) == 1 and default_schema:
        return (default_db, default_schema, parts[0])
    return (None, None, None)


def validate_env():
    req = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_ROLE', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE'
    ]
    miss = [v for v in req if not os.getenv(v)]
    if miss:
        sys.exit(f"Missing env vars: {miss}")


def get_conn():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help="Report but don’t exit on errors")
    args = parser.parse_args()

    validate_env()
    target_db = os.getenv('SNOWFLAKE_DATABASE')

    # 1) Scan SQL folders for destructive ops
    impacted = {}
    for folder in SQL_FOLDERS:
        if not os.path.isdir(folder):
            continue
        for fname in os.listdir(folder):
            if not fname.lower().endswith('.sql'):
                continue
            schema_match = SCHEMA_FROM_FILENAME_RE.search(fname)
            default_schema = schema_match.group(1).upper() if schema_match else None
            content = open(os.path.join(folder, fname), 'r').read()
            for domain, patterns in DESTRUCTIVE_PATTERNS.items():
                for pat in patterns:
                    for m in pat.finditer(content):
                        db, sch, nm = parse_fq(m.group(1), target_db, default_schema)
                        key = (domain, db, sch, nm)
                        impacted.setdefault(key, []).append(fname)

    if not impacted:
        print("✅ No destructive SQL found.")
        return

    # 2) Check against your local metadata table
    conn = get_conn()
    cur = conn.cursor()
    blocking = []
    for (dom, db, sch, nm), files in impacted.items():
        print(f"Checking dependencies for {db}.{sch}.{nm}…")
        sql = (
            f"SELECT REFERENCING_DATABASE, REFERENCING_SCHEMA, "
            f"REFERENCING_OBJECT_NAME, REFERENCING_OBJECT_DOMAIN "
            f"FROM {METADATA_TABLE} "
            f"WHERE DEPENDS_ON_DOMAIN=%s "
            f"AND DEPENDS_ON_DATABASE=%s "
            f"AND DEPENDS_ON_SCHEMA=%s "
            f"AND DEPENDS_ON_NAME=%s"
        )
        cur.execute(sql, (dom, db, sch, nm))
        for row in cur.fetchall():
            blocking.append({
                'dropped': f"{db}.{sch}.{nm}",
                'dependent': f"{row[0]}.{row[1]}.{row[2]}",
                'domain': row[3]
            })
    cur.close()
    conn.close()

    # 3) Filter out self-drops
    drops = {f"{db}.{sch}.{nm}" for (dom, db, sch, nm) in impacted}
    true_blockers = [b for b in blocking if b['dependent'] not in drops]

    # 4) Write report
    with open('blocking_dependencies.json', 'w') as fo:
        json.dump(true_blockers, fo, indent=2)

    if true_blockers and not args.dry_run:
        print("❌ Blocking dependencies found:")
        for b in true_blockers:
            print(f" • {b['dependent']} depends on {b['dropped']}")
        sys.exit(1)

    print("✅ No blocking dependencies.")
    

if __name__ == '__main__':
    main()
