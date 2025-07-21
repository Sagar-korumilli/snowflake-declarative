import os
import re
import snowflake.connector

# Folders containing SQL to scan
SQL_FOLDERS = ['snowflake/setup', 'snowflake/migrations']

# Destructive patterns per object type (multiple patterns per domain)
DESTRUCTIVE_PATTERNS = {
    'TABLE': [
        re.compile(r'\bDROP\s+TABLE\s+([^\s;]+)', re.IGNORECASE),
        re.compile(r'\bTRUNCATE\s+TABLE\s+([^\s;]+)', re.IGNORECASE)
    ],
    'VIEW': [
        re.compile(r'\bDROP\s+VIEW\s+([^\s;]+)', re.IGNORECASE)
    ],
    'FUNCTION': [
        re.compile(r'\bDROP\s+FUNCTION\s+([^\s(]+)', re.IGNORECASE)
    ],
    'PROCEDURE': [
        re.compile(r'\bDROP\s+PROCEDURE\s+([^\s(]+)', re.IGNORECASE)
    ],
    'STAGE': [
        re.compile(r'\bDROP\s+STAGE\s+([^\s;]+)', re.IGNORECASE)
    ],
    'FILE_FORMAT': [
        re.compile(r'\bDROP\s+FILE\s+FORMAT\s+([^\s;]+)', re.IGNORECASE)
    ],
    'SEQUENCE': [
        re.compile(r'\bDROP\s+SEQUENCE\s+([^\s;]+)', re.IGNORECASE)
    ],
    'COLUMN': [
        re.compile(r'\bALTER\s+TABLE\s+([^\s]+)\s+DROP\s+COLUMN\s+([^\s;]+)', re.IGNORECASE)
    ]
}

# Collect impacted objects
impacted = []

for folder in SQL_FOLDERS:
    for fname in os.listdir(folder):
        if not fname.lower().endswith('.sql'):
            continue
        sql = open(os.path.join(folder, fname)).read()
        for domain, regex_list in DESTRUCTIVE_PATTERNS.items():
            for regex in regex_list:
                for match in regex.finditer(sql):
                    if domain == 'COLUMN':
                        tbl = match.group(1).split('.')[-1]
                        impacted.append(('TABLE', tbl.upper()))
                    else:
                        obj = match.group(1).split('.')[-1]
                        impacted.append((domain, obj.upper()))

# Dedupe list
impacted = sorted(set(impacted))

if not impacted:
    print("✅ No destructive operations found—skipping dependency check.")
    exit(0)

print(f"🔍 Checking downstream dependencies for {len(impacted)} object(s):")
for dom, obj in impacted:
    print(f" • {dom}.{obj}")

# Connect to Snowflake
ctx = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    role=os.environ['SNOWFLAKE_ROLE']
)
target_db = os.environ['SNOWFLAKE_DATABASE']
cur = ctx.cursor()

# Check for downstream dependencies
blocking = []

for domain, obj in impacted:
    query = f"""
        SELECT 
            REFERENCING_DATABASE,
            REFERENCING_SCHEMA,
            REFERENCING_OBJECT_NAME,
            REFERENCING_OBJECT_DOMAIN
        FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
        WHERE REFERENCED_DATABASE = %s
          AND REFERENCED_OBJECT_DOMAIN = %s
          AND REFERENCED_OBJECT_NAME = %s
    """
    cur.execute(query, (target_db, domain, obj))
    for db_, schema_, name_, dom_ in cur.fetchall():
        blocking.append((domain, obj, db_, schema_, name_, dom_))

cur.close()
ctx.close()

if blocking:
    print("\n❌ Cannot proceed—these downstream dependencies would break:\n")
    for dom, obj, db_, schema_, name_, dom_ in blocking:
        print(f" • {name_} ({dom_}) in {db_}.{schema_} depends on {dom}.{obj}")
    exit(1)

print("\n✅ No blocking dependencies found. You’re safe to proceed.")
exit(0)
