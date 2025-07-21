import os, re, snowflake.connector

# 1. Patterns for destructive operations by object domain
#   key: ACCOUNT_USAGE.OBJECT_DEPENDENCIES domain name
#   value: regex to capture the object name (may include schema prefix)
DESTRUCTIVE_PATTERNS = {
    'TABLE':      re.compile(r'\bDROP\s+TABLE\s+([^\s;]+)', re.IGNORECASE),
    'TABLE':      re.compile(r'\bTRUNCATE\s+TABLE\s+([^\s;]+)', re.IGNORECASE),
    'VIEW':       re.compile(r'\bDROP\s+VIEW\s+([^\s;]+)', re.IGNORECASE),
    'FUNCTION':   re.compile(r'\bDROP\s+FUNCTION\s+([^\s(]+)', re.IGNORECASE),
    'PROCEDURE':  re.compile(r'\bDROP\s+PROCEDURE\s+([^\s(]+)', re.IGNORECASE),
    'STAGE':      re.compile(r'\bDROP\s+STAGE\s+([^\s;]+)', re.IGNORECASE),
    'FILE_FORMAT':re.compile(r'\bDROP\s+FILE\s+FORMAT\s+([^\s;]+)', re.IGNORECASE),
    'SEQUENCE':   re.compile(r'\bDROP\s+SEQUENCE\s+([^\s;]+)', re.IGNORECASE),
    'COLUMN':     re.compile(r'\bALTER\s+TABLE\s+([^\s]+)\s+DROP\s+COLUMN\s+([^\s;]+)', re.IGNORECASE),
    # you can add more (e.g. STREAM, TASK) if you use them
}

# 2. Discover all impacted objects
impacted = []  # list of tuples (domain, object_name)
for folder in ('snowflake/setup', 'snowflake/migrations'):
    for fname in os.listdir(folder):
        if not fname.lower().endswith('.sql'):
            continue
        sql = open(os.path.join(folder, fname)).read()
        for domain, regex in DESTRUCTIVE_PATTERNS.items():
            for match in regex.finditer(sql):
                # For COLUMN we capture table+column; treat table as the object
                if domain == 'COLUMN':
                    tbl = match.group(1).split('.')[-1]
                    impacted.append(('TABLE', tbl.upper()))
                else:
                    obj = match.group(1).split('.')[-1]
                    impacted.append((domain, obj.upper()))

# Dedupe
impacted = sorted(set(impacted))
if not impacted:
    print("✅ No destructive operations found—skipping dependency check.")
    exit(0)

print(f"🔎 Checking downstream dependencies for {len(impacted)} object(s):")
for dom, obj in impacted:
    print(f" • {dom}.{obj}")

# 3. Connect to Snowflake (ACCOUNT_USAGE sits in SNOWFLAKE database)
ctx = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    role=os.environ['SNOWFLAKE_ROLE']
)
target_db = os.environ['SNOWFLAKE_DATABASE']
cur = ctx.cursor()

# 4. For each impacted object, query its dependents
blocking = []
for domain, obj in impacted:
    sql = f"""
        SELECT 
            REFERENCING_DATABASE,
            REFERENCING_SCHEMA,
            REFERENCING_OBJECT_NAME,
            REFERENCING_OBJECT_DOMAIN
        FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
        WHERE REFERENCED_DATABASE = %s
          AND REFERENCED_OBJECT_DOMAIN = %s
          AND REFERENCED_OBJECT_NAME   = %s
    """
    cur.execute(sql, (target_db, domain, obj))
    for db_, schema_, name_, dom_ in cur.fetchall():
        blocking.append((domain, obj, db_, schema_, name_, dom_))

cur.close()
ctx.close()

# 5. Report and fail if any blocking dependencies found
if blocking:
    print("\n❌ Cannot proceed—these downstream dependencies would break:\n")
    for dom, obj, db_, schema_, name_, dom_ in blocking:
        print(f" • {name_} ({dom_}) in {db_}.{schema_} depends on {dom}.{obj}")
    exit(1)

print("\n✅ No blocking dependencies found. You’re safe to proceed!")
exit(0)
