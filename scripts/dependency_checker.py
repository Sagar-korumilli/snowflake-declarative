import os
import snowflake.connector

# Connect (database + schema on connect doesn’t matter for ACCOUNT_USAGE queries)
ctx = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    role=os.environ['SNOWFLAKE_ROLE']
)

def check_dependencies(database, schema):
    print(f"🔎 Checking dependencies on {database}.{schema}")
    cur = ctx.cursor()
    try:
        # Use correct column names
        cur.execute(f"""
            SELECT
                REFERENCING_DATABASE,
                REFERENCING_SCHEMA,
                REFERENCING_OBJECT_NAME,
                REFERENCING_OBJECT_DOMAIN,
                REFERENCED_OBJECT_NAME,
                REFERENCED_OBJECT_DOMAIN
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
            WHERE REFERENCED_DATABASE = %s
              AND REFERENCED_SCHEMA   = %s
              AND REFERENCED_OBJECT_DOMAIN IN ('TABLE','VIEW')
        """, (database, schema))
        rows = cur.fetchall()
        if rows:
            print("❌ Detected blocking dependencies:")
            for r in rows:
                print(f"  · {r[2]} ({r[3]}) in {r[0]}.{r[1]} depends on {r[4]} ({r[5]})")
            raise RuntimeError("Please resolve these before proceeding.")
        else:
            print("✅ No blocking dependencies found.")
    finally:
        cur.close()

# Discover schemas from your SQL filenames
schemas = set()
for folder in ['snowflake/setup', 'snowflake/migrations']:
    for fname in os.listdir(folder):
        if not fname.endswith('.sql'):
            continue
        parts = fname.split('__')
        if len(parts) >= 3:
            schemas.add(parts[1].upper())

target_db = os.environ['SNOWFLAKE_DATABASE']
for schema in schemas:
    check_dependencies(target_db, schema)

ctx.close()
