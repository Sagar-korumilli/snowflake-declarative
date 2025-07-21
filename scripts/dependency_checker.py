import os
import snowflake.connector

# Connect (database+schema don’t matter since we query ACCOUNT_USAGE)
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
        cur.execute(f"""
            SELECT 
                referencing_database_name,
                referencing_schema_name,
                referencing_object_name,
                referencing_object_type,
                referenced_object_name,
                referenced_object_type
            FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
            WHERE referenced_database_name = %s
              AND referenced_schema_name   = %s
              AND referenced_object_type IN ('TABLE','VIEW')
              -- you can also filter on referenced_object_name if desired
            """,
            (database, schema)
        )
        rows = cur.fetchall()
        if rows:
            print("❌ Detected existing dependencies:")
            for r in rows:
                print(f"  · {r[2]} ({r[3]}) in {r[0]}.{r[1]} depends on {r[4]} ({r[5]})")
            raise RuntimeError("Please resolve these before proceeding.")
        else:
            print("✅ No blocking dependencies found.")
    finally:
        cur.close()

# Discover schemas from filenames
schemas = set()
for folder in ['snowflake/setup', 'snowflake/migrations']:
    for fname in os.listdir(folder):
        if not fname.endswith('.sql'):
            continue
        parts = fname.split('__')
        if len(parts) >= 3:
            schemas.add(parts[1])

# Run check for each schema in your target database
target_db = os.environ['SNOWFLAKE_DATABASE']
for schema in schemas:
    check_dependencies(target_db, schema.upper())

ctx.close()
