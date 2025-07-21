import snowflake.connector, os

# Establish connection to the Snowflake database (already sets the database)
ctx = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],  # e.g. MY_DB
    role=os.environ['SNOWFLAKE_ROLE']
)

def check_dependencies(schema_name):
    print(f"🔎 Checking dependencies on schema: {schema_name}")
    cur = ctx.cursor()
    try:
        cur.execute(f"""
            SELECT 
                referencing_object_name,
                referencing_object_type,
                referenced_object_name,
                referenced_object_type
            FROM {os.environ['SNOWFLAKE_DATABASE']}.INFORMATION_SCHEMA.OBJECT_DEPENDENCIES
            WHERE referenced_schema_name = %s
              AND referenced_object_type IN ('TABLE', 'VIEW');
        """, (schema_name,))
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"❗ {r[0]} ({r[1]}) depends on {r[2]} ({r[3]})")
            raise RuntimeError("❌ Dependency conflicts detected; halt deployment.")
        else:
            print("✅ No dependency conflicts found.")
    finally:
        cur.close()

# Discover schemas by filename as before
schemas = set()
for folder in ['snowflake/setup', 'snowflake/migrations']:
    for fname in os.listdir(folder):
        if not fname.endswith('.sql'):
            continue
        parts = fname.split('__')
        if len(parts) >= 3:
            schemas.add(parts[1])

for schema in schemas:
    check_dependencies(schema.upper())

ctx.close()
