import snowflake.connector, os

ctx = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    role=os.environ['SNOWFLAKE_ROLE']
)

def check_dependencies(schema):
    print(f"🔎 Checking dependencies for schema: {schema}")
    cur = ctx.cursor()
    try:
        cur.execute(f"""
            SELECT 
                referencing_object_name, referencing_object_type,
                referenced_object_name, referenced_object_type
            FROM {schema}.INFORMATION_SCHEMA.OBJECT_DEPENDENCIES
            WHERE referenced_object_type IN ('TABLE', 'VIEW')
        """)
        rows = cur.fetchall()
        for r in rows:
            print(f"❗ {r[0]} ({r[1]}) depends on {r[2]} ({r[3]})")
        if rows:
            raise RuntimeError("Dependency conflicts detected; halt deployment.")
    finally:
        cur.close()

# Discover schemas from both folders
schemas = set()
for folder in ['snowflake/setup', 'snowflake/migrations']:
    for fname in os.listdir(folder):
        if fname.endswith('.sql'):
            parts = fname.split('__')
            if len(parts) >= 3:
                schemas.add(parts[1])

for schema in schemas:
    check_dependencies(schema.upper())

ctx.close()
