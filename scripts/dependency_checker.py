import os
import re
import sys
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# --- Configuration ---

# Folders containing SQL files to scan
SQL_FOLDERS = ['snowflake/setup', 'snowflake/migrations']

# Regex to extract schema name from the filename (e.g., V1__MYSCHEMA__description.sql)
SCHEMA_FROM_FILENAME_RE = re.compile(r'__([a-zA-Z0-9_]+)__', re.IGNORECASE)

# Patterns to identify statements that drop or alter objects.
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
    # For DROP COLUMN, we check dependencies on the TABLE itself.
    'ALTER_TABLE_DROP_COLUMN': [
        re.compile(r'\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s]+)\s+DROP\s+COLUMN\s+([^\s;]+)', re.IGNORECASE)
    ]
}

# --- Helper Functions ---

def parse_fully_qualified_name(name_str, default_db, default_schema):
    """
    Parses an object identifier string into its components, applying defaults.
    """
    parts = name_str.upper().replace('"', '').split('.')
    if len(parts) == 3:
        return (parts[0], parts[1], parts[2])  # DB, SCHEMA, NAME
    elif len(parts) == 2:
        return (default_db, parts[0], parts[1]) # DB (default), SCHEMA, NAME
    elif len(parts) == 1:
        if not default_schema:
            return (None, None, None) # Cannot determine schema
        return (default_db, default_schema, parts[0]) # DB (default), SCHEMA (default), NAME
    
    return (None, None, None) # Invalid format

# --- Main Logic ---

def main():
    """Main execution function."""
    target_db = os.environ.get('SNOWFLAKE_DATABASE')
    if not target_db:
        print("❌ Environment variable SNOWFLAKE_DATABASE is not set.")
        sys.exit(1)

    print("Step 1: Parsing SQL files for destructive operations...")
    impacted_objects = {}

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
                        if domain == 'ALTER_TABLE_DROP_COLUMN':
                            object_domain = 'TABLE'
                            unparsed_name = match.group(1)
                        else:
                            object_domain = domain
                            unparsed_name = match.group(1)

                        db, schema, name = parse_fully_qualified_name(unparsed_name, target_db.upper(), default_schema)

                        if not all([db, schema, name]):
                            print(f"❌ Could not fully qualify object name '{unparsed_name}' from file '{fname}'.")
                            print("   Ensure the object name is fully qualified or the file has a schema in its name (e.g., __MYSCHEMA__).")
                            sys.exit(1)

                        key = (object_domain, db, schema, name)
                        if key not in impacted_objects:
                            impacted_objects[key] = []
                        impacted_objects[key].append(fname)

    if not impacted_objects:
        print("\n✅ No destructive operations found. Skipping dependency check.")
        sys.exit(0)

    print(f"\nFound {len(impacted_objects)} potentially destructive operation(s).")
    print("Step 2: Checking for downstream dependencies using INFORMATION_SCHEMA (real-time)...")

    try:
        ctx = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
            role=os.environ['SNOWFLAKE_ROLE'],
            database=target_db
        )
        cur = ctx.cursor()
    except Exception as e:
        print(f"❌ Failed to connect to Snowflake: {e}")
        sys.exit(1)

    blocking_dependencies = []

    for (domain, db, schema, name), files in impacted_objects.items():
        print(f"Checking: {domain} {db}.{schema}.{name} (from {', '.join(files)})")
        
        query = f"""
            SELECT
                REFERENCING_DATABASE,
                REFERENCING_SCHEMA,
                REFERENCING_OBJECT_NAME,
                REFERENCING_OBJECT_DOMAIN
            FROM TABLE({target_db}.INFORMATION_SCHEMA.OBJECT_DEPENDENCIES(
                    OBJECT_DOMAIN => '{domain}',
                    OBJECT_NAME => '{db}.{schema}.{name}'
            ))
            WHERE REFERENCING_OBJECT_ID != REFERENCED_OBJECT_ID
        """

        try:
            cur.execute(query)
            
            for dep_db, dep_schema, dep_name, dep_domain in cur.fetchall():
                blocking_dependencies.append({
                    "dropped_domain": domain,
                    "dropped_name": f"{db}.{schema}.{name}",
                    "dependent_domain": dep_domain,
                    "dependent_name": f"{dep_db}.{dep_schema}.{dep_name}"
                })
        except ProgrammingError as e:
            if "does not exist or not authorized" in str(e):
                print(f"   -> Info: Object {db}.{schema}.{name} does not exist. No dependencies to check.")
                continue
            
            print(f"\n❌ A database error occurred: {e}")
            print(f"   Please ensure the role has USAGE privilege on database '{target_db}'.")
            cur.close()
            ctx.close()
            sys.exit(1)

    # Create a set of fully-qualified names for all objects being dropped for easy lookup.
    # e.g., {'TABLE.PROD.PUBLIC.MY_TABLE', 'VIEW.PROD.PUBLIC.MY_VIEW'}
    dropped_object_keys = {
        f"{domain}.{db}.{schema}.{name}" for (domain, db, schema, name) in impacted_objects.keys()
    }

    # Filter out dependencies that are also being dropped in this same run.
    truly_blocking_dependencies = []
    for dep in blocking_dependencies:
        # Construct the key for the dependent object to check against the drop list.
        dependent_key = f"{dep['dependent_domain']}.{dep['dependent_name'].upper()}"
        
        if dependent_key not in dropped_object_keys:
            truly_blocking_dependencies.append(dep)

    cur.close()
    ctx.close()

    if truly_blocking_dependencies:
        print("\n========================= ❌ DANGER ❌ =========================")
        print("Execution stopped. Found blocking downstream dependencies:")
        for dep in truly_blocking_dependencies:
            print(f" • Object '{dep['dependent_name']}' ({dep['dependent_domain']}) depends on '{dep['dropped_name']}' which is being dropped or modified.")
        print("=================================================================")
        sys.exit(1)

    print("\n✅ Success! No blocking dependencies found. It is safe to proceed.")
    sys.exit(0)

if __name__ == "__main__":
    main()
