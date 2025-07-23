import os
import re
import subprocess
from datetime import datetime
import snowflake.connector
from pathlib import Path
from git import Repo

# ------------------ ENV VARS ------------------ #
account = os.environ["SNOWFLAKE_ACCOUNT"]
user = os.environ["SNOWFLAKE_USER"]
password = os.environ["SNOWFLAKE_PASSWORD"]
role = os.environ["SNOWFLAKE_ROLE"]
warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
database = os.environ["SNOWFLAKE_DATABASE"]

# Optional default schema if none found
default_schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")

# ------------------ FUNCTIONS ------------------ #

def get_changed_sql_files(migrations_folder="snowflake/migrations"):
    """Return list of changed .sql files in the migrations folder."""
    repo = Repo(".")
    changed_files = []
    for item in repo.index.diff("HEAD"):
        if item.a_path.startswith(migrations_folder) and item.a_path.endswith(".sql"):
            changed_files.append(item.a_path)
    return changed_files

def extract_table_names_from_sql(sql_text):
    """Extract schema and table names from SQL statements."""
    pattern = r"(ALTER|DROP|TRUNCATE)\s+TABLE\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)"
    matches = re.findall(pattern, sql_text, flags=re.IGNORECASE)
    return [match[1] for match in matches]

def run_clone(cursor, schema, table):
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    backup_name = f"{table}_backup_{timestamp}"
    full_src = f"{schema}.{table}"
    full_backup = f"{schema}.{backup_name}"

    try:
        cursor.execute(f"CREATE OR REPLACE TABLE {full_backup} CLONE {full_src}")
        print(f"✅ Cloned {full_src} → {full_backup} (7-day retention simulated)")
    except Exception as e:
        print(f"❌ Failed to clone {full_src}: {e}")

# ------------------ MAIN ------------------ #

def main():
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database
    )
    cursor = conn.cursor()

    changed_files = get_changed_sql_files()
    print(f"🔍 Changed SQL files: {changed_files}")
    if not changed_files:
        print("✅ No changes in migration SQL files, skipping clone.")
        return

    tables_to_clone = set()
    for file in changed_files:
        with open(file, "r") as f:
            content = f.read()
            tables = extract_table_names_from_sql(content)
            for full_name in tables:
                if "." in full_name:
                    schema, table = full_name.split(".")
                else:
                    schema = default_schema
                    table = full_name
                tables_to_clone.add((schema, table))

    if not tables_to_clone:
        print("✅ No destructive SQL (ALTER/DROP/TRUNCATE) found.")
        return

    print(f"🔁 Cloning impacted tables...")
    for schema, table in tables_to_clone:
        run_clone(cursor, schema, table)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
