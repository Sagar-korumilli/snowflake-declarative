import os
import re
import tempfile
from pathlib import Path
import snowflake.connector


def get_snowflake_connection():
    private_key = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as key_file:
        key_file.write(private_key.encode())
        key_path = key_file.name

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=key_path,
        private_key_password=os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"]
    )
    return conn


def get_current_ddl(conn, object_type: str, full_name: str) -> str:
    with conn.cursor() as cur:
        cur.execute(f"SELECT GET_DDL('{object_type}', '{full_name}', TRUE)")
        return cur.fetchone()[0]


def replace_object(sql: str, object_type: str, full_name: str, new_ddl: str) -> str:
    pattern = rf"(?i)(CREATE\s+(?:OR\s+REPLACE\s+)?{object_type}\s+{re.escape(full_name)}\s+.*?;)"
    return re.sub(pattern, new_ddl.strip() + ";", sql, flags=re.DOTALL)


def update_setup_with_changes(schema_path: Path, changed_file: Path, conn):
    setup_file = next(schema_path.glob("V001__*.sql"))
    original_sql = setup_file.read_text()
    changed_sql = changed_file.read_text()
    updated_sql = original_sql

    # Process ALTER TABLES
    for sch, tbl in re.findall(r'ALTER\s+TABLE\s+(\w+)\.(\w+)', changed_sql, re.IGNORECASE):
        full_name = f"{sch}.{tbl}"
        ddl = get_current_ddl(conn, "TABLE", full_name)
        updated_sql = replace_object(updated_sql, "TABLE", full_name, ddl)

    # Process CREATE OR REPLACE objects (view, stage, sequence, file format)
    for obj_type, sch, name in re.findall(r'CREATE\s+OR\s+REPLACE\s+(VIEW|SEQUENCE|FILE FORMAT|STAGE)\s+(\w+)\.(\w+)', changed_sql, re.IGNORECASE):
        full_name = f"{sch}.{name}"
        ddl = get_current_ddl(conn, obj_type.upper(), full_name)
        updated_sql = replace_object(updated_sql, obj_type.upper(), full_name, ddl)

    # Write to backup/
    backup_dir = schema_path / "backup"
    backup_dir.mkdir(exist_ok=True)
    backup_path = backup_dir / setup_file.name
    backup_path.write_text(updated_sql)

    print(f"✅ Backup created at {backup_path}")
    print(f"📄 Content:\n{'-'*60}\n{updated_sql}\n{'-'*60}")


def find_changed_sql_files(sf_root: str) -> list:
    changed = []
    for schema_dir in Path(sf_root).iterdir():
        if schema_dir.is_dir() and schema_dir.name != "rollback":
            for sql_file in sorted(schema_dir.glob("V[0-9]*__*.sql")):
                if not sql_file.name.startswith("V001__"):
                    changed.append(sql_file)
    return changed


def main():
    sf_root = os.environ.get("SNOWFLAKE_ROOT", "snowflake")
    changed_files = find_changed_sql_files(sf_root)
    if not changed_files:
        print("✅ No changed SQL files; skipping backups.")
        return

    print(f"🔍 Changed SQL files: {[str(f) for f in changed_files]}")
    conn = get_snowflake_connection()

    for changed_file in changed_files:
        schema = changed_file.parts[1]  # e.g., "hr"
        schema_path = Path(sf_root) / schema
        update_setup_with_changes(schema_path, changed_file, conn)


if __name__ == "__main__":
    main()
