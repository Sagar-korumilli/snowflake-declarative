import argparse
import os
import sys
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

def get_snowflake_connection():
    # load env vars
    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    private_key_str = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    role = os.getenv("SNOWFLAKE_ROLE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")

    if not all([user, account, private_key_str, role, warehouse, database]):
        print("❌ Missing required Snowflake environment variables", file=sys.stderr)
        sys.exit(1)

    # load private key
    private_key = serialization.load_pem_private_key(
        private_key_str.encode('utf-8'),
        password=private_key_passphrase.encode() if private_key_passphrase else None,
        backend=default_backend()
    )

    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # connect
    ctx = snowflake.connector.connect(
        user=user,
        account=account,
        private_key=pkb,
        role=role,
        warehouse=warehouse,
        database=database,
        autocommit=True,
    )
    return ctx


def rollback_table(conn, database, schema, table, timestamp):
    cursor = conn.cursor()

    # Use unquoted identifiers (Snowflake uppercases unquoted)
    qualified_table = f"{database}.{schema}.{table}"
    backup_table = f"{database}.{schema}.{table}_backup_for_rollback"

    try:
        print(f"Creating backup table {backup_table} from {qualified_table} at timestamp {timestamp}...")
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {backup_table} AS
            SELECT * FROM {qualified_table} AT (TIMESTAMP => '{timestamp}');
        """)

        print(f"Restoring main table {qualified_table} from backup {backup_table}...")
        cursor.execute(f"TRUNCATE TABLE {qualified_table};")
        cursor.execute(f"INSERT INTO {qualified_table} SELECT * FROM {backup_table};")

    finally:
        print(f"Dropping backup table {backup_table}...")
        cursor.execute(f"DROP TABLE IF EXISTS {backup_table};")

    cursor.close()


def main():
    parser = argparse.ArgumentParser(description="Snowflake Time Travel Rollback Script with backup table")
    parser.add_argument("--timestamp", required=True, help="Rollback timestamp (format YYYY-MM-DD HH24:MI:SS)")
    parser.add_argument("--database", required=True, help="Snowflake database name")
    parser.add_argument("--schema", default="PUBLIC", help="Schema name (default PUBLIC)")
    parser.add_argument("--tables", nargs='*', default=[], help="List of tables to rollback. If omitted, all tables in schema are rolled back.")
    args = parser.parse_args()

    # sanitize inputs
    args.schema = args.schema.strip('"').upper()
    args.tables = [t.strip('"').upper() for t in args.tables]

    conn = get_snowflake_connection()

    # If no tables specified, retrieve all tables in the schema
    if not args.tables:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog = '{args.database}'
              AND table_schema = '{args.schema}'
              AND table_type = 'BASE TABLE';
        """)
        args.tables = [row[0] for row in cursor.fetchall()]
        cursor.close()

    if not args.tables:
        print(f"No tables found in schema '{args.schema}' to rollback.")
        conn.close()
        sys.exit(0)

    for table in args.tables:
        try:
            rollback_table(conn, args.database, args.schema, table, args.timestamp)
            print(f"Rollback successful for table: {table}")
        except Exception as e:
            print(f"❌ Error rolling back table {table}: {e}", file=sys.stderr)

    print("Rollback process completed.")
    conn.close()


if __name__ == "__main__":
    main()
