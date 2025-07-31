import argparse
import os
import sys
import tempfile
import snowflake.connector


def get_snowflake_connection():
    # ensure required env vars exist
    required = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]
    for var in required:
        if not os.getenv(var):
            print(f"❌ Missing environment variable: {var}", file=sys.stderr)
            sys.exit(1)

    # write raw PEM from env into a temp file
    key_content = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.pem') as key_file:
        key_file.write(key_content)
        key_path = key_file.name
    os.chmod(key_path, 0o600)

    try:
        ctx = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            private_key_file=key_path,
            private_key_file_pwd=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
            authenticator='snowflake_jwt',
            autocommit=True
        )
        return ctx
    except Exception as e:
        os.remove(key_path)
        print(f"❌ Failed to connect to Snowflake: {e}", file=sys.stderr)
        sys.exit(1)


def rollback_table(conn, database, schema, table, timestamp):
    cursor = conn.cursor()
    qualified = f"{database}.{schema}.{table}"
    backup = f"{database}.{schema}.{table}_backup_for_rollback"

    try:
        print(f"Creating backup table {backup} from {qualified} at timestamp {timestamp}...")
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {backup} AS
            SELECT * FROM {qualified} AT (TIMESTAMP => '{timestamp}');
        """)

        print(f"Restoring main table {qualified} from backup {backup}...")
        cursor.execute(f"TRUNCATE TABLE {qualified};")
        cursor.execute(f"INSERT INTO {qualified} SELECT * FROM {backup};")

    finally:
        print(f"Dropping backup table {backup}...")
        cursor.execute(f"DROP TABLE IF EXISTS {backup};")
        cursor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Snowflake Time Travel Rollback Script with backup table using key file auth"
    )
    parser.add_argument("--timestamp", required=True,
                        help="Rollback timestamp (format YYYY-MM-DD HH24:MI:SS)")
    parser.add_argument("--database", required=True,
                        help="Snowflake database name")
    parser.add_argument("--schema", default="PUBLIC",
                        help="Schema name (default PUBLIC)")
    parser.add_argument("--tables", nargs='*', default=[],
                        help="Tables to rollback. If omitted, all in schema")
    args = parser.parse_args()

    # sanitize identifiers
    args.schema = args.schema.strip('"').upper()
    args.tables = [t.strip('"').upper() for t in args.tables]

    conn = get_snowflake_connection()

    if not args.tables:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog = '{args.database}'
              AND table_schema = '{args.schema}'
              AND table_type = 'BASE TABLE';
        """)
        args.tables = [r[0] for r in cur.fetchall()]
        cur.close()

    if not args.tables:
        print(f"No tables found in schema '{args.schema}' to rollback.")
        sys.exit(0)

    for table in args.tables:
        try:
            rollback_table(conn, args.database, args.schema, table, args.timestamp)
            print(f"✅ Rollback successful for table: {table}")
        except Exception as e:
            print(f"❌ Error rolling back {table}: {e}", file=sys.stderr)

    print("Rollback process completed.")
    conn.close()


if __name__ == "__main__":
    main()
