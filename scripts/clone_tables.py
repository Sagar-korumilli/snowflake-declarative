#!/usr/bin/env python3
import os
import re
import argparse
from datetime import datetime
import snowflake.connector

# Patterns to detect table operations
OPERATIONS = [
    r"ALTER\s+TABLE\s+(\S+)",
    r"TRUNCATE\s+TABLE\s+(\S+)",
    r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\S+)",
]

# Pull creds from your existing env vars
SF_ACCOUNT   = os.environ['SNOWFLAKE_ACCOUNT']
SF_USER      = os.environ['SNOWFLAKE_USER']
SF_PASSWORD  = os.environ['SNOWFLAKE_PASSWORD']
SF_ROLE      = os.environ['SNOWFLAKE_ROLE']
SF_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']
SF_DATABASE  = os.environ['SNOWFLAKE_DATABASE']

def get_conn():
    return snowflake.connector.connect(
        account   = SF_ACCOUNT,
        user      = SF_USER,
        password  = SF_PASSWORD,
        role      = SF_ROLE,
        warehouse = SF_WAREHOUSE,
        database  = SF_DATABASE,
        
    )

def parse_sql_for_tables(sql_text):
    tables = set()
    for patt in OPERATIONS:
        for tbl in re.findall(patt, sql_text, re.IGNORECASE):
            tbl = re.sub(r'[;(\s].*$', '', tbl)
            tables.add(tbl)
    return tables

def clone_table(conn, full_table):
    # Detect schema.table or assume default SF_SCHEMA
    if '.' in full_table:
        schema, table = full_table.split('.', 1)
    else:
        schema, table = SF_SCHEMA, full_table

    ts = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    backup_name = f"{schema}.{table}_backup_{ts}"
    cur = conn.cursor()
    try:
        # Zero‑copy clone into same schema
        cur.execute(f"CREATE OR REPLACE TABLE {backup_name} CLONE {schema}.{table}")
        # Enforce 7-day retention
        cur.execute(f"ALTER TABLE {backup_name} SET DATA_RETENTION_TIME_IN_DAYS = 1")
        print(f"✅ Cloned {schema}.{table} → {backup_name} (7‑day retention)")
    except Exception as e:
        print(f"⚠️ Failed to clone {schema}.{table}: {e}")
    finally:
        cur.close()

def main():
    parser = argparse.ArgumentParser(description="Zero‑copy clone impacted tables before DDL")
    parser.add_argument(
        '--migrations-folder', required=True,
        help='Path to your snowflake/migrations folder'
    )
    args = parser.parse_args()

    conn = get_conn()
    tables = set()

    # Aggregate all impacted tables
    for root, _, files in os.walk(args.migrations_folder):
        for fname in files:
            if fname.lower().endswith('.sql'):
                with open(os.path.join(root, fname)) as fh:
                    tables |= parse_sql_for_tables(fh.read())

    # Clone each one in its own schema
    for tbl in tables:
        clone_table(conn, tbl)

    conn.close()

if __name__ == '__main__':
    main()
