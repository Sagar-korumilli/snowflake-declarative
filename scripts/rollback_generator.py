#!/usr/bin/env python3
import os
import re
import argparse
import snowflake.connector
from pathlib import Path

def get_column_type(conn, full_table, column):
    """
    Query INFORMATION_SCHEMA to get the data type of an existing column.
    full_table = 'schema.table'
    """
    schema, table = full_table.split('.', 1)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM {conn.database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (schema, table, column))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    dt, char_len, prec, scale = row
    if dt.upper() in ("VARCHAR","CHAR","TEXT") and char_len:
        return f"{dt}({char_len})"
    if dt.upper() in ("NUMBER","DECIMAL","NUMERIC") and prec is not None:
        return f"{dt}({prec},{scale})"
    return dt

def generate_rollback(sql, folder_type, conn):
    lines = []
    # 1) ADD COLUMN -> DROP COLUMN
    for tbl, col in re.findall(r'ALTER\s+TABLE\s+(\S+)\s+ADD\s+COLUMN\s+(\S+)', sql, re.IGNORECASE):
        lines.append(f"ALTER TABLE {tbl} DROP COLUMN {col};")

    # 2) DROP COLUMN -> ADD COLUMN <exact type>
    for tbl, col in re.findall(r'ALTER\s+TABLE\s+(\S+)\s+DROP\s+COLUMN\s+IF\s+EXISTS\s+(\S+)', sql, re.IGNORECASE):
        orig_type = get_column_type(conn, tbl, col) or "<ORIGINAL_TYPE>"
        lines.append(f"ALTER TABLE {tbl} ADD COLUMN {col} {orig_type};")

    # 3) MODIFY/SET DATA TYPE -> revert to old
    for tbl, col, newtype in re.findall(
        r'ALTER\s+TABLE\s+(\S+)\s+ALTER\s+COLUMN\s+(\S+)\s+SET\s+DATA\s+TYPE\s+([\w\(\), ]+);',
        sql, re.IGNORECASE
    ):
        orig_type = get_column_type(conn, tbl, col) or "<ORIGINAL_TYPE>"
        lines.append(f"ALTER TABLE {tbl} ALTER COLUMN {col} SET DATA TYPE {orig_type};")

    # 4) RENAME COLUMN -> swap old/new
    for tbl, old, new in re.findall(
        r'ALTER\s+TABLE\s+(\S+)\s+RENAME\s+COLUMN\s+(\S+)\s+TO\s+(\S+);',
        sql, re.IGNORECASE
    ):
        lines.append(f"ALTER TABLE {tbl} RENAME COLUMN {new} TO {old};")

    # 5) CREATE OR REPLACE X -> DROP X
    for obj, name in re.findall(
        r'CREATE\s+OR\s+REPLACE\s+(TABLE|VIEW|FUNCTION|PROCEDURE|SEQUENCE|STAGE|FILE FORMAT)\s+(\S+)',
        sql, re.IGNORECASE
    ):
        lines.append(f"DROP {obj.upper()} IF EXISTS {name};")

    # 6) Full‐setup teardown
    if folder_type == 'setup' and 'full_setup.sql' in sql.lower():
        # assume filename gives schema: V001__<schema>__full_setup.sql
        # but you can also extract from SQL if it does `CREATE SCHEMA`
        schema = re.search(r'CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+(\S+)', sql, re.IGNORECASE)
        if schema:
            lines.append(f"DROP SCHEMA IF EXISTS {schema.group(1)};")

    return lines

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--migrations-dir', required=True, help='root folder for setup|migrations')
    parser.add_argument('--rollback-dir',   required=True, help='where to write rollback SQL')
    parser.add_argument('--sf-account',     required=True)
    parser.add_argument('--sf-user',        required=True)
    parser.add_argument('--sf-password',    required=True)
    parser.add_argument('--sf-role',        required=True)
    parser.add_argument('--sf-warehouse',   required=True)
    parser.add_argument('--sf-database',    required=True)
    parser.add_argument('--sf-schema',      default='PUBLIC')
    args = parser.parse_args()

    # ensure output
    out_dir = Path(args.rollback_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # connect to Snowflake for metadata lookups
    conn = snowflake.connector.connect(
        account   = args.sf_account,
        user      = args.sf_user,
        password  = args.sf_password,
        role      = args.sf_role,
        warehouse = args.sf_warehouse,
        database  = args.sf_database,
        schema    = args.sf_schema
    )

    for sub in ('setup', 'migrations'):
        folder = Path(args.migrations_dir) / sub
        for sql_file in sorted(folder.glob('*.sql')):
            sql_txt = sql_file.read_text()
            rollback_stmts = generate_rollback(sql_txt, sub, conn)
            if rollback_stmts:
                out_name = f"R_{sub}__{sql_file.name}"
                (out_dir / out_name).write_text(
                    "-- AUTO-GENERATED ROLLBACK\n\n" +
                    "\n".join(rollback_stmts) + "\n"
                )
                print(f"✅ Generated rollback: {out_name}")
            else:
                print(f"⚠️ No rollback logic for {sub}/{sql_file.name}")

    conn.close()

if __name__ == '__main__':
    main()
