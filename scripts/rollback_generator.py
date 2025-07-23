#!/usr/bin/env python3
import os
import re
from pathlib import Path
import snowflake.connector

# Folders containing your setup and migration SQL files
FOLDERS = ['snowflake/setup', 'snowflake/migrations']
# Folder where rollback scripts will be written
ROLLBACK_FOLDER = 'snowflake/rollback'
Path(ROLLBACK_FOLDER).mkdir(parents=True, exist_ok=True)

# Read Snowflake connection details from env vars
SF_ACCOUNT   = os.environ['SNOWFLAKE_ACCOUNT']
SF_USER      = os.environ['SNOWFLAKE_USER']
SF_PASSWORD  = os.environ['SNOWFLAKE_PASSWORD']
SF_ROLE      = os.environ['SNOWFLAKE_ROLE']
SF_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']
SF_DATABASE  = os.environ['SNOWFLAKE_DATABASE']
SF_SCHEMA    = os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')

# Connect to Snowflake for metadata lookups
def get_conn():
    return snowflake.connector.connect(
        account   = SF_ACCOUNT,
        user      = SF_USER,
        password  = SF_PASSWORD,
        role      = SF_ROLE,
        warehouse = SF_WAREHOUSE,
        database  = SF_DATABASE,
        schema    = SF_SCHEMA
    )

# Query INFORMATION_SCHEMA for original column types
def get_column_type(conn, full_table, column):
    schema, table = full_table.split('.', 1)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM {SF_DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (schema, table, column))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    dt, char_len, prec, scale = row
    dt = dt.upper()
    if dt in ('VARCHAR','CHAR','TEXT') and char_len:
        return f"{dt}({char_len})"
    if dt in ('NUMBER','DECIMAL','NUMERIC') and prec is not None:
        return f"{dt}({prec},{scale})"
    return dt

# Invert common DDL to create rollback statements
def generate_rollback(sql, folder_type, conn):
    stmts = []
    # 1) ADD → DROP
    for tbl, col in re.findall(r'ALTER\s+TABLE\s+(\S+)\s+ADD\s+COLUMN\s+(\S+)', sql, re.IGNORECASE):
        stmts.append(f"ALTER TABLE {tbl} DROP COLUMN {col};")
    # 2) DROP → ADD (with exact type)
    for tbl, col in re.findall(r'ALTER\s+TABLE\s+(\S+)\s+DROP\s+COLUMN\s+IF\s+EXISTS\s+(\S+)', sql, re.IGNORECASE):
        typ = get_column_type(conn, tbl, col) or '<ORIGINAL_TYPE>'
        stmts.append(f"ALTER TABLE {tbl} ADD COLUMN {col} {typ};")
    # 3) SET DATA TYPE → revert
    for tbl, col, _ in re.findall(r'ALTER\s+TABLE\s+(\S+)\s+ALTER\s+COLUMN\s+(\S+)\s+SET\s+DATA\s+TYPE', sql, re.IGNORECASE):
        typ = get_column_type(conn, tbl, col) or '<ORIGINAL_TYPE>'
        stmts.append(f"ALTER TABLE {tbl} ALTER COLUMN {col} SET DATA TYPE {typ};")
    # 4) RENAME → swap
    for tbl, old, new in re.findall(r'ALTER\s+TABLE\s+(\S+)\s+RENAME\s+COLUMN\s+(\S+)\s+TO\s+(\S+);', sql, re.IGNORECASE):
        stmts.append(f"ALTER TABLE {tbl} RENAME COLUMN {new} TO {old};")
    # 5) CREATE OR REPLACE → DROP
    for obj, name in re.findall(r'CREATE\s+OR\s+REPLACE\s+(TABLE|VIEW|FUNCTION|PROCEDURE|SEQUENCE|STAGE|FILE FORMAT)\s+(\S+)', sql, re.IGNORECASE):
        stmts.append(f"DROP {obj.upper()} IF EXISTS {name};")
    # 6) full_setup teardown
    if folder_type=='setup' and 'full_setup.sql' in sql.lower():
        m = re.search(r'CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+(\S+)', sql, re.IGNORECASE)
        if m:
            stmts.append(f"DROP SCHEMA IF EXISTS {m.group(1)};")
    return stmts

if __name__=='__main__':
    conn = get_conn()
    for folder in FOLDERS:
        typ = 'setup' if 'setup' in folder else 'migrations'
        for f in sorted(Path(folder).glob('*.sql')):
            sql = f.read_text()
            rb = generate_rollback(sql, typ, conn)
            if rb:
                out = Path(ROLLBACK_FOLDER)/f"rollback_{typ}__{f.name}"
                out.write_text('-- AUTO-GENERATED ROLLBACK\n\n'+"\n".join(rb)+"\n")
                print(f"✅ Generated rollback for {typ}/{f.name}")
            else:
                print(f"⚠️ No rollback logic for {typ}/{f.name}")
    conn.close()
