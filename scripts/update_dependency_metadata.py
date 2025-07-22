#!/usr/bin/env python3
import os
import snowflake.connector
from datetime import datetime, timezone

# --- Configuration ---
# Columns to mirror from ACCOUNT_USAGE.OBJECT_DEPENDENCIES
COLUMNS = [
    "OBJECT_ID",
    "OBJECT_DATABASE",
    "OBJECT_SCHEMA",
    "OBJECT_NAME",
    "OBJECT_DOMAIN",
    "DEPENDS_ON_ID",
    "DEPENDS_ON_DATABASE",
    "DEPENDS_ON_SCHEMA",
    "DEPENDS_ON_NAME",
    "DEPENDS_ON_DOMAIN",
    "CREATED",
    "LAST_ALTERED"
]

# Full name of your metadata table (schema.table)
METADATA_TABLE = os.getenv("METADATA_TABLE", "PUBLIC.DEPENDENCY_METADATA")


def get_conn():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )


def ensure_table(cur):
    ddl_cols = []
    for col in COLUMNS:
        typ = "TIMESTAMP_NTZ" if col in ("CREATED", "LAST_ALTERED") else "VARCHAR"
        ddl_cols.append(f"{col} {typ}")
    ddl_cols.append("LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (\n  "
        + ",\n  ".join(ddl_cols)
        + "\n);"
    )


def get_last_ts(cur):
    cur.execute(f"SELECT MAX(LAST_ALTERED) FROM {METADATA_TABLE}")
    row = cur.fetchone()
    return row[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)


def fetch_incremental(cur, since_ts):
    sql = (
        f"SELECT {', '.join(COLUMNS)} "
        f"FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES "
        f"WHERE LAST_ALTERED > %s"
    )
    cur.execute(sql, (since_ts,))
    return cur.fetchall()


def load_rows(cur, rows):
    if not rows:
        print("✔ No new dependency rows.")
        return
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    ins = (
        f"INSERT INTO {METADATA_TABLE} ({', '.join(COLUMNS)}) "
        f"VALUES ({placeholders})"
    )
    cur.executemany(ins, rows)
    print(f"✔ Inserted {cur.rowcount} new rows.")


def main():
    # Validate required env vars
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise SystemExit(f"Missing env vars: {missing}")

    conn = get_conn()
    cur = conn.cursor()
    try:
        ensure_table(cur)
        last_ts = get_last_ts(cur)
        print(f"Loading rows with LAST_ALTERED > {last_ts.isoformat()}")
        rows = fetch_incremental(cur, last_ts)
        load_rows(cur, rows)
        conn.commit()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
