#!/usr/bin/env python3
import os
import snowflake.connector
from datetime import datetime, timezone

# ── 1) Mirror exactly the columns in ACCOUNT_USAGE.OBJECT_DEPENDENCIES ───────
COLUMNS = [
    "REFERENCED_DATABASE",
    "REFERENCED_SCHEMA",
    "REFERENCED_OBJECT_NAME",
    "REFERENCED_OBJECT_ID",
    "REFERENCED_OBJECT_DOMAIN",
    "REFERENCING_DATABASE",
    "REFERENCING_SCHEMA",
    "REFERENCING_OBJECT_NAME",
    "REFERENCING_OBJECT_ID",
    "REFERENCING_OBJECT_DOMAIN",
    "DEPENDENCY_TYPE"               # <-- corrected
]

# ── 2) Your local table to store them ────────────────────────────────────────
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
    # Use VARCHAR for everything; you can tighten types later if you like
    cols = [f"{col} VARCHAR" for col in COLUMNS]
    cols.append("LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (\n  "
        + ",\n  ".join(cols)
        + "\n);"
    )
    cur.execute(ddl)

def get_last_loaded(cur):
    cur.execute(f"SELECT MAX(LOADED_AT) FROM {METADATA_TABLE}")
    last = cur.fetchone()[0]
    return last or datetime(1970,1,1,tzinfo=timezone.utc)

def fetch_incremental(cur, since_ts):
    # Filter on CREATED (which does exist) rather than a non‑existent column
    sql = f"""
      SELECT {', '.join(COLUMNS)}
        FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
       WHERE CREATED > %s
    """
    cur.execute(sql, (since_ts,))
    return cur.fetchall()

def load_rows(cur, rows):
    if not rows:
        print("✔ No new dependencies.")
        return
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    ins = (
        f"INSERT INTO {METADATA_TABLE} ({', '.join(COLUMNS)}) "
        f"VALUES ({placeholders})"
    )
    cur.executemany(ins, rows)
    print(f"✔ Inserted {cur.rowcount} rows.")

def main():
    # Validate required env vars
    required = [
        "SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE","SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE"
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise SystemExit(f"Missing env vars: {missing}")

    conn = get_conn()
    cur = conn.cursor()
    try:
        ensure_table(cur)
        last = get_last_loaded(cur)
        print(f"Loading dependencies CREATED after {last.isoformat()}")
        new_rows = fetch_incremental(cur, last)
        load_rows(cur, new_rows)
        conn.commit()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
