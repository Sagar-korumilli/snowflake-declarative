import re
import os
import sys
from datetime import datetime
import snowflake.connector

# 1. parse args: schema name, migration filename
schema = sys.argv[1]          # e.g. "my_schema"
migration_file = sys.argv[2]  # e.g. "V002__add_column_to_orders.sql"

# 2. scan migration for object names
pattern = re.compile(r'(TABLE|VIEW)\s+(\w+\.\w+)', re.IGNORECASE)
with open(migration_file) as f:
    text = f.read()
objects = {m.group(2) for m in pattern.finditer(text)}

# 3. connect to Snowflake & fetch current DDL
ctx = snowflake.connector.connect(**{
    'account': os.environ['SF_ACCOUNT'],
    'user':    os.environ['SF_USER'],
    'password':os.environ['SF_PWD'],
    'role':    os.environ['SF_ROLE'],
    'warehouse':os.environ['SF_WH'],
    'database':os.environ['SF_DB'],
})
cur = ctx.cursor()
ddl_map = {}
for obj in objects:
    obj_type = "TABLE" if ".TABLE." not in obj.upper() else "VIEW"
    cur.execute(f"SHOW CREATE {obj_type} {obj}")
    ddl_map[obj] = cur.fetchone()[1]  # the CREATE statement

# 4. read initial setup into memory
initial_path = os.path.join(os.path.dirname(migration_file), 'V001__initial_setup.sql')
with open(initial_path) as f:
    base = f.read()

# 5. replace each block between your markers
for obj, create_ddl in ddl_map.items():
    name = obj.split('.')[-1]
    begin = re.escape(f"-- ### BEGIN {name}")
    end   = re.escape(f"-- ### END {name}")
    block_re = re.compile(begin + r'.*?' + end, re.DOTALL)
    new_block = f"-- ### BEGIN {name}\n{create_ddl.strip()}\n-- ### END {name}"
    base = block_re.sub(new_block, base)

# 6. write out to backup folder
ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
backup_dir = os.path.join(os.path.dirname(initial_path), 'backup')
os.makedirs(backup_dir, exist_ok=True)
out_path = os.path.join(backup_dir, f"V001__initial_setup_backup_{ts}.sql")
with open(out_path, 'w') as f:
    f.write(base)

print(f"Written updated baseline to {out_path}")
