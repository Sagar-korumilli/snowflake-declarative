import os, re

FOLDERS = ['snowflake/setup', 'snowflake/migrations']
ROLLBACK_FOLDER = 'rollback'
os.makedirs(ROLLBACK_FOLDER, exist_ok=True)

def generate_rollback(file_path, folder_type):
    with open(file_path, 'r') as f:
        sql = f.read()
    filename = os.path.basename(file_path)
    rollback_lines = []

    # Column additions
    adds = re.findall(r'ALTER TABLE (\S+) ADD COLUMN (\S+)', sql, re.IGNORECASE)
    for table, column in adds:
        rollback_lines.append(f'ALTER TABLE {table} DROP COLUMN {column};')

    # CREATE OR REPLACE objects
    if re.search(r'CREATE OR REPLACE', sql, re.IGNORECASE):
        m = re.search(r'CREATE OR REPLACE (TABLE|VIEW|FUNCTION|PROCEDURE) (\S+)', sql, re.IGNORECASE)
        if m:
            obj_type, obj_name = m.groups()
            rollback_lines.append(f'DROP {obj_type.upper()} {obj_name};')

    # For setup folder you might also want to DROP entire schema if it's a full-setup
    if folder_type == 'setup' and filename.lower().endswith('full_setup.sql'):
        # Assuming your file name format V###__schema__full_setup.sql
        schema = filename.split('__')[1]
        rollback_lines.append(f'DROP SCHEMA IF EXISTS {schema};')

    if rollback_lines:
        out = os.path.join(ROLLBACK_FOLDER, f"rollback_{folder_type}_{filename}")
        with open(out, 'w') as f:
            f.write('\n'.join(rollback_lines))
        print(f"✅ Generated {folder_type} rollback for {filename}")
    else:
        print(f"⚠️ No rollback logic for {folder_type}/{filename}")

for folder in FOLDERS:
    typ = 'setup' if 'setup' in folder else 'migrations'
    for fname in sorted(os.listdir(folder)):
        if fname.endswith('.sql'):
            generate_rollback(os.path.join(folder, fname), typ)
