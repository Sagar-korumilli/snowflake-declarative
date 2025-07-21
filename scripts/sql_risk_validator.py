import os
import re

SQL_FOLDERS = ['snowflake/setup', 'snowflake/migrations']
BLOCK_PATTERNS = [
    r'DROP\s+TABLE',
    r'DROP\s+COLUMN',
    r'TRUNCATE\s+TABLE',
    r'ALTER\s+TABLE\s+\S+\s+DROP\s+COLUMN'
]

def is_risky(sql):
    for pattern in BLOCK_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE):
            return True, pattern
    return False, None

errors = []
for folder in SQL_FOLDERS:
    for filename in os.listdir(folder):
        if not filename.endswith('.sql'):
            continue
        path = os.path.join(folder, filename)
        with open(path, 'r') as f:
            sql = f.read()
        risky, pattern = is_risky(sql)
        if risky:
            errors.append(f"{folder}/{filename}: matched '{pattern}'")

if errors:
    print("❌ Risky SQL detected:")
    print("\n".join(errors))
    exit(1)

print("✅ All SQL files passed risk validation.")
