import os
import re

# Directories to scan
SQL_FOLDERS = ['snowflake/setup', 'snowflake/migrations']

# Destructive patterns to warn on (no blocking)
WARN_PATTERNS = [
    r'DROP\s+TABLE',
    r'DROP\s+COLUMN',
    r'TRUNCATE\s+TABLE',
    r'ALTER\s+TABLE\s+\S+\s+DROP\s+COLUMN'
]


def scan_sql():
    warnings = []
    for folder in SQL_FOLDERS:
        for filename in os.listdir(folder):
            if not filename.lower().endswith('.sql'):
                continue
            path = os.path.join(folder, filename)
            sql = open(path, 'r').read()
            for pattern in WARN_PATTERNS:
                if re.search(pattern, sql, re.IGNORECASE):
                    warnings.append(f"{folder}/{filename}: matched '{pattern}'")
    return warnings


def main():
    warnings = scan_sql()

    if warnings:
        print("⚠️  SQL warnings detected (no blocking patterns):")
        for w in warnings:
            print(f"  - {w}")
    else:
        print("✅ No destructive SQL patterns detected.")

    # Always succeed
    exit(0)

if __name__ == '__main__':
    main()
