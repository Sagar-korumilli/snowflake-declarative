import os
import shutil

# Path to your flat model files
base_dir = "snowflake/models"
files = [f for f in os.listdir(base_dir) if f.endswith(".sql")]

for file in files:
    schema_name = os.path.splitext(file)[0]  # e.g., admin.sql → admin
    src_path = os.path.join(base_dir, file)
    
    # Create target directory: models/admin/tables/
    target_dir = os.path.join(base_dir, schema_name, "tables")
    os.makedirs(target_dir, exist_ok=True)

    # Rename the SQL file inside the new location
    dst_path = os.path.join(target_dir, f"{schema_name}_table.sql")
    
    shutil.move(src_path, dst_path)
    print(f"Moved {file} → {dst_path}")
