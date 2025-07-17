import os
import snowflake.connector
from git import Repo
from git.exc import InvalidGitRepositoryError, NoSuchPathError
import sys

# --- Environment Variables ---
# These are passed from the GitHub Actions workflow to the Python script
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
CURRENT_FAILED_COMMIT = os.environ.get('CURRENT_FAILED_COMMIT')
LAST_SUCCESSFUL_COMMIT = os.environ.get('LAST_SUCCESSFUL_COMMIT')

# --- Input Validation ---
if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE]):
    print("Error: Missing Snowflake environment variables for rollback.")
    sys.exit(1)

conn = None
cursor = None
try:
    # --- Establish Snowflake Connection ---
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE
    )
    cursor = conn.cursor()
    print("Successfully connected to Snowflake for rollback operations.")

    rollback_sqls = []

    # --- Step 1: Identify the last *successfully applied* migration from SCHEMA_CHANGE_HISTORY ---
    # This is the most reliable way to find the last good state.
    # IMPORTANT: For this to work robustly, you should enhance your deploy job
    # to update the SCHEMA_CHANGE_HISTORY table with the 'commit_hash'
    # of the deployed code upon successful completion.
    try:
        # Assuming a 'COMMIT_HASH' column exists in your SCHEMA_CHANGE_HISTORY table
        cursor.execute(f"SELECT SCRIPT_NAME, VERSION, COMMIT_HASH FROM {SNOWFLAKE_DATABASE}.PUBLIC.SCHEMA_CHANGE_HISTORY WHERE STATUS = 'SUCCESS' ORDER BY INSTALLED_ON DESC LIMIT 1;")
        last_successful_migration_info = cursor.fetchone()
        if last_successful_migration_info:
            print(f"Last successful migration: Script '{last_successful_migration_info[0]}', Version '{last_successful_migration_info[1]}', Commit: '{last_successful_migration_info[2]}'")
            # If the GITHUB_OUTPUT for LAST_SUCCESSFUL_COMMIT was empty, try to use this
            if not LAST_SUCCESSFUL_COMMIT or LAST_SUCCESSFUL_COMMIT == "null": # "null" can happen if output is empty
                LAST_SUCCESSFUL_COMMIT = last_successful_migration_info[2]
                print(f"Using last successful commit from DB history: {LAST_SUCCESSFUL_COMMIT}")
        else:
            print("No successful migrations found in SCHEMA_CHANGE_HISTORY. Cannot perform automated rollback based on DB history.")
            # If no successful history, we can't reliably rollback automatically.
            # We'll still proceed to check Git diff if LAST_SUCCESSFUL_COMMIT was provided by GH Actions.
    except Exception as e:
        print(f"Warning: Could not query SCHEMA_CHANGE_HISTORY for last successful commit: {e}.")
        print("Proceeding with Git-based comparison if LAST_SUCCESSFUL_COMMIT was provided by GitHub Actions, but this is less reliable.")
        last_successful_migration_info = None

    # --- Step 2: Generate rollback SQL by comparing the failed commit with the last successful one ---
    # This part is highly conceptual and needs a sophisticated "diff" tool for SQL DDL.
    # It's unlikely to be fully automatic for complex modifications without a dedicated DDL diff tool.
    print(f"Comparing current failed commit ({CURRENT_FAILED_COMMIT}) with last known successful commit ({LAST_SUCCESSFUL_COMMIT})...")
    
    if LAST_SUCCESSFUL_COMMIT and LAST_SUCCESSFUL_COMMIT != "null":
        try:
            repo = Repo(os.getcwd())
            
            # --- Automated Rollback Logic (Highly Conceptual) ---
            # This section needs to be fleshed out based on your specific needs.
            # For "modifications, revert to last successful deploy", this implies:
            # 1. Identifying the specific SQL files that were part of the *failed* deployment attempt.
            # 2. For each such file, determining its inverse DDL operation.

            # Example: If a new table or view was created in the failed commit:
            # Look for files in 'snowflake/migrations' and 'snowflake/setup' that are new in CURRENT_FAILED_COMMIT
            # compared to LAST_SUCCESSFUL_COMMIT.
            current_tree = repo.commit(CURRENT_FAILED_COMMIT).tree
            previous_tree = repo.commit(LAST_SUCCESSFUL_COMMIT).tree

            for folder in ['snowflake/setup', 'snowflake/migrations']:
                try:
                    current_files = {f.name for f in current_tree.join(folder).blobs if f.name.endswith('.sql')}
                    previous_files = {f.name for f in previous_tree.join(folder).blobs if f.name.endswith('.sql')}

                    newly_added_files = current_files - previous_files
                    for filename in newly_added_files:
                        # Attempt to parse filename to get schema and object name
                        parts = filename.split('__')
                        if len(parts) >= 3:
                            schema_name = parts[1].upper()
                            object_base_name = parts[2].replace('.sql', '').upper()
                            # Basic heuristic for tables/views based on naming convention
                            if 'TABLE' in object_base_name or 'VIEW' in object_base_name or 'FULL_SETUP' in object_base_name:
                                # This assumes the file creates a single, top-level object
                                # and the object name is derivable from the filename.
                                # You might need a more sophisticated parser here.
                                object_name = object_base_name.replace('FULL_SETUP', '').strip('_')
                                if object_name:
                                    rollback_sqls.append(f"DROP TABLE IF EXISTS {schema_name}.{object_name};")
                                    rollback_sqls.append(f"DROP VIEW IF EXISTS {schema_name}.{object_name};") # In case it was a view
                                    print(f"Generated DROP for newly added object: {schema_name}.{object_name}")
                except NoSuchPathError:
                    print(f"Warning: Folder '{folder}' not found in one of the commits during diff. Skipping.")
                except Exception as e:
                    print(f"Error during diffing new files in {folder}: {e}")

            # --- For modifications (ALTER statements): This is much harder ---
            # You would need to compare the content of *modified* SQL files
            # between the two commits and derive inverse ALTER statements.
            # This typically requires a dedicated DDL diffing tool or very specific,
            # pre-defined "down" scripts if you follow that pattern.
            # Since schemachange doesn't have "down" migrations, this would be custom.
            print("\n--- NOTE: Automated rollback for ALTER modifications is complex and requires custom DDL diffing logic. ---")
            print("   For complex ALTERs, manual intervention or a dedicated DDL comparison tool is often necessary.")
            print("   Consider fixing the failing script and re-deploying as the primary recovery method.")
            print("--------------------------------------------------------------------------------------------------\n")

        except InvalidGitRepositoryError:
            print("Error: Not a valid Git repository for rollback operations.")
            rollback_sqls.append("-- Git repository error during rollback. Manual intervention required.")
        except Exception as e:
            print(f"Error during Git comparison for rollback: {e}")
            rollback_sqls.append(f"-- Error during Git comparison for rollback: {e}. Manual intervention required.")
    else:
        print("LAST_SUCCESSFUL_COMMIT not determined or invalid. Cannot perform automated Git-based rollback.")
        rollback_sqls.append("-- Last successful commit not found or invalid. Automated rollback not possible based on Git diff.")

    # --- Execute generated (or hardcoded) rollback SQL ---
    if rollback_sqls:
        print("Executing generated rollback SQLs:")
        for sql in rollback_sqls:
            print(f"Executing: {sql}")
            try:
                cursor.execute(sql)
                print("SQL executed successfully.")
            except Exception as e:
                print(f"Error executing rollback SQL '{sql}': {e}")
                # Decide if you want to fail the rollback job here or continue
                # For critical rollbacks, you might want to sys.exit(1) here.
    else:
        print("No automatic rollback SQLs generated or defined. Manual intervention is likely required.")
        # If no automated rollback is possible, ensure a clear alert.
        sys.exit(1) # Fail the rollback job if no automated action was taken

except snowflake.connector.errors.ProgrammingError as e:
    print(f"Snowflake connection or SQL execution error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred during rollback: {e}")
    sys.exit(1)
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Snowflake connection closed for rollback.")
