import os
import sys
import tempfile
import snowflake.connector

def get_snowflake_connection():
    # ensure required env vars exist
    required = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_PRIVATE_KEY', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'
    ]
    for var in required:
        if not os.getenv(var):
            print(f"❌ Missing environment variable: {var}", file=sys.stderr)
            sys.exit(1)

    # write raw PEM from env into a temp file
    key_content = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.pem') as key_file:
        key_file.write(key_content)
        key_path = key_file.name
    os.chmod(key_path, 0o600)

    try:
        ctx = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            private_key_file=key_path,
            private_key_file_pwd=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
            authenticator='snowflake_jwt',
            autocommit=True
        )
        return ctx, key_path
    except Exception as e:
        os.remove(key_path)
        print(f"❌ Failed to connect to Snowflake: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    conn, key_path = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        ts = cursor.fetchone()[0]
        print(ts.strftime('%Y-%m-%d %H:%M:%S'))
        cursor.close()
    finally:
        conn.close()
        # clean up
        try:
            os.remove(key_path)
        except Exception:
            pass

if __name__ == "__main__":
    main()
