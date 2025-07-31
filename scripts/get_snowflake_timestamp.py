import os
import snowflake.connector
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import sys

def get_snowflake_connection():
    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    private_key_str = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    role = os.getenv("SNOWFLAKE_ROLE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")

    if not all([user, account, private_key_str, role, warehouse, database]):
        print("❌ Missing required environment variables for Snowflake connection", file=sys.stderr)
        sys.exit(1)

    # Decode and load private key
    private_key_bytes = base64.b64decode(private_key_str)
    pkey = serialization.load_pem_private_key(
        private_key_bytes,
        password=private_key_passphrase.encode() if private_key_passphrase else None,
        backend=default_backend()
    )
    pkb = pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    ctx = snowflake.connector.connect(
        user=user,
        account=account,
        private_key=pkb,
        role=role,
        warehouse=warehouse,
        database=database,
        autocommit=True,
    )
    return ctx

def main():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_TIMESTAMP()")
    ts = cursor.fetchone()[0]
    print(ts.strftime('%Y-%m-%d %H:%M:%S'))
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
