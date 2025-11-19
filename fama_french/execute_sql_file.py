

# execute_sql_file.py (FINAL, ROBUST FILE-BASED VERSION)
# ... (get_snowflake_connection function is unchanged) ...
import os
import sys
import snowflake.connector
from cryptography.hazmat.primitives import serialization

def get_snowflake_connection():
    # This connection logic is perfect and remains unchanged.
    try:
        user = os.environ['SRC_USER']
        warehouse = os.environ['SRC_WAREHOUSE']
        private_key_passphrase = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        password_arg = private_key_passphrase.encode() if private_key_passphrase else None
        private_key_path = os.path.expanduser("~/.ssh/snowflake_key.p8")
        with open(private_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(key_file.read(), password=password_arg)
        private_key_bytes = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        conn = snowflake.connector.connect(
            user=user, private_key=private_key_bytes, account='fpb76675.us-east-1',
            warehouse=warehouse, role='SYSADMIN', database='FAMA_FRENCH', schema='PROCESSED_COMPUSTAT_DATA'
        )
        return conn
    except Exception as e:
        print(f"CRITICAL: Failed to connect to Snowflake: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """
    Main function. Now accepts an optional second argument pointing to a file
    that contains the variable for SQL substitution.
    Usage:
      python execute_sql_file.py <path_to_sql_file>
      python execute_sql_file.py <path_to_template_sql_file> <path_to_variable_file>
    """
    if len(sys.argv) < 2:
        print("Usage: python execute_sql_file.py <sql_file> [variable_file]", file=sys.stderr)
        sys.exit(1)

    sql_file_path = sys.argv[1]
    variable_file_path = sys.argv[2] if len(sys.argv) > 2 else None

    # ... (file existence checks are the same) ...
    if not os.path.exists(sql_file_path):
        print(f"CRITICAL: SQL file not found at: {sql_file_path}", file=sys.stderr)
        sys.exit(1)

    print(f"--- Python Executor: Processing SQL file: {sql_file_path} ---")

    conn = None
    try:
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()

        # If a variable file was passed, read it and perform substitution
        if variable_file_path:
            if not os.path.exists(variable_file_path):
                raise FileNotFoundError(f"Variable file not found: {variable_file_path}")
            
            with open(variable_file_path, 'r') as f_var:
                variable_content = f_var.read()

            print("Substitution variable file found. Replacing '&{TICKER_LIST_VAR}'...")
            sql_content = sql_content.replace("&{TICKER_LIST_VAR}", variable_content)

        if not sql_content.strip():
            raise ValueError("SQL content is empty after processing. Aborting.")

        conn = get_snowflake_connection()
        print("Successfully connected to Snowflake via Python.")

        for cursor in conn.execute_string(sql_content):
            print(f"Statement executed successfully. Result: {cursor.fetchone()}")

        print(f"--- Successfully executed all statements in {sql_file_path} ---")

    except Exception as e:
        print(f"CRITICAL: Error during SQL execution: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Snowflake connection closed.")

if __name__ == "__main__":
    main()