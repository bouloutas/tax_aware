import snowflake.connector
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import sys
from cryptography.hazmat.primitives import serialization

print("Script 'copy_optimization_returns_to_mysql.py' started.")

# --- Configuration ---
try:
    load_dotenv()
    print(".env file loaded (if found).")
except Exception as e:
    print(f"Note: Error loading .env file: {e}")

# --- Unified Snowflake Credentials ---
SNOWFLAKE_USER = os.getenv('SRC_USER')
SNOWFLAKE_WAREHOUSE = os.getenv('SRC_WAREHOUSE')
SNOWFLAKE_ACCOUNT = 'fpb76675.us-east-1'

SNOWFLAKE_DATABASE = 'FAMA_FRENCH'
SNOWFLAKE_SCHEMA = 'PROCESSED_COMPUSTAT_DATA'
SNOWFLAKE_SOURCE_TABLE_OPT_RETURNS = 'OPTIMIZATION_PORTFOLIO_MONTHLY_RETURNS'

# --- MySQL Credentials ---
MYSQL_USER = os.getenv('MYSQL_USER_LOCAL', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD_LOCAL', '')
MYSQL_HOST = os.getenv('MYSQL_HOST_LOCAL', 'localhost')
MYSQL_DATABASE_TARGET = 'fama_french_local'
MYSQL_TABLE_TARGET_OPT_RETURNS = 'optimization_portfolio_monthly_returns'
MYSQL_SOCKET = os.getenv('MYSQL_SOCKET_LOCAL', '/Users/tasosbouloutas/mysql_data/mysql.sock')

# --- Corrected Credential Check ---
print("--- Configuration Loaded ---")
print(f"Source Snowflake Table: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_SOURCE_TABLE_OPT_RETURNS}")
print(f"Target MySQL Table: {MYSQL_DATABASE_TARGET}.{MYSQL_TABLE_TARGET_OPT_RETURNS}")
if not all([SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE]):
    print("CRITICAL ERROR: One or more Snowflake credentials (SRC_USER, SNOWFLAKE_ACCOUNT, SRC_WAREHOUSE) are not set.")
    print("Please ensure they are in your .env file or set as environment variables.")
    sys.exit("Exiting due to missing Snowflake credentials.")

print(f"SF User: {SNOWFLAKE_USER}")
print(f"SF Account: {SNOWFLAKE_ACCOUNT}")
print(f"SF Warehouse: {SNOWFLAKE_WAREHOUSE}")


# --- Unified Snowflake Connection Function ---
def get_snowflake_connection():
    """Establishes a Snowflake connection using key-pair auth."""
    print("Attempting to connect to Snowflake...")
    try:
        private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
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
            user=SNOWFLAKE_USER,
            private_key=private_key_bytes,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role='SYSADMIN'
        )
        print("Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        if not SNOWFLAKE_USER or not SNOWFLAKE_WAREHOUSE:
             print("CRITICAL ERROR: SRC_USER or SRC_WAREHOUSE environment variables are not set.")
        return None

# --- MySQL Connection & Setup (Unchanged) ---
def create_mysql_database_if_not_exists(db_name):
    print(f"Ensuring MySQL database '{db_name}' exists...")
    try:
        cnx_admin = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, unix_socket=MYSQL_SOCKET)
        cursor_admin = cnx_admin.cursor(); cursor_admin.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        cursor_admin.close(); cnx_admin.close()
    except mysql.connector.Error as err:
        print(f"Error creating/checking MySQL database '{db_name}': {err}"); raise

def get_mysql_connection(db_name=MYSQL_DATABASE_TARGET):
    print(f"Attempting to connect to MySQL database '{db_name}'...")
    try:
        cnx = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, unix_socket=MYSQL_SOCKET, database=db_name)
        print(f"Successfully connected to MySQL database: {db_name}")
        return cnx
    except mysql.connector.Error as err:
        print(f"MySQL Connection Error to '{db_name}': {err}"); return None

def create_mysql_optimization_returns_table(cnx_mysql, table_name):
    cursor = cnx_mysql.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        TICKER VARCHAR(20), MONTH_END_DATE DATE, GVKEY VARCHAR(6), IID VARCHAR(3),
        LAST_ADJUSTED_PRICE_OF_MONTH DECIMAL(20, 8), MONTHLY_RETURN DECIMAL(20, 10),
        PRIMARY KEY (TICKER, MONTH_END_DATE), INDEX idx_opt_gvkey_date (GVKEY, MONTH_END_DATE)
    );
    """
    try:
        print(f"Ensuring MySQL table '`{table_name}`' exists..."); cursor.execute(create_table_query); cnx_mysql.commit()
    except mysql.connector.Error as err:
        print(f"Error creating MySQL table '`{table_name}`': {err}"); raise
    finally:
        cursor.close()

# --- Data Transfer Functions (Unchanged) ---
def fetch_data_from_snowflake(sf_conn, query):
    print(f"Fetching data from Snowflake with query: {query}")
    try:
        df = pd.read_sql(query, sf_conn)
        print(f"Fetched {len(df)} rows from Snowflake.")
    except Exception as e:
        print(f"Error during Snowflake fetch: {e}"); df = pd.DataFrame()
    return df

def load_data_to_mysql(mysql_conn, df, table_name, chunk_size=10000):
    if df.empty: print("DataFrame is empty. No data to load."); return
    cursor = mysql_conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE `{table_name}`;"); mysql_conn.commit()
    except mysql.connector.Error as err:
        print(f"Error truncating table '`{table_name}`': {err}")

    print(f"Loading {len(df)} rows into MySQL table '`{table_name}`'...")
    cols_mysql_safe = [f"`{c}`" for c in df.columns]
    cols_str = ", ".join(cols_mysql_safe)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO `{table_name}` ({cols_str}) VALUES ({placeholders})"
    data_tuples = [tuple(x) for x in df.replace({pd.NaT: None, np.nan: None}).to_numpy()]
    total_inserted = 0
    try:
        for i in range(0, len(data_tuples), chunk_size):
            chunk = data_tuples[i:i + chunk_size]
            cursor.executemany(insert_sql, chunk); mysql_conn.commit()
            total_inserted += len(chunk)
            print(f"Inserted chunk {i // chunk_size + 1} ({len(chunk)} rows). Total inserted: {total_inserted}")
    except mysql.connector.Error as err:
        print(f"Error inserting data chunk into MySQL: {err}"); mysql_conn.rollback()
    cursor.close()

# --- Main Execution Logic (Unchanged) ---
def main():
    print("--- Starting Snowflake to MySQL Data Transfer for Optimization Returns ---")
    try:
        create_mysql_database_if_not_exists(MYSQL_DATABASE_TARGET)
    except Exception as e:
        print(f"CRITICAL: Failed to ensure MySQL database exists: {e}. Exiting."); sys.exit(1)

    sf_conn = get_snowflake_connection()
    if not sf_conn: print("CRITICAL: Failed to connect to Snowflake. Exiting."); sys.exit(1)

    mysql_conn = get_mysql_connection()
    if not mysql_conn:
        if sf_conn: sf_conn.close()
        print("CRITICAL: Failed to connect to MySQL. Exiting."); sys.exit(1)

    try:
        create_mysql_optimization_returns_table(mysql_conn, MYSQL_TABLE_TARGET_OPT_RETURNS)
        query_sf = f"SELECT TICKER, MONTH_END_DATE, GVKEY, IID, LAST_ADJUSTED_PRICE_OF_MONTH, MONTHLY_RETURN FROM {SNOWFLAKE_SOURCE_TABLE_OPT_RETURNS};"
        df_from_snowflake = fetch_data_from_snowflake(sf_conn, query_sf)

        if not df_from_snowflake.empty:
            print("Preparing DataFrame for MySQL insertion...")
            if 'MONTH_END_DATE' in df_from_snowflake.columns:
                 df_from_snowflake['MONTH_END_DATE'] = pd.to_datetime(df_from_snowflake['MONTH_END_DATE']).dt.date
            load_data_to_mysql(mysql_conn, df_from_snowflake, MYSQL_TABLE_TARGET_OPT_RETURNS)
        else:
            print("No data fetched from Snowflake for optimization returns or DataFrame is empty.")
    except Exception as e:
        import traceback; traceback.print_exc()
    finally:
        if sf_conn: sf_conn.close(); print("Snowflake connection closed.")
        if mysql_conn: mysql_conn.close(); print("MySQL connection closed.")

if __name__ == "__main__":
    main()
    print("--- Script 'copy_optimization_returns_to_mysql.py' Finished ---")