
import duckdb
import pandas as pd

SOURCE_DB = '/home/tasos/compustat.duckdb'
TARGET_DB = 'compustat_edgar.duckdb'
GVKEYS = ['012141', '117768'] # MSFT, NVDA

def get_tables(db_path):
    conn = duckdb.connect(db_path, read_only=True)
    try:
        # Handle schema.table format
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
        return [t[0] for t in tables], conn
    except Exception as e:
        print(f"Error reading {db_path}: {e}")
        return [], None

def count_rows(conn, table, gvkeys):
    # Check if table has GVKEY column
    try:
        columns = conn.execute(f"DESCRIBE {table}").df()['column_name'].tolist()
        # Handle case sensitivity
        gvkey_col = next((c for c in columns if c.upper() == 'GVKEY'), None)
        
        if not gvkey_col:
            # Check if it links via another ID (e.g. coifnd_id)
            if 'coifnd_id' in [c.lower() for c in columns] or 'COIFND_ID' in columns:
                # Need to join with CSCO_IKEY? No, too complex for generic check.
                # Just return -1 for now.
                return -1 
            return -1 
            
        gvkeys_str = ",".join([f"'{g}'" for g in gvkeys])
        count = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {gvkey_col} IN ({gvkeys_str})").fetchone()[0]
        return count
    except Exception as e:
        return -2 # Error

def compare_schemas():
    source_tables, source_conn = get_tables(SOURCE_DB)
    target_tables, target_conn = get_tables(TARGET_DB)
    
    print(f"Source Tables: {len(source_tables)}")
    print(f"Target Tables: {len(target_tables)}")
    
    print("\nTable Analysis for MSFT & NVDA:")
    print(f"{'Table':<30} | {'Source Rows':<12} | {'Target Rows':<12} | {'Status'}")
    print("-" * 70)
    
    all_tables = sorted(list(set(source_tables) | set(target_tables)))
    
    for table in all_tables:
        if table.upper() in ['R_UPDATES', 'DUCKDB_TABLES', 'DUCKDB_COLUMNS']: continue 
        
        s_count = 0
        t_count = 0
        
        if table in source_tables:
            s_count = count_rows(source_conn, table, GVKEYS)
        else:
            s_count = "N/A"
            
        if table in target_tables:
            t_count = count_rows(target_conn, table, GVKEYS)
        else:
            t_count = "N/A"
            
        # Filter out tables that are not relevant (no rows in source for these companies)
        if s_count == 0 and (t_count == 0 or t_count == "N/A"):
            continue
        if s_count == -1: # No GVKEY
            # Try to count all rows if small table? No.
            continue
            
        status = ""
        if s_count == "N/A": status = "Target Only"
        elif t_count == "N/A": status = "Missing in Target"
        elif s_count > 0 and t_count == 0: status = "Empty in Target"
        elif s_count > 0 and t_count > 0: status = "Populated"
        
        print(f"{table:<30} | {str(s_count):<12} | {str(t_count):<12} | {status}")

    if source_conn: source_conn.close()
    if target_conn: target_conn.close()

if __name__ == "__main__":
    compare_schemas()

