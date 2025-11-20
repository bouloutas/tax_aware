
import duckdb
import re

DB_PATH = 'compustat_edgar.duckdb'
SQL_FILE = 'src/sql/create_co_ifndq.sql'

def get_table_columns(sql_content):
    # Regex to extract column names from CREATE TABLE statement
    # Assumes "    COLNAME TYPE" format
    matches = re.findall(r'^\s+(\w+)\s+\w+', sql_content, re.MULTILINE)
    return matches

def populate():
    con = duckdb.connect(DB_PATH)
    
    # 1. Create Table
    print("Creating CO_IFNDQ table...")
    with open(SQL_FILE, 'r') as f:
        create_sql = f.read()
        con.execute(create_sql)
        
    # 2. Get Columns
    target_cols = get_table_columns(create_sql)
    print(f"Target CO_IFNDQ has {len(target_cols)} columns.")
    
    # 3. Get Key Columns from CSCO_IKEY
    ikey_cols = [c[0] for c in con.execute("DESCRIBE CSCO_IKEY").fetchall()]
    ikey_cols_set = set(ikey_cols)
    
    # 4. Build INSERT Query
    print("Building insert query...")
    
    select_parts = []
    
    for col in target_cols:
        if col in ikey_cols_set:
            select_parts.append(f"k.{col}")
        else:
            # Assume it's an item
            # Handle _DC columns? No, we don't extract data codes usually.
            # If extracted data doesn't have it, it will be NULL.
            select_parts.append(f"MAX(CASE WHEN f.item = '{col}' THEN f.valuei END) as {col}")
            
    # We need to group by all key columns
    # Which are the key columns? The ones we selected from k.
    group_by_cols = [col for col in target_cols if col in ikey_cols_set]
    
    # Also need coifnd_id for joining, but it might not be in target.
    # We group by coifnd_id implicitly if we group by keys?
    # Better to group by k.coifnd_id to ensure 1-to-1 with reports, then agg.
    # But coifnd_id is not in target (likely).
    # So we group by k.GVKEY, k.DATADATE etc.
    
    query = f"""
    INSERT INTO CO_IFNDQ
    SELECT
        {", ".join(select_parts)}
    FROM CSCO_IKEY k
    LEFT JOIN CSCO_IFNDQ f ON k.coifnd_id = f.coifnd_id
    GROUP BY {", ".join([f"k.{c}" for c in group_by_cols])}
    """
    
    # 5. Execute
    print("Executing population query...")
    try:
        # Clear existing data to avoid duplicates
        con.execute("DELETE FROM CO_IFNDQ")
        con.execute(query)
        count = con.execute("SELECT COUNT(*) FROM CO_IFNDQ").fetchone()[0]
        print(f"Successfully populated CO_IFNDQ with {count} rows.")
    except Exception as e:
        print(f"Error executing query: {e}")
        # print(query) # Too large to print usually
    
    con.close()

if __name__ == "__main__":
    populate()
