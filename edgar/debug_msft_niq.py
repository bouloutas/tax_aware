
import duckdb
import pandas as pd

DB_PATH = 'compustat_edgar.duckdb'

def debug_msft():
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Get COIFND_ID for 2023-06-30
    keys = con.execute("""
        SELECT coifnd_id
        FROM CSCO_IKEY 
        WHERE gvkey = '012141' AND datadate = '2023-06-30'
        LIMIT 1
    """).fetchone()
    
    if keys:
        cid = keys[0]
        print(f"Analyzing Report {cid} (2023-06-30):")
        items = con.execute(f"SELECT item, valuei FROM CSCO_IFNDQ WHERE coifnd_id = {cid}").fetchall()
        for i in items:
            print(f"  {i[0]}: {i[1]}")
    else:
        print("Report not found.")

    con.close()

if __name__ == "__main__":
    debug_msft()
