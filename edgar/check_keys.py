
import duckdb

TARGET_DB = 'compustat_edgar.duckdb'

def check_key_columns():
    con = duckdb.connect(TARGET_DB, read_only=True)
    try:
        print("CSCO_IKEY columns:")
        cols = con.execute("DESCRIBE CSCO_IKEY").fetchall()
        print([c[0] for c in cols])
        
        print("\nCO_IFNDQ columns (first 20):")
        cols2 = con.execute("DESCRIBE CO_IFNDQ").fetchall() # Wait, I created it? No, I generated SQL but haven't run it.
        # I can check the SQL file or just assume from my previous inspect.
    except Exception as e:
        print(e)
    con.close()

if __name__ == "__main__":
    check_key_columns()

