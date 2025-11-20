
import duckdb

DB_PATH = '/home/tasos/compustat.duckdb'

def inspect():
    con = duckdb.connect(DB_PATH, read_only=True)
    cols = con.execute("DESCRIBE CSCO_IKEY").fetchall()
    print([c[0] for c in cols])
    
    # Check values of 'datafmt' or 'indfmt' or 'popsrc'
    print("Sample IKEY rows:")
    print(con.execute("SELECT * FROM CSCO_IKEY LIMIT 5").df())
    con.close()

if __name__ == "__main__":
    inspect()

