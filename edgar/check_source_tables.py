
import duckdb

DB_PATH = '/home/tasos/compustat.duckdb'

def check():
    con = duckdb.connect(DB_PATH, read_only=True)
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
    tables = [t[0] for t in tables]
    
    relevant = [t for t in tables if t.startswith('CO_') or t.startswith('FUND') or t.startswith('CSCO_')]
    print(sorted(relevant))
    con.close()

if __name__ == "__main__":
    check()

