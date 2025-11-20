
import duckdb

DB_PATH = '/home/tasos/compustat.duckdb'

def inspect():
    con = duckdb.connect(DB_PATH, read_only=True)
    
    for table in ['CO_AFND1', 'CO_AFND2', 'CSCO_AFND']:
        try:
            cols = con.execute(f"DESCRIBE {table}").fetchall()
            print(f"\n{table} ({len(cols)} columns):")
            print([c[0] for c in cols[:10]] + ['...'])
        except Exception as e:
            print(f"\n{table}: {e}")
            
    con.close()

if __name__ == "__main__":
    inspect()

