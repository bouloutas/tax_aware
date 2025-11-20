
import duckdb

SOURCE_DB = '/home/tasos/compustat.duckdb'

def list_tables():
    con = duckdb.connect(SOURCE_DB, read_only=True)
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'CO_%'").fetchall()
    print("CO_ Tables:")
    for t in sorted(tables):
        print(t[0])
    con.close()

if __name__ == "__main__":
    list_tables()

