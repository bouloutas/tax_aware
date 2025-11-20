import duckdb
import pandas as pd

def check_status():
    con = duckdb.connect('/home/tasos/tax_aware/edgar/compustat_edgar.duckdb', read_only=True)
    
    print("=== NVDA (CIK 1045810) Check ===")
    # Check COMPANY table
    res = con.execute("SELECT * FROM COMPANY WHERE cik='0001045810' OR cik='1045810'").fetchall()
    print(f"COMPANY Table Records: {res}")
    
    # Check if any data exists in CSCO_IKEY for NVDA GVKEY (likely 061241)
    res_ikey = con.execute("SELECT * FROM CSCO_IKEY WHERE gvkey='061241'").fetchall()
    print(f"CSCO_IKEY Records for GVKEY 061241: {len(res_ikey)}")

    print("\n=== MSFT (GVKEY 012141) Check ===")
    # Check specific Quarter REVTQ vs NIQ
    query = """
    SELECT k.datadate, f.item, f.valuei
    FROM CSCO_IFNDQ f
    JOIN CSCO_IKEY k ON f.coifnd_id = k.coifnd_id
    WHERE k.gvkey='012141' AND f.item IN ('REVTQ', 'NIQ')
    ORDER BY k.datadate DESC
    LIMIT 10
    """
    print(con.execute(query).df().to_string())
    
    con.close()

if __name__ == "__main__":
    check_status()

