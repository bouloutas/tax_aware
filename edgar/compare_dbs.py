import duckdb
import pandas as pd
from pathlib import Path
import sys

# Configuration
DB_PATH = Path('compustat_edgar.duckdb')
REF_DB_PATH = Path('../compustat.duckdb')  # Adjust if needed

def compare_databases():
    if not DB_PATH.exists():
        print(f"Error: {DB_PATH} not found.")
        return
    if not REF_DB_PATH.exists():
        print(f"Error: {REF_DB_PATH} not found.")
        # Try absolute path or prompt user
        return

    print(f"Comparing {DB_PATH} vs {REF_DB_PATH}")
    
    con = duckdb.connect(str(DB_PATH))
    
    # Attach reference DB
    con.execute(f"ATTACH '{REF_DB_PATH}' AS ref")
    
    # Get GVKEYs present in Edgar DB
    gvkeys = con.execute("SELECT DISTINCT GVKEY FROM main.CSCO_IKEY").fetchall()
    gvkeys = [g[0] for g in gvkeys]
    
    print(f"Found GVKEYs in Edgar DB: {gvkeys}")
    
    # Key items to compare
    key_items = ['REVTQ', 'NIQ', 'ATQ', 'LTQ', 'COGSQ', 'XSGAQ']
    
    for gvkey in gvkeys:
        print(f"\n{'='*60}")
        print(f"Analyzing GVKEY: {gvkey}")
        
        # Query to compare
        query = f"""
        WITH edgar_data AS (
            SELECT 
                i.DATADATE,
                f.ITEM,
                f.VALUEI as val_edgar
            FROM main.CSCO_IFNDQ f
            JOIN main.CSCO_IKEY i ON f.COIFND_ID = i.COIFND_ID
            WHERE i.GVKEY = '{gvkey}'
            AND f.ITEM IN ({','.join([f"'{x}'" for x in key_items])})
        ),
        ref_data AS (
            SELECT 
                DATADATE,
                coalesce(saleq, revtq) as REVTQ,
                niq as NIQ,
                atq as ATQ,
                ltq as LTQ,
                cogsq as COGSQ,
                xsgaq as XSGAQ
            FROM ref.fundq
            WHERE gvkey = '{gvkey}'
            AND datadate >= '2023-01-01'
        ),
        ref_unpivoted AS (
            UNPIVOT ref_data
            ON REVTQ, NIQ, ATQ, LTQ, COGSQ, XSGAQ
            INTO NAME item VALUE val_ref
        )
        SELECT 
            r.DATADATE,
            r.item,
            r.val_ref,
            e.val_edgar,
            (e.val_edgar - r.val_ref) as diff,
            CASE WHEN r.val_ref != 0 
                 THEN ABS((e.val_edgar - r.val_ref) / r.val_ref) * 100 
                 ELSE 0 END as pct_diff
        FROM ref_unpivoted r
        JOIN edgar_data e ON r.DATADATE = e.DATADATE AND r.item = e.item
        WHERE ABS(diff) > 1.0  -- Filter small rounding diffs
        ORDER BY r.DATADATE, r.item
        """
        
        try:
            df = con.execute(query).df()
            if df.empty:
                print(f"{'='*60}")
                print("SUCCESS: Key financials match exactly!")
            else:
                print(f"{'='*60}")
                print(f"FOUND {len(df)} DISCREPANCIES in Key Items:")
                print(df.to_string())
        except Exception as e:
            print(f"Error comparing GVKEY {gvkey}: {e}")
            
        # Check coverage
        ref_count = con.execute(f"SELECT COUNT(*) FROM ref.fundq WHERE gvkey = '{gvkey}' AND datadate >= '2023-01-01'").fetchone()[0]
        edgar_count = con.execute(f"SELECT COUNT(*) FROM main.CSCO_IKEY WHERE gvkey = '{gvkey}'").fetchone()[0]
        print(f"\nCoverage Stats:")
        print(f"Reference Records: {ref_count}")
        print(f"Edgar Records:     {edgar_count}")

if __name__ == "__main__":
    compare_databases()



