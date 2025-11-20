
import duckdb
import pandas as pd
import os

SOURCE_DB = '/home/tasos/compustat.duckdb'
TARGET_DB = 'compustat_edgar.duckdb'
GVKEYS = ['012141', '117768']

def compare_co_table():
    print("Comparing CO_IFNDQ (Wide Table)...")
    con_src = duckdb.connect(SOURCE_DB, read_only=True)
    con_tgt = duckdb.connect(TARGET_DB, read_only=True)
    
    # Items to check
    items = ['ATQ', 'LTQ', 'NIQ', 'SALEQ', 'COGSQ', 'TXTQ', 'MIBQ', 'LTMIBQ']
    
    for gvkey in GVKEYS:
        print(f"\nAnalyzing GVKEY {gvkey}...")
        
        # Select from Source
        # Note: Source column names might be uppercase
        cols = ", ".join(items)
        src_df = con_src.execute(f"""
            SELECT DATADATE, {cols}
            FROM CO_IFNDQ
            WHERE GVKEY = '{gvkey}' AND DATADATE >= '1994-01-01'
            ORDER BY DATADATE
        """).df()
        src_df = src_df.add_suffix('_src')
        src_df = src_df.rename(columns={'DATADATE_src': 'datadate'})
        
        # Select from Target
        tgt_df = con_tgt.execute(f"""
            SELECT DATADATE, {cols}
            FROM CO_IFNDQ
            WHERE GVKEY = '{gvkey}'
            ORDER BY DATADATE
        """).df()
        tgt_df = tgt_df.add_suffix('_tgt')
        tgt_df = tgt_df.rename(columns={'DATADATE_tgt': 'datadate'})
        
        # Merge
        merged = pd.merge(src_df, tgt_df, on='datadate', how='inner') # Inner join to compare matching periods
        
        if merged.empty:
            print("No overlapping periods found.")
            continue
            
        print(f"Comparing {len(merged)} overlapping periods.")
        
        for item in items:
            col_src = f"{item}_src"
            col_tgt = f"{item}_tgt"
            
            # Calculate diff
            merged[f'{item}_diff'] = merged[col_tgt] - merged[col_src]
            
            # Check for significant diffs
            diffs = merged[merged[f'{item}_diff'].abs() > 0.1]
            
            if diffs.empty:
                print(f"{item}: MATCH ✅")
            else:
                print(f"{item}: MISMATCH ❌ ({len(diffs)} rows)")
                print(diffs[['datadate', col_src, col_tgt, f'{item}_diff']].tail(3).to_string(index=False))

    con_src.close()
    con_tgt.close()

if __name__ == "__main__":
    compare_co_table()
