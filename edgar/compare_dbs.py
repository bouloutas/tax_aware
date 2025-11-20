import duckdb
import pandas as pd
import sys

def compare_databases(gvkeys):
    # Connect to both databases (read-only for reference)
    con_edgar = duckdb.connect('compustat_edgar.duckdb', read_only=True)
    con_ref = duckdb.connect('/home/tasos/compustat.duckdb', read_only=True)
    
    print(f"Comparing Data for GVKEYS: {gvkeys}")
    
    for gvkey in gvkeys:
        print(f"\n{'='*60}")
        print(f"Analyzing GVKEY: {gvkey}")
        print(f"{'='*60}")
        
        # 1. Get Reference Data (Truth)
        # We focus on CSCO_IFNDQ (Quarterly Financials)
        # Joined with IKEY to get dates
        query_ref = f"""
            SELECT k.datadate, f.item, f.valuei as val_ref
            FROM CSCO_IFNDQ f
            JOIN CSCO_IKEY k ON f.coifnd_id = k.coifnd_id
            WHERE k.gvkey = '{gvkey}'
            ORDER BY k.datadate, f.item
        """
        try:
            df_ref = con_ref.execute(query_ref).df()
            df_ref.columns = [c.lower() for c in df_ref.columns]
        except Exception as e:
            print(f"Error reading reference DB: {e}")
            continue
            
        if df_ref.empty:
            print("No reference data found.")
            continue

        # 2. Get Generated Data (Edgar)
        query_edgar = f"""
            SELECT k.datadate, f.item, f.valuei as val_edgar
            FROM CSCO_IFNDQ f
            JOIN CSCO_IKEY k ON f.coifnd_id = k.coifnd_id
            WHERE k.gvkey = '{gvkey}'
            ORDER BY k.datadate, f.item
        """
        try:
            df_edgar = con_edgar.execute(query_edgar).df()
            df_edgar.columns = [c.lower() for c in df_edgar.columns]
        except Exception as e:
            print(f"Error reading Edgar DB: {e}")
            continue

        if df_edgar.empty:
            print("No Edgar data generated.")
            continue

        # 3. Merge and Compare
        # Merge on Date + Item
        merged = pd.merge(df_ref, df_edgar, on=['datadate', 'item'], how='outer', suffixes=('_ref', '_edgar'))
        
        # Filter for items we actually have in Edgar (or missing)
        # Key items to check: REVTQ, NIQ, ATQ, LTQ, CEQQ
        key_items = ['REVTQ', 'NIQ', 'ATQ', 'LTQ', 'CEQQ', 'SALEQ', 'COGSQ', 'XSGAQ']
        
        merged_key = merged[merged['item'].isin(key_items)].copy()
        merged_key['diff'] = merged_key['val_edgar'] - merged_key['val_ref']
        merged_key['pct_diff'] = (merged_key['diff'] / merged_key['val_ref']).abs() * 100
        
        # Drop exact matches (or close enough floating point)
        discrepancies = merged_key[merged_key['pct_diff'] > 0.01] # > 0.01% diff
        
        if discrepancies.empty:
            print("SUCCESS: Key financials match exactly!")
        else:
            print(f"FOUND {len(discrepancies)} DISCREPANCIES in Key Items:")
            print(discrepancies.sort_values('datadate').tail(20).to_string())
            
        # Check coverage
        print(f"\nCoverage Stats:")
        print(f"Reference Records: {len(df_ref)}")
        print(f"Edgar Records:     {len(df_edgar)}")
        
    con_edgar.close()
    con_ref.close()

if __name__ == "__main__":
    # Find GVKEYs from SEC_IDCURRENT table (tic is there, not COMPANY)
    con = duckdb.connect('/home/tasos/compustat.duckdb', read_only=True)
    try:
        res = con.execute("SELECT gvkey FROM SEC_IDCURRENT WHERE ITEM='TIC' AND ITEMVALUE='NVDA' LIMIT 1").fetchone()
        if res:
            print(f"Found NVDA GVKEY: {res[0]}")
            nvda_gvkey = res[0]
        else:
            nvda_gvkey = '117768' # Known NVDA GVKEY
            
        res_msft = con.execute("SELECT gvkey FROM SEC_IDCURRENT WHERE ITEM='TIC' AND ITEMVALUE='MSFT' LIMIT 1").fetchone()
        if res_msft:
            print(f"Found MSFT GVKEY: {res_msft[0]}")
            msft_gvkey = res_msft[0]
        else:
            msft_gvkey = '012141'
            
    except Exception:
        nvda_gvkey = '061241'
        msft_gvkey = '012141'
    finally:
        con.close()

    compare_databases([msft_gvkey, nvda_gvkey])

