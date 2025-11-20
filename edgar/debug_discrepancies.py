
import duckdb
import pandas as pd
import os

SOURCE_DB = '/home/tasos/compustat.duckdb'
TARGET_DB = 'compustat_edgar.duckdb'

def get_values(db_path, gvkey, items):
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}")
        return pd.DataFrame()
        
    conn = duckdb.connect(db_path, read_only=True)
    items_str = ",".join([f"'{i}'" for i in items])
    query = f"""
        SELECT k.datadate, f.item, f.valuei
        FROM main.CSCO_IFNDQ f
        JOIN main.CSCO_IKEY k USING(coifnd_id)
        WHERE k.gvkey = '{gvkey}'
        AND f.item IN ({items_str})
        AND k.datadate >= '2023-01-01'
        ORDER BY k.datadate, f.item
    """
    try:
        df = conn.execute(query).df()
        conn.close()
        return df
    except Exception as e:
        print(f"Error reading {db_path}: {e}")
        conn.close()
        return pd.DataFrame()

def compare(gvkey, company_name):
    items = ['RCDQ', 'LTMIBQ', 'TXDBQ', 'REVTQ', 'COGSQ', 'LTQ', 'MIBQ']
    print(f"\nAnalyzing {company_name} ({gvkey}) for items: {items}")
    
    source_df = get_values(SOURCE_DB, gvkey, items)
    target_df = get_values(TARGET_DB, gvkey, items)
    
    # Normalize column names
    source_df.columns = [c.lower() for c in source_df.columns]
    target_df.columns = [c.lower() for c in target_df.columns]
    
    print(f"Source columns: {source_df.columns}")
    print(f"Target columns: {target_df.columns}")
    
    if source_df.empty and target_df.empty:
        print("Missing data in both DBs")
        return

    # Rename value columns
    if not source_df.empty:
        source_df = source_df.rename(columns={'valuei': 'Source'})
    else:
        source_df = pd.DataFrame(columns=['datadate', 'item', 'Source'])
        
    if not target_df.empty:
        target_df = target_df.rename(columns={'valuei': 'Target'})
    else:
        target_df = pd.DataFrame(columns=['datadate', 'item', 'Target'])

    # Merge
    merged = pd.merge(source_df, target_df, on=['datadate', 'item'], how='outer')
    merged['Diff'] = merged['Target'] - merged['Source']
    
    # Sort by item and date
    merged = merged.sort_values(['item', 'datadate'])
    
    for item in items:
        item_data = merged[merged['item'] == item]
        if item_data.empty:
            print(f"\n{item}: No data found")
            continue
            
        print(f"\n{item} Comparison:")
        print(item_data[['datadate', 'Source', 'Target', 'Diff']].to_string(index=False))

compare('012141', 'MSFT')
compare('117768', 'NVDA')

