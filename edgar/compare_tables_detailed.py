#!/usr/bin/env python3
"""
Detailed table-by-table comparison for MSFT and NVDA
"""
import sys
import duckdb
from collections import defaultdict

source_conn = duckdb.connect('/home/tasos/compustat.duckdb')
target_conn = duckdb.connect('compustat_edgar.duckdb')

print('='*80)
print('DETAILED TABLE COMPARISON: MSFT & NVDA')
print('='*80)

# Key tables to compare
key_tables = ['COMPANY', 'SECURITY', 'SEC_IDCURRENT', 'CSCO_IKEY', 'CSCO_IFNDQ']

results = defaultdict(dict)

for table_name in key_tables:
    print(f'\n{"="*80}')
    print(f'TABLE: {table_name}')
    print(f'{"="*80}')
    
    try:
        # Get source structure
        source_cols = source_conn.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'main' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()
        
        # Get target structure
        target_cols = target_conn.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'main' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()
        
        if not source_cols:
            print(f'  Source table does not exist')
            continue
        
        if not target_cols:
            print(f'  Target table does not exist')
            continue
        
        source_col_dict = {c[0]: c[1] for c in source_cols}
        target_col_dict = {c[0]: c[1] for c in target_cols}
        
        common_cols = set(source_col_dict.keys()) & set(target_col_dict.keys())
        source_only = set(source_col_dict.keys()) - set(target_col_dict.keys())
        target_only = set(target_col_dict.keys()) - set(source_col_dict.keys())
        
        print(f'\nColumns:')
        print(f'  Source: {len(source_col_dict)} columns')
        print(f'  Target: {len(target_col_dict)} columns')
        print(f'  Common: {len(common_cols)} columns')
        if source_only:
            print(f'  Source only: {len(source_only)} columns')
        if target_only:
            print(f'  Target only: {len(target_only)} columns')
        
        # Count records
        try:
            source_count = source_conn.execute(f"""
                SELECT COUNT(*) FROM main.{table_name} 
                WHERE GVKEY IN ('012141', '117768')
            """).fetchone()[0]
        except:
            source_count = 0
        
        try:
            target_count = target_conn.execute(f"""
                SELECT COUNT(*) FROM main.{table_name} 
                WHERE GVKEY IN ('012141', '117768')
            """).fetchone()[0]
        except:
            target_count = 0
        
        print(f'\nRecords (MSFT/NVDA):')
        print(f'  Source: {source_count:,}')
        print(f'  Target: {target_count:,}')
        
        # Check populated fields
        print(f'\nField Population Status:')
        populated_fields = []
        unpopulated_fields = []
        mismatched_types = []
        
        # Determine filter condition based on table
        if table_name == 'CSCO_IFNDQ':
            # CSCO_IFNDQ doesn't have GVKEY, link through CSCO_IKEY
            filter_condition = """
                COIFND_ID IN (
                    SELECT COIFND_ID FROM main.CSCO_IKEY WHERE GVKEY IN ('012141', '117768')
                )
            """
        elif 'GVKEY' in common_cols:
            filter_condition = "GVKEY IN ('012141', '117768')"
        else:
            filter_condition = "1=1"  # No filter
        
        for col in sorted(common_cols):
            try:
                # Check if field has data
                non_null_count = target_conn.execute(f"""
                    SELECT COUNT(*) FROM main.{table_name} 
                    WHERE {filter_condition} AND {col} IS NOT NULL
                """).fetchone()[0]
                
                if non_null_count > 0:
                    populated_fields.append(col)
                else:
                    unpopulated_fields.append(col)
                
                # Check type match
                if source_col_dict[col] != target_col_dict[col]:
                    mismatched_types.append((col, source_col_dict[col], target_col_dict[col]))
            except Exception as e:
                # Column might not be queryable (e.g., complex types)
                pass
        
        print(f'  Populated: {len(populated_fields)}/{len(common_cols)} fields')
        print(f'  Unpopulated: {len(unpopulated_fields)} fields')
        
        if populated_fields:
            print(f'\n  Populated fields ({len(populated_fields)}):')
            for field in populated_fields[:20]:
                print(f'    - {field}')
            if len(populated_fields) > 20:
                print(f'    ... and {len(populated_fields) - 20} more')
        
        if unpopulated_fields:
            print(f'\n  Unpopulated fields ({len(unpopulated_fields)}):')
            for field in unpopulated_fields[:20]:
                print(f'    - {field}')
            if len(unpopulated_fields) > 20:
                print(f'    ... and {len(unpopulated_fields) - 20} more')
        
        if mismatched_types:
            print(f'\n  Type mismatches ({len(mismatched_types)}):')
            for col, src_type, tgt_type in mismatched_types[:10]:
                print(f'    - {col}: Source={src_type}, Target={tgt_type}')
        
        results[table_name] = {
            'source_cols': len(source_col_dict),
            'target_cols': len(target_col_dict),
            'common_cols': len(common_cols),
            'source_records': source_count,
            'target_records': target_count,
            'populated_fields': len(populated_fields),
            'unpopulated_fields': len(unpopulated_fields),
        }
        
    except Exception as e:
        print(f'  Error analyzing table: {e}')

# Summary
print(f'\n{"="*80}')
print('SUMMARY')
print(f'{"="*80}')

total_source_cols = sum(r['source_cols'] for r in results.values())
total_target_cols = sum(r['target_cols'] for r in results.values())
total_common_cols = sum(r['common_cols'] for r in results.values())
total_populated = sum(r['populated_fields'] for r in results.values())
total_unpopulated = sum(r['unpopulated_fields'] for r in results.values())

print(f'\nAcross all key tables:')
print(f'  Total source columns: {total_source_cols}')
print(f'  Total target columns: {total_target_cols}')
print(f'  Common columns: {total_common_cols}')
print(f'  Populated fields: {total_populated}/{total_common_cols} ({total_populated/total_common_cols*100:.1f}%)')
print(f'  Unpopulated fields: {total_unpopulated}/{total_common_cols} ({total_unpopulated/total_common_cols*100:.1f}%)')

print(f'\nBy table:')
for table_name, r in results.items():
    pct = (r['populated_fields'] / r['common_cols'] * 100) if r['common_cols'] > 0 else 0
    print(f'  {table_name}: {r["populated_fields"]}/{r["common_cols"]} fields ({pct:.1f}%)')

source_conn.close()
target_conn.close()

