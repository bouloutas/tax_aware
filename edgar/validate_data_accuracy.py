#!/usr/bin/env python3
"""
Comprehensive data accuracy validation script.
Compares extracted values in compustat_edgar.duckdb against source compustat.duckdb
for MSFT and NVDA.
"""

import sys
import duckdb
from pathlib import Path
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def compare_company_fields(source_db, target_db, gvkey, name):
    """Compare COMPANY table fields."""
    logger.info(f"\n{'='*80}")
    logger.info(f"COMPANY TABLE VALIDATION: {name} (GVKEY: {gvkey})")
    logger.info(f"{'='*80}")
    
    source = source_db.execute(f'''
        SELECT GVKEY, CIK, CONM, CONML, ADD1, ADD2, CITY, STATE, ADDZIP, 
               SIC, PHONE, WEBURL, EIN, FYRC, GSECTOR, GGROUP, GIND, GSUBIND
        FROM main.COMPANY WHERE GVKEY = '{gvkey}'
    ''').fetchone()
    
    target = target_db.execute(f'''
        SELECT GVKEY, CIK, CONM, CONML, ADD1, ADD2, CITY, STATE, ADDZIP,
               SIC, PHONE, WEBURL, EIN, FYRC, GSECTOR, GGROUP, GIND, GSUBIND
        FROM main.COMPANY WHERE GVKEY = '{gvkey}'
    ''').fetchone()
    
    if not source or not target:
        logger.error(f"Missing data for {name}")
        return
    
    fields = ['GVKEY', 'CIK', 'CONM', 'CONML', 'ADD1', 'ADD2', 'CITY', 'STATE', 
              'ADDZIP', 'SIC', 'PHONE', 'WEBURL', 'EIN', 'FYRC', 
              'GSECTOR', 'GGROUP', 'GIND', 'GSUBIND']
    
    matches = 0
    mismatches = []
    
    for i, field in enumerate(fields):
        source_val = source[i] if source[i] is not None else ''
        target_val = target[i] if target[i] is not None else ''
        
        # Normalize for comparison
        source_str = str(source_val).strip().upper()
        target_str = str(target_val).strip().upper()
        
        if source_str == target_str:
            matches += 1
            logger.info(f"✅ {field:15s}: {source_val} = {target_val}")
        else:
            mismatches.append((field, source_val, target_val))
            logger.warning(f"❌ {field:15s}: Source={source_val} | Target={target_val}")
    
    logger.info(f"\nCompany Fields: {matches}/{len(fields)} matching ({matches/len(fields)*100:.1f}%)")
    return matches, len(fields), mismatches

def compare_financial_items(source_db, target_db, gvkey, name, tolerance=0.01):
    """Compare financial item values."""
    logger.info(f"\n{'='*80}")
    logger.info(f"FINANCIAL ITEMS VALIDATION: {name} (GVKEY: {gvkey})")
    logger.info(f"{'='*80}")
    
    # Get date range from target database (what we've extracted)
    target_date_range = target_db.execute(f'''
        SELECT MIN(k.CYEARQ), MAX(k.CYEARQ), MIN(k.CQTR), MAX(k.CQTR)
        FROM main.CSCO_IKEY k
        WHERE k.GVKEY = '{gvkey}' AND k.CYEARQ IS NOT NULL
    ''').fetchone()
    
    if target_date_range and target_date_range[0]:
        min_year, max_year, min_qtr, max_qtr = target_date_range
        logger.info(f"Target database date range: {min_year}-Q{min_qtr} to {max_year}-Q{max_qtr}")
    else:
        logger.warning("Could not determine target date range")
        min_year, max_year, min_qtr, max_qtr = None, None, None, None
    
    # Get all items that exist in both databases
    common_items = source_db.execute(f'''
        SELECT DISTINCT f1.ITEM
        FROM main.CSCO_IFNDQ f1
        JOIN main.CSCO_IKEY k1 ON f1.COIFND_ID = k1.COIFND_ID
        WHERE k1.GVKEY = '{gvkey}'
        AND f1.ITEM IN (
            SELECT DISTINCT f2.ITEM
            FROM main.CSCO_IFNDQ f2
            JOIN main.CSCO_IKEY k2 ON f2.COIFND_ID = k2.COIFND_ID
            WHERE k2.GVKEY = '{gvkey}'
        )
        ORDER BY f1.ITEM
    ''').df()['ITEM'].tolist()
    
    logger.info(f"Comparing {len(common_items)} common financial items...")
    
    # Get all records for each item
    item_stats = defaultdict(lambda: {'source_count': 0, 'target_count': 0, 
                                      'matches': 0, 'mismatches': 0, 
                                      'source_total': 0, 'target_total': 0,
                                      'max_diff': 0, 'max_diff_pct': 0})
    
    for item in common_items:
        # Build date filter for source records (only compare overlapping periods)
        date_filter = ""
        if min_year and max_year:
            date_filter = f"AND k.CYEARQ >= {min_year} AND k.CYEARQ <= {max_year}"
        
        # Get source records with all key fields for matching (only in date range)
        source_records = source_db.execute(f'''
            SELECT k.DATADATE, k.CQTR, k.CYEARQ, f.VALUEI
            FROM main.CSCO_IFNDQ f
            JOIN main.CSCO_IKEY k ON f.COIFND_ID = k.COIFND_ID
            WHERE k.GVKEY = '{gvkey}' AND f.ITEM = '{item}' {date_filter}
            ORDER BY k.CYEARQ, k.CQTR, k.DATADATE
        ''').fetchall()
        
        # Get target records with all key fields for matching
        target_records = target_db.execute(f'''
            SELECT k.DATADATE, k.CQTR, k.CYEARQ, f.VALUEI
            FROM main.CSCO_IFNDQ f
            JOIN main.CSCO_IKEY k ON f.COIFND_ID = k.COIFND_ID
            WHERE k.GVKEY = '{gvkey}' AND f.ITEM = '{item}'
            ORDER BY k.CYEARQ, k.CQTR, k.DATADATE
        ''').fetchall()
        
        item_stats[item]['source_count'] = len(source_records)
        item_stats[item]['target_count'] = len(target_records)
        
        # Create composite key dictionaries for comparison (use CQTR + CYEARQ as primary key)
        source_dict = {}
        for row in source_records:
            datadate, cqtr, cyearq, value = row
            # Use quarter + year as key, with datadate as fallback
            key = f"{cyearq}-Q{cqtr}" if cqtr and cyearq else str(datadate)
            source_dict[key] = value
        
        target_dict = {}
        for row in target_records:
            datadate, cqtr, cyearq, value = row
            key = f"{cyearq}-Q{cqtr}" if cqtr and cyearq else str(datadate)
            target_dict[key] = value
        
        # Compare values for matching dates
        for date, source_val in source_dict.items():
            if date in target_dict:
                target_val = target_dict[date]
                
                # Handle None values
                if source_val is None and target_val is None:
                    item_stats[item]['matches'] += 1
                    continue
                if source_val is None or target_val is None:
                    item_stats[item]['mismatches'] += 1
                    continue
                
                # Compare numeric values
                try:
                    source_float = float(source_val)
                    target_float = float(target_val)
                    
                    item_stats[item]['source_total'] += abs(source_float)
                    item_stats[item]['target_total'] += abs(target_float)
                    
                    # Calculate difference
                    diff = abs(source_float - target_float)
                    if abs(source_float) > 0:
                        diff_pct = (diff / abs(source_float)) * 100
                    else:
                        diff_pct = 100 if target_float != 0 else 0
                    
                    item_stats[item]['max_diff'] = max(item_stats[item]['max_diff'], diff)
                    item_stats[item]['max_diff_pct'] = max(item_stats[item]['max_diff_pct'], diff_pct)
                    
                    # Check if values match within tolerance
                    if abs(source_float) < 1.0:  # For very small values, use absolute tolerance
                        if diff < tolerance:
                            item_stats[item]['matches'] += 1
                        else:
                            item_stats[item]['mismatches'] += 1
                    else:  # For larger values, use percentage tolerance
                        if diff_pct < (tolerance * 100):
                            item_stats[item]['matches'] += 1
                        else:
                            item_stats[item]['mismatches'] += 1
                except (ValueError, TypeError):
                    item_stats[item]['mismatches'] += 1
    
    # Calculate summary statistics
    total_matches = sum(s['matches'] for s in item_stats.values())
    total_mismatches = sum(s['mismatches'] for s in item_stats.values())
    total_comparisons = total_matches + total_mismatches
    
    accuracy = (total_matches / total_comparisons * 100) if total_comparisons > 0 else 0
    
    logger.info(f"\nFinancial Items Summary:")
    logger.info(f"  Total items compared: {len(common_items)}")
    logger.info(f"  Total value comparisons: {total_comparisons:,}")
    if total_comparisons > 0:
        logger.info(f"  Matches: {total_matches:,} ({total_matches/total_comparisons*100:.1f}%)")
        logger.info(f"  Mismatches: {total_mismatches:,} ({total_mismatches/total_comparisons*100:.1f}%)")
        logger.info(f"  Overall accuracy: {accuracy:.1f}%")
    else:
        logger.warning(f"  No matching dates found for comparison!")
        logger.info(f"  Source records: {sum(s['source_count'] for s in item_stats.values()):,}")
        logger.info(f"  Target records: {sum(s['target_count'] for s in item_stats.values()):,}")
    
    # Find items with highest discrepancies
    high_diff_items = sorted(
        [(item, stats) for item, stats in item_stats.items() if stats['mismatches'] > 0],
        key=lambda x: x[1]['max_diff_pct'],
        reverse=True
    )[:20]
    
    if high_diff_items:
        logger.info(f"\nTop 20 items with highest discrepancies:")
        for item, stats in high_diff_items:
            match_rate = (stats['matches'] / (stats['matches'] + stats['mismatches']) * 100) if (stats['matches'] + stats['mismatches']) > 0 else 0
            logger.info(f"  {item:12s}: {stats['mismatches']:4d} mismatches, "
                       f"{match_rate:5.1f}% match rate, "
                       f"max diff: {stats['max_diff']:,.0f} ({stats['max_diff_pct']:.1f}%)")
    
    return {
        'total_items': len(common_items),
        'total_comparisons': total_comparisons,
        'matches': total_matches,
        'mismatches': total_mismatches,
        'accuracy': accuracy,
        'item_stats': item_stats
    }

def main():
    source_db_path = '/home/tasos/compustat.duckdb'
    target_db_path = 'compustat_edgar.duckdb'
    
    if not Path(source_db_path).exists():
        logger.error(f"Source database not found: {source_db_path}")
        return
    
    if not Path(target_db_path).exists():
        logger.error(f"Target database not found: {target_db_path}")
        return
    
    source_db = duckdb.connect(source_db_path, read_only=True)
    target_db = duckdb.connect(target_db_path, read_only=True)
    
    companies = [
        ('012141', 'MSFT'),
        ('117768', 'NVDA')
    ]
    
    results = {}
    
    for gvkey, name in companies:
        logger.info(f"\n{'#'*80}")
        logger.info(f"VALIDATING: {name} (GVKEY: {gvkey})")
        logger.info(f"{'#'*80}")
        
        # Compare company fields
        company_result = compare_company_fields(source_db, target_db, gvkey, name)
        results[f'{name}_company'] = company_result
        
        # Compare financial items
        financial_result = compare_financial_items(source_db, target_db, gvkey, name)
        results[f'{name}_financial'] = financial_result
    
    # Final summary
    logger.info(f"\n{'='*80}")
    logger.info("FINAL VALIDATION SUMMARY")
    logger.info(f"{'='*80}")
    
    for gvkey, name in companies:
        company_result = results.get(f'{name}_company')
        financial_result = results.get(f'{name}_financial')
        
        logger.info(f"\n{name}:")
        if company_result:
            matches, total, mismatches = company_result
            logger.info(f"  Company Fields: {matches}/{total} ({matches/total*100:.1f}%)")
            if mismatches:
                logger.info(f"    Mismatches: {', '.join([m[0] for m in mismatches])}")
        
        if financial_result:
            logger.info(f"  Financial Items: {financial_result['accuracy']:.1f}% accuracy")
            logger.info(f"    Items: {financial_result['total_items']}")
            logger.info(f"    Comparisons: {financial_result['total_comparisons']:,}")
            logger.info(f"    Matches: {financial_result['matches']:,} ({financial_result['matches']/financial_result['total_comparisons']*100:.1f}%)")
            logger.info(f"    Mismatches: {financial_result['mismatches']:,} ({financial_result['mismatches']/financial_result['total_comparisons']*100:.1f}%)")
    
    source_db.close()
    target_db.close()
    
    logger.info(f"\n{'='*80}")
    logger.info("Validation complete!")
    logger.info(f"{'='*80}")

if __name__ == '__main__':
    main()

