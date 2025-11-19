#!/usr/bin/env python3
"""
Validate extracted financial data against compustat.duckdb
"""
import sys
import logging
import duckdb
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Compare financial data between source and target databases."""
    logger.info("="*80)
    logger.info("Validating Financial Data Against Compustat")
    logger.info("="*80)
    
    source_db = Path("/home/tasos/compustat.duckdb")
    target_db = Path("compustat_edgar.duckdb")
    
    source_conn = duckdb.connect(str(source_db))
    target_conn = duckdb.connect(str(target_db))
    
    try:
        # Compare financial data for MSFT and NVDA
        for gvkey, name in [('012141', 'MSFT'), ('117768', 'NVDA')]:
            logger.info(f"\n=== {name} (GVKEY: {gvkey}) ===")
            
            # Get data from source Compustat
            try:
                source_data = source_conn.execute("""
                    SELECT 
                        k.DATADATE,
                        k.FQTR,
                        k.FYEARQ,
                        MAX(CASE WHEN f.ITEM = 'REVTQ' THEN f.VALUEI END) as REVTQ,
                        MAX(CASE WHEN f.ITEM = 'ATQ' THEN f.VALUEI END) as ATQ,
                        MAX(CASE WHEN f.ITEM = 'NIQ' THEN f.VALUEI END) as NIQ,
                        MAX(CASE WHEN f.ITEM = 'CEQQ' THEN f.VALUEI END) as CEQQ,
                        MAX(CASE WHEN f.ITEM = 'CHEQ' THEN f.VALUEI END) as CHEQ
                    FROM main.CSCO_IKEY k
                    LEFT JOIN main.CSCO_IFNDQ f ON k.COIFND_ID = f.COIFND_ID
                    WHERE k.GVKEY = ? 
                    AND k.DATADATE >= '2024-01-01'
                    AND k.DATADATE <= '2024-12-31'
                    GROUP BY k.DATADATE, k.FQTR, k.FYEARQ
                    ORDER BY k.DATADATE DESC
                    LIMIT 4
                """, [gvkey]).fetchall()
            except Exception as e:
                logger.warning(f"Could not query source Compustat: {e}")
                source_data = []
            
            # Get data from target database
            try:
                target_data = target_conn.execute("""
                    SELECT 
                        k.DATADATE,
                        k.FQTR,
                        k.FYEARQ,
                        MAX(CASE WHEN f.ITEM = 'REVTQ' THEN f.VALUEI END) as REVTQ,
                        MAX(CASE WHEN f.ITEM = 'ATQ' THEN f.VALUEI END) as ATQ,
                        MAX(CASE WHEN f.ITEM = 'NIQ' THEN f.VALUEI END) as NIQ,
                        MAX(CASE WHEN f.ITEM = 'CEQQ' THEN f.VALUEI END) as CEQQ,
                        MAX(CASE WHEN f.ITEM = 'CHEQ' THEN f.VALUEI END) as CHEQ
                    FROM main.CSCO_IKEY k
                    LEFT JOIN main.CSCO_IFNDQ f ON k.COIFND_ID = f.COIFND_ID
                    WHERE k.GVKEY = ?
                    GROUP BY k.DATADATE, k.FQTR, k.FYEARQ
                    ORDER BY k.DATADATE DESC
                    LIMIT 4
                """, [gvkey]).fetchall()
            except Exception as e:
                logger.error(f"Could not query target database: {e}")
                target_data = []
            
            logger.info(f"\nSource Compustat ({len(source_data)} records):")
            for row in source_data[:3]:
                rev_str = f"{row[3]:,.0f}" if row[3] else "N/A"
                assets_str = f"{row[4]:,.0f}" if row[4] else "N/A"
                ni_str = f"{row[5]:,.0f}" if row[5] else "N/A"
                logger.info(f"  {row[0]} Q{row[1]} FY{row[2]}: Rev={rev_str}, Assets={assets_str}, NI={ni_str}")
            
            logger.info(f"\nTarget Database ({len(target_data)} records):")
            for row in target_data[:3]:
                rev_str = f"{row[3]:,.0f}" if row[3] else "N/A"
                assets_str = f"{row[4]:,.0f}" if row[4] else "N/A"
                ni_str = f"{row[5]:,.0f}" if row[5] else "N/A"
                logger.info(f"  {row[0]} Q{row[1]} FY{row[2]}: Rev={rev_str}, Assets={assets_str}, NI={ni_str}")
            
            # Compare if we have matching dates
            if source_data and target_data:
                logger.info("\nComparison:")
                source_dict = {(row[0], row[1]): row for row in source_data}
                target_dict = {(row[0], row[1]): row for row in target_data}
                
                for key in sorted(set(source_dict.keys()) & set(target_dict.keys()))[:3]:
                    s_row = source_dict[key]
                    t_row = target_dict[key]
                    logger.info(f"  {key[0]} Q{key[1]}:")
                    if s_row[3] and t_row[3]:
                        diff = abs(s_row[3] - t_row[3]) / s_row[3] * 100
                        logger.info(f"    Revenue: Source={s_row[3]:,.0f}, Target={t_row[3]:,.0f}, Diff={diff:.1f}%")
                    if s_row[4] and t_row[4]:
                        diff = abs(s_row[4] - t_row[4]) / s_row[4] * 100
                        logger.info(f"    Assets: Source={s_row[4]:,.0f}, Target={t_row[4]:,.0f}, Diff={diff:.1f}%")
        
        logger.info("\n" + "="*80)
        logger.info("Validation complete!")
        
    except Exception as e:
        logger.error(f"Error during validation: {e}", exc_info=True)
        return 1
    finally:
        source_conn.close()
        target_conn.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

