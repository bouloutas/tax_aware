#!/usr/bin/env python3
"""
Validate MSFT/NVDA data against compustat.duckdb
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
    """Compare MSFT/NVDA data between source and target databases."""
    logger.info("="*80)
    logger.info("Validating MSFT/NVDA Data Against Compustat")
    logger.info("="*80)
    
    source_db = Path("/home/tasos/compustat.duckdb")
    target_db = Path("compustat_edgar.duckdb")
    
    if not source_db.exists():
        logger.error(f"Source database not found: {source_db}")
        return 1
    
    if not target_db.exists():
        logger.error(f"Target database not found: {target_db}")
        return 1
    
    source_conn = duckdb.connect(str(source_db))
    target_conn = duckdb.connect(str(target_db))
    
    try:
        # Compare COMPANY table
        logger.info("\n=== COMPANY TABLE ===")
        for gvkey in ['012141', '117768']:
            source_data = source_conn.execute(f"""
                SELECT GVKEY, CIK, CONM 
                FROM main.COMPANY 
                WHERE GVKEY = '{gvkey}'
            """).fetchone()
            
            target_data = target_conn.execute(f"""
                SELECT GVKEY, CIK, CONM 
                FROM main.COMPANY 
                WHERE GVKEY = '{gvkey}'
            """).fetchone()
            
            logger.info(f"\nGVKEY {gvkey}:")
            if source_data:
                logger.info(f"  Source: CIK={source_data[1]}, Name={source_data[2]}")
            else:
                logger.warning(f"  Source: Not found")
            
            if target_data:
                logger.info(f"  Target: CIK={target_data[1]}, Name={target_data[2]}")
            else:
                logger.warning(f"  Target: Not found")
            
            if source_data and target_data:
                if source_data[1] == target_data[1]:
                    logger.info("  ✅ CIK matches")
                else:
                    logger.error(f"  ❌ CIK mismatch: {source_data[1]} vs {target_data[1]}")
        
        # Compare SECURITY table
        logger.info("\n=== SECURITY TABLE ===")
        for gvkey in ['012141', '117768']:
            source_data = source_conn.execute(f"""
                SELECT GVKEY, IID, TIC 
                FROM main.SECURITY 
                WHERE GVKEY = '{gvkey}' AND IID = '01'
            """).fetchone()
            
            target_data = target_conn.execute(f"""
                SELECT GVKEY, IID, TIC 
                FROM main.SECURITY 
                WHERE GVKEY = '{gvkey}' AND IID = '01'
            """).fetchone()
            
            logger.info(f"\nGVKEY {gvkey}:")
            if source_data:
                logger.info(f"  Source: TIC={source_data[2]}")
            else:
                logger.warning(f"  Source: Not found")
            
            if target_data:
                logger.info(f"  Target: TIC={target_data[2]}")
            else:
                logger.warning(f"  Target: Not found")
            
            if source_data and target_data:
                if source_data[2] == target_data[2]:
                    logger.info("  ✅ Ticker matches")
                else:
                    logger.error(f"  ❌ Ticker mismatch: {source_data[2]} vs {target_data[2]}")
        
        # Check FUNDA table (if exists) for FY 2024
        logger.info("\n=== FUNDA TABLE (FY 2024) ===")
        for gvkey in ['012141', '117768']:
            source_data = source_conn.execute(f"""
                SELECT GVKEY, DATADATE, REVT, AT, LT, SEQ, NI, EPSPX, CSHPR
                FROM main.FUNDA 
                WHERE GVKEY = '{gvkey}' 
                AND DATADATE >= '2024-01-01' 
                AND DATADATE <= '2024-12-31'
                ORDER BY DATADATE DESC
                LIMIT 1
            """).fetchone()
            
            if source_data:
                logger.info(f"\nGVKEY {gvkey} - Latest FY 2024 data:")
                logger.info(f"  Date: {source_data[1]}")
                logger.info(f"  Revenue: {source_data[2]}")
                logger.info(f"  Assets: {source_data[3]}")
                logger.info(f"  Liabilities: {source_data[4]}")
                logger.info(f"  Equity: {source_data[5]}")
                logger.info(f"  Net Income: {source_data[6]}")
                logger.info(f"  EPS: {source_data[7]}")
                logger.info(f"  Shares: {source_data[8]}")
            else:
                logger.warning(f"  GVKEY {gvkey}: No FUNDA data found for FY 2024")
        
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

