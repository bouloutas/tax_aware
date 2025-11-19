#!/usr/bin/env python3
"""
Process MSFT and NVDA filings and populate compustat_edgar.duckdb
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.data_extractor import DataExtractor
from config import RAW_FILINGS_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Process MSFT and NVDA filings."""
    logger.info("="*80)
    logger.info("Processing MSFT and NVDA Filings")
    logger.info("="*80)
    
    # Target CIKs: MSFT=789019, NVDA=1045810
    target_ciks = {'789019', '1045810'}
    
    extractor = DataExtractor()
    
    try:
        # Process 2024 filings
        logger.info("Processing 2024 filings...")
        filings_2024 = []
        for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
            quarter_dir = RAW_FILINGS_DIR / '2024' / quarter
            if quarter_dir.exists():
                for cik_dir in quarter_dir.iterdir():
                    if cik_dir.is_dir() and cik_dir.name in target_ciks:
                        for filing_file in cik_dir.glob('*.txt'):
                            filings_2024.append(filing_file)
        
        logger.info(f"Found {len(filings_2024)} filings for MSFT/NVDA in 2024")
        
        # Extract data
        extracted_data = []
        for filing_path in filings_2024:
            data = extractor.extract_from_filing(filing_path)
            if data:
                extracted_data.append(data)
        
        logger.info(f"Extracted data from {len(extracted_data)} filings")
        
        # Populate database
        logger.info("Populating database...")
        extractor.populate_all_tables(extracted_data)
        
        logger.info("Done!")
        
        # Show summary
        import duckdb
        from config import COMPUSTAT_EDGAR_DB
        conn = duckdb.connect(str(COMPUSTAT_EDGAR_DB))
        
        msft_nvda = conn.execute("""
            SELECT GVKEY, CIK, CONM 
            FROM main.COMPANY 
            WHERE GVKEY IN ('012141', '117768')
        """).fetchall()
        
        logger.info("\nMSFT/NVDA in database:")
        for row in msft_nvda:
            logger.info(f"  {row[0]}: {row[2]} (CIK: {row[1]})")
        
        securities = conn.execute("""
            SELECT GVKEY, IID, TIC 
            FROM main.SECURITY 
            WHERE GVKEY IN ('012141', '117768')
        """).fetchall()
        
        logger.info("\nSecurities:")
        for row in securities:
            logger.info(f"  {row[0]}/{row[1]}: {row[2]}")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    finally:
        extractor.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

