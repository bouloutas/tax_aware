"""
Re-process MSFT and NVDA filings with fixed YTD parser.
This script clears existing financial data and re-processes filings.
"""
import sys
import logging
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).parent))

from src.data_extractor import DataExtractor
from config import COMPUSTAT_EDGAR_DB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/reprocess_msft_nvda.log')
    ]
)
logger = logging.getLogger(__name__)

def clear_existing_data(gvkeys):
    """Clear existing financial data for given GVKEYs."""
    con = duckdb.connect(COMPUSTAT_EDGAR_DB)
    
    for gvkey in gvkeys:
        logger.info(f"Clearing data for GVKEY {gvkey}...")
        # Get coifnd_ids for this gvkey
        coifnd_ids = con.execute("""
            SELECT coifnd_id FROM CSCO_IKEY WHERE gvkey = ?
        """, [gvkey]).fetchall()
        
        if coifnd_ids:
            ids = [row[0] for row in coifnd_ids]
            logger.info(f"Found {len(ids)} records to delete")
            
            # Delete from CSCO_IFNDQ
            con.execute(f"""
                DELETE FROM CSCO_IFNDQ 
                WHERE coifnd_id IN ({','.join(['?' for _ in ids])})
            """, ids)
            
            # Delete from CSCO_IKEY
            con.execute(f"""
                DELETE FROM CSCO_IKEY 
                WHERE coifnd_id IN ({','.join(['?' for _ in ids])})
            """, ids)
            
            logger.info(f"Deleted {len(ids)} records for GVKEY {gvkey}")
    
    con.close()

def reprocess_filings(gvkeys):
    """Re-process filings for given GVKEYs."""
    # Find CIKs for these GVKEYs
    con = duckdb.connect(COMPUSTAT_EDGAR_DB)
    ciks = {}
    for gvkey in gvkeys:
        cik = con.execute("SELECT cik FROM COMPANY WHERE gvkey = ?", [gvkey]).fetchone()
        if cik:
            ciks[gvkey] = cik[0].lstrip('0') or '0'
            logger.info(f"GVKEY {gvkey} -> CIK {ciks[gvkey]}")
    con.close()
    
    # Process filings
    extractor = DataExtractor()
    
    data_dir = Path('data/raw')
    all_extracted_data = []
    
    # Process 2023 Q3-Q4 and 2024 filings to ensure full fiscal year coverage for MSFT (FY ends June)
    # MSFT FY24 Q1 was filed in Oct 2023 (2023 Q4)
    years_quarters = [
        ('2023', ['Q3', 'Q4']),
        ('2024', ['Q1', 'Q2', 'Q3', 'Q4'])
    ]
    
    for year, quarters in years_quarters:
        for quarter in quarters:
            quarter_dir = data_dir / year / quarter
            if not quarter_dir.exists():
                continue
                
            logger.info(f"Processing {year} {quarter}...")
            
            # Find filings for our CIKs
            for gvkey, cik in ciks.items():
                cik_dir = quarter_dir / cik
                if not cik_dir.exists():
                    continue
                    
                logger.info(f"Processing GVKEY {gvkey} (CIK {cik}) in {year} {quarter}...")
                
                # Extract data
                extracted_data = extractor.extract_from_directory(cik_dir)
                all_extracted_data.extend(extracted_data)
                logger.info(f"Extracted {len(extracted_data)} filings")
    
    # Map and insert financial data using the extractor's populate method
    if all_extracted_data:
        logger.info(f"Total extracted: {len(all_extracted_data)} filings")
        extractor.populate_financial_tables(all_extracted_data)
        logger.info("Inserted financial data")
    
    extractor.close()
    logger.info("Re-processing complete!")

if __name__ == '__main__':
    # MSFT and NVDA GVKEYs
    gvkeys = ['012141', '117768']
    
    logger.info("Step 1: Clearing existing data...")
    clear_existing_data(gvkeys)
    
    logger.info("Step 2: Re-processing filings with fixed parser...")
    reprocess_filings(gvkeys)
    
    logger.info("Done!")

