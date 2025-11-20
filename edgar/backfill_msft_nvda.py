
import sys
import logging
from datetime import date
from pathlib import Path

# Monkeypatch config before other imports use it
import config
# Set START_DATE to 1994-01-01 to capture all EDGAR history
config.START_DATE = date(1994, 1, 1)

# Now import the rest
from src.edgar_downloader import EdgarDownloader
from src.data_extractor import DataExtractor
from config import RAW_FILINGS_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def backfill():
    # MSFT: 789019 -> 012141
    # NVDA: 1045810 -> 117768
    target_mapping = {'789019': '012141', '1045810': '117768'}
    
    logger.info("="*80)
    logger.info("BACKFILL OPERATION: MSFT & NVDA (1994-2025)")
    logger.info("="*80)
    
    logger.info("Step 1: Downloading historical filings...")
    downloader = EdgarDownloader()
    
    # Download
    try:
        counts = downloader.download_filings_for_companies(
            cik_mapping=target_mapping,
            limit=5000  # Cover everything
        )
        logger.info(f"Downloaded counts: {counts}")
    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
        return

    logger.info("\nStep 2: Processing filings...")
    extractor = DataExtractor()
    
    all_data = []
    
    # Manually iterate to find files for our target companies
    # (extract_from_directory processes everything, which might be slow if we have other files)
    # But currently we likely only have these 2 companies or very few others.
    # Safer to filter.
    
    years = sorted([y for y in RAW_FILINGS_DIR.glob('*') if y.is_dir()])
    logger.info(f"Scanning {len(years)} years of data...")
    
    for year_dir in years:
        for q_dir in sorted(year_dir.glob('Q*')):
            for cik in target_mapping.keys():
                cik_dir = q_dir / cik
                if cik_dir.exists():
                    for filing in sorted(cik_dir.glob('*.txt')):
                        try:
                            data = extractor.extract_from_filing(filing)
                            if data:
                                all_data.append(data)
                        except Exception as e:
                            logger.error(f"Failed to parse {filing}: {e}")
    
    logger.info(f"Extracted data from {len(all_data)} filings.")
    
    if all_data:
        logger.info("Populating database tables...")
        extractor.populate_all_tables(all_data)
        logger.info("Database population complete.")
    
    extractor.close()
    logger.info("Backfill complete.")

if __name__ == '__main__':
    backfill()

