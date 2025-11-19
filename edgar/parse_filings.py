#!/usr/bin/env python3
"""
Parse downloaded SEC filings and extract data.
"""
import sys
import logging
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.data_extractor import DataExtractor
from config import RAW_FILINGS_DIR, COMPUSTAT_EDGAR_DB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/parse_filings.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Parse SEC filings."""
    parser = argparse.ArgumentParser(description='Parse SEC filings and extract data')
    parser.add_argument('--directory', type=str, help='Directory containing filings (default: data/raw)')
    parser.add_argument('--filing-types', nargs='+', help='Filing types to process (e.g., 10-K 10-Q)')
    parser.add_argument('--limit', type=int, help='Limit number of filings to process (for testing)')
    parser.add_argument('--company-only', action='store_true', help='Only populate COMPANY table')
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("SEC Filing Parser")
    logger.info("="*80)
    
    # Determine directory
    directory = Path(args.directory) if args.directory else RAW_FILINGS_DIR
    if not directory.exists():
        logger.error(f"Directory not found: {directory}")
        return 1
    
    # Initialize extractor
    extractor = DataExtractor()
    
    try:
        # Extract data from filings
        logger.info(f"Extracting data from filings in {directory}...")
        extracted_data = extractor.extract_from_directory(
            directory,
            filing_types=args.filing_types
        )
        
        if args.limit:
            extracted_data = extracted_data[:args.limit]
            logger.info(f"Limited to {args.limit} filings for testing")
        
        if not extracted_data:
            logger.warning("No data extracted from filings")
            return 1
        
        # Populate database tables
        logger.info("Populating database tables...")
        
        if args.company_only:
            extractor.populate_company_table(extracted_data)
        else:
            # Populate all tables
            extractor.populate_all_tables(extracted_data)
        
        logger.info("Done!")
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    finally:
        extractor.close()


if __name__ == "__main__":
    sys.exit(main())

