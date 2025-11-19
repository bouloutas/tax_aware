#!/usr/bin/env python3
"""
Download filings and process them in one go.
"""
import sys
import logging
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.edgar_downloader import EdgarDownloader
from src.data_extractor import DataExtractor
from config import COMPUSTAT_EDGAR_DB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/download_and_process.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Download and process SEC filings')
    parser.add_argument('--year', type=int, help='Year to download')
    parser.add_argument('--quarter', type=int, choices=[1, 2, 3, 4], help='Quarter to download')
    parser.add_argument('--limit', type=int, default=50, help='Limit number of filings (default: 50)')
    parser.add_argument('--skip-download', action='store_true', help='Skip download, only process existing files')
    parser.add_argument('--skip-process', action='store_true', help='Skip processing, only download')
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("SEC EDGAR Download and Process Pipeline")
    logger.info("="*80)
    
    # Step 1: Download filings
    if not args.skip_download:
        logger.info("Step 1: Downloading filings...")
        downloader = EdgarDownloader()
        try:
            download_counts = downloader.download_filings_for_companies(
                limit=args.limit,
                year=args.year,
                quarter=args.quarter
            )
            logger.info(f"Downloaded {sum(download_counts.values())} filings for {len(download_counts)} companies")
        except KeyboardInterrupt:
            logger.warning("Download interrupted by user")
        except Exception as e:
            logger.error(f"Download error: {e}", exc_info=True)
    else:
        logger.info("Skipping download step")
    
    # Step 2: Process filings
    if not args.skip_process:
        logger.info("\nStep 2: Processing filings...")
        extractor = DataExtractor()
        try:
            from config import RAW_FILINGS_DIR
            extracted_data = extractor.extract_from_directory(RAW_FILINGS_DIR, limit=args.limit)
            
            if extracted_data:
                logger.info(f"Extracted data from {len(extracted_data)} filings")
                extractor.populate_all_tables(extracted_data)
                logger.info("Database population complete!")
            else:
                logger.warning("No data extracted from filings")
        except Exception as e:
            logger.error(f"Processing error: {e}", exc_info=True)
        finally:
            extractor.close()
    else:
        logger.info("Skipping processing step")
    
    logger.info("\nPipeline complete!")


if __name__ == "__main__":
    main()

