#!/usr/bin/env python3
"""
Full pipeline to download and process SEC filings for 5-year date range.
"""
import sys
import logging
from pathlib import Path
from datetime import date, timedelta
from typing import List, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from src.edgar_downloader import EdgarDownloader
from src.data_extractor import DataExtractor
from config import START_DATE, END_DATE, COMPUSTAT_EDGAR_DB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/full_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_quarters_in_range(start_date: date, end_date: date) -> List[Tuple[int, int]]:
    """Get list of (year, quarter) tuples for date range."""
    quarters = []
    current_date = start_date
    
    while current_date <= end_date:
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        quarters.append((year, quarter))
        
        # Move to next quarter
        if quarter == 4:
            current_date = date(year + 1, 1, 1)
        else:
            current_date = date(year, quarter * 3 + 1, 1)
    
    return quarters


def main():
    """Run full pipeline for 5-year date range."""
    logger.info("="*80)
    logger.info("Full Pipeline: Download and Process SEC Filings (5 Years)")
    logger.info("="*80)
    logger.info(f"Date range: {START_DATE} to {END_DATE}")
    
    # Get all quarters in range
    quarters = get_quarters_in_range(START_DATE, END_DATE)
    logger.info(f"Processing {len(quarters)} quarters")
    
    downloader = EdgarDownloader()
    extractor = DataExtractor()
    
    total_downloaded = 0
    total_processed = 0
    
    try:
        for year, quarter in quarters:
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing {year} Q{quarter}")
            logger.info(f"{'='*80}")
            
            # Download filings for this quarter
            logger.info(f"Downloading filings for {year} Q{quarter}...")
            try:
                download_counts = downloader.download_filings_for_companies(
                    year=year,
                    quarter=quarter
                )
                quarter_downloaded = sum(download_counts.values())
                total_downloaded += quarter_downloaded
                logger.info(f"Downloaded {quarter_downloaded} filings for {year} Q{quarter}")
            except Exception as e:
                logger.error(f"Error downloading {year} Q{quarter}: {e}")
                continue
            
            # Process filings for this quarter
            logger.info(f"Processing filings for {year} Q{quarter}...")
            try:
                from config import RAW_FILINGS_DIR
                quarter_dir = RAW_FILINGS_DIR / str(year) / f"Q{quarter}"
                
                if quarter_dir.exists():
                    extracted_data = extractor.extract_from_directory(quarter_dir)
                    if extracted_data:
                        extractor.populate_all_tables(extracted_data)
                        total_processed += len(extracted_data)
                        logger.info(f"Processed {len(extracted_data)} filings for {year} Q{quarter}")
                else:
                    logger.warning(f"Directory not found: {quarter_dir}")
            except Exception as e:
                logger.error(f"Error processing {year} Q{quarter}: {e}")
                continue
        
        logger.info(f"\n{'='*80}")
        logger.info("Pipeline Complete!")
        logger.info(f"{'='*80}")
        logger.info(f"Total filings downloaded: {total_downloaded}")
        logger.info(f"Total filings processed: {total_processed}")
        
        # Final database status
        import duckdb
        conn = duckdb.connect(str(COMPUSTAT_EDGAR_DB))
        companies = conn.execute("SELECT COUNT(*) FROM main.COMPANY").fetchone()[0]
        securities = conn.execute("SELECT COUNT(*) FROM main.SECURITY").fetchone()[0]
        identifiers = conn.execute("SELECT COUNT(*) FROM main.SEC_IDCURRENT").fetchone()[0]
        conn.close()
        
        logger.info(f"\nFinal Database Status:")
        logger.info(f"  Companies: {companies}")
        logger.info(f"  Securities: {securities}")
        logger.info(f"  Identifiers: {identifiers}")
        
    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
    finally:
        extractor.close()


if __name__ == "__main__":
    main()

