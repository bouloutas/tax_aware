#!/usr/bin/env python3
"""
Download SEC filings from EDGAR for companies in CIK mapping.
"""
import sys
import logging
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.edgar_downloader import EdgarDownloader
from config import START_DATE, END_DATE

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/download_filings.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Download SEC filings."""
    parser = argparse.ArgumentParser(description='Download SEC filings from EDGAR')
    parser.add_argument('--limit', type=int, help='Limit number of filings to download (for testing)')
    parser.add_argument('--year', type=int, help='Download only specific year')
    parser.add_argument('--quarter', type=int, choices=[1, 2, 3, 4], help='Download only specific quarter')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be downloaded without downloading')
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("SEC EDGAR Filing Downloader")
    logger.info("="*80)
    logger.info(f"Date range: {START_DATE} to {END_DATE}")
    
    downloader = EdgarDownloader()
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No files will be downloaded")
        # Just show what would be downloaded
        if args.year and args.quarter:
            filings = downloader.download_full_index(args.year, args.quarter)
            if filings:
                logger.info(f"Would download {len(filings)} filings for {args.year} Q{args.quarter}")
                # Show sample
                for f in filings[:10]:
                    logger.info(f"  {f['form_type']} - CIK {f['cik']} - {f['date_filed']}")
        else:
            all_filings = downloader.download_quarter_indexes()
            cik_mapping = downloader.load_cik_mapping()
            cik_set = set(cik_mapping.keys())
            relevant = [f for f in all_filings if f['cik'] in cik_set]
            logger.info(f"Would download {len(relevant)} filings for {len(cik_set)} companies")
    else:
        # Actual download
        if args.year and args.quarter:
            logger.info(f"Downloading filings for {args.year} Q{args.quarter}...")
            filings = downloader.download_full_index(args.year, args.quarter)
            if filings:
                cik_mapping = downloader.load_cik_mapping()
                cik_set = set(cik_mapping.keys())
                relevant = [f for f in filings if f['cik'] in cik_set]
                
                for filing in relevant:
                    downloader.download_filing(
                        filing['cik'],
                        filing['accession_number'],
                        filing['form_type'],
                        filing['filename'],
                        filing['date_filed']
                    )
        else:
            # Download all filings
            download_counts = downloader.download_filings_for_companies(
                limit=args.limit,
                year=args.year,
                quarter=args.quarter
            )
            
            logger.info("\nDownload Summary:")
            logger.info(f"  Companies with filings: {len(download_counts)}")
            logger.info(f"  Total filings downloaded: {sum(download_counts.values())}")
            if download_counts:
                avg = sum(download_counts.values()) / len(download_counts)
                logger.info(f"  Average filings per company: {avg:.1f}")
                logger.info(f"  Max filings for one company: {max(download_counts.values())}")
    
    logger.info("Done!")


if __name__ == "__main__":
    main()

