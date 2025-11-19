"""
Download SEC filings from EDGAR.
"""
import time
import requests
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import date, timedelta
import logging
import csv
import re
from urllib.parse import urljoin

from config import (
    RAW_FILINGS_DIR,
    SEC_EDGAR_ARCHIVE_URL,
    SEC_EDGAR_INDEX_URL,
    REQUEST_DELAY_SECONDS,
    USER_AGENT,
    FILING_TYPES,
    START_DATE,
    END_DATE,
    MAPPING_FILE,
)

logger = logging.getLogger(__name__)


class EdgarDownloader:
    """Download SEC filings from EDGAR."""
    
    def __init__(self):
        """Initialize downloader with rate limiting."""
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.request_delay = REQUEST_DELAY_SECONDS
        self.last_request_time = 0
        self.base_url = "https://www.sec.gov"
        
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """
        Make HTTP request with retry logic.
        
        Args:
            url: URL to request
            max_retries: Maximum number of retry attempts
            
        Returns:
            Response object or None if failed
        """
        self._rate_limit()
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=60)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {max_retries} attempts: {e}")
                    return None
        return None
    
    def load_cik_mapping(self) -> Dict[str, str]:
        """
        Load CIK to GVKEY mapping.
        
        Returns:
            Dictionary mapping CIK (as string, no leading zeros) to GVKEY
        """
        mapping = {}
        if not MAPPING_FILE.exists():
            logger.warning(f"Mapping file not found: {MAPPING_FILE}")
            return mapping
            
        with open(MAPPING_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cik = row['CIK'].strip().lstrip('0') or '0'  # Remove leading zeros, handle '0000000000'
                mapping[cik] = row['GVKEY']
        
        logger.info(f"Loaded {len(mapping)} CIK mappings")
        return mapping
    
    def get_full_index_url(self, year: int, quarter: int) -> str:
        """
        Get URL for SEC full-index file.
        
        Args:
            year: Year (e.g., 2020)
            quarter: Quarter (1-4)
            
        Returns:
            URL to full-index file
        """
        return f"{self.base_url}/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    
    def download_full_index(self, year: int, quarter: int) -> Optional[List[Dict]]:
        """
        Download and parse SEC full-index file for a quarter.
        
        Args:
            year: Year
            quarter: Quarter (1-4)
            
        Returns:
            List of filing records as dictionaries, or None if failed
        """
        url = self.get_full_index_url(year, quarter)
        logger.info(f"Downloading full-index for {year} Q{quarter}...")
        
        response = self._make_request(url)
        if not response:
            return None
        
        # Parse index file
        # Format: CIK|Company Name|Form Type|Date Filed|Filename
        filings = []
        lines = response.text.split('\n')
        
        # Skip header lines (first 10 lines are header)
        for line in lines[10:]:
            if not line.strip():
                continue
            
            parts = line.split('|')
            if len(parts) >= 5:
                try:
                    cik = parts[0].strip().lstrip('0') or '0'
                    company_name = parts[1].strip()
                    form_type = parts[2].strip()
                    date_filed = parts[3].strip()
                    filename = parts[4].strip()
                    
                    # Parse date (format: YYYY-MM-DD)
                    try:
                        if '-' in date_filed:
                            # Format: YYYY-MM-DD
                            year, month, day = date_filed.split('-')
                            filed_date = date(int(year), int(month), int(day))
                        else:
                            # Format: YYYYMMDD (fallback)
                            filed_date = date(int(date_filed[:4]), int(date_filed[4:6]), int(date_filed[6:8]))
                    except (ValueError, IndexError) as e:
                        logger.debug(f"Error parsing date '{date_filed}': {e}")
                        continue
                    
                    # Filter by date range and filing types
                    if START_DATE <= filed_date <= END_DATE and form_type in FILING_TYPES:
                        filings.append({
                            'cik': cik,
                            'company_name': company_name,
                            'form_type': form_type,
                            'date_filed': filed_date,
                            'filename': filename,
                            'accession_number': self._extract_accession_number(filename)
                        })
                except (ValueError, IndexError) as e:
                    logger.debug(f"Error parsing line: {line[:100]}... Error: {e}")
                    continue
        
        logger.info(f"Found {len(filings)} relevant filings in {year} Q{quarter}")
        return filings
    
    def _extract_accession_number(self, filename: str) -> str:
        """
        Extract accession number from filename.
        
        Args:
            filename: Filename like "edgar/data/1234567/0001234567-20-000001.txt"
            
        Returns:
            Accession number like "0001234567-20-000001"
        """
        match = re.search(r'/(\d{10}-\d{2}-\d{6})', filename)
        if match:
            return match.group(1)
        return ""
    
    def get_filing_url(self, cik: str, accession_number: str, filename: str) -> str:
        """
        Construct URL for a specific filing.
        
        Args:
            cik: Company CIK (without leading zeros)
            accession_number: Accession number
            filename: Original filename from index (e.g., "edgar/data/1000045/0000950170-24-014566.txt")
            
        Returns:
            URL to filing
        """
        # The filename from index already contains the full path
        # Format: "edgar/data/{CIK}/{accession_number}.txt"
        # Just prepend the base URL
        return f"{self.base_url}/Archives/{filename}"
    
    def download_filing(self, cik: str, accession_number: str, form_type: str, 
                       filename: str, date_filed: date) -> Optional[Path]:
        """
        Download a specific filing.
        
        Args:
            cik: Company CIK (without leading zeros)
            accession_number: SEC accession number
            form_type: Type of filing (e.g., '10-K')
            filename: Original filename from index
            date_filed: Date filing was filed
            
        Returns:
            Path to downloaded file, or None if download failed
        """
        # Create directory structure: data/raw/{year}/{quarter}/{cik}/
        year = date_filed.year
        quarter = (date_filed.month - 1) // 3 + 1
        company_dir = RAW_FILINGS_DIR / str(year) / f"Q{quarter}" / cik
        company_dir.mkdir(parents=True, exist_ok=True)
        
        # Create filename: {form_type}_{accession_number}.txt
        output_filename = f"{form_type}_{accession_number}.txt"
        output_path = company_dir / output_filename
        
        if output_path.exists():
            logger.debug(f"Filing already exists: {output_path}")
            return output_path
        
        # Construct URL
        url = self.get_filing_url(cik, accession_number, filename)
        
        response = self._make_request(url)
        if not response:
            logger.error(f"Failed to download filing: {accession_number}")
            return None
        
        try:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded: {form_type} for CIK {cik} ({date_filed}) -> {output_path}")
            return output_path
        except IOError as e:
            logger.error(f"Error writing file {output_path}: {e}")
            return None
    
    def download_quarter_indexes(self, start_date: date = None, end_date: date = None) -> List[Dict]:
        """
        Download all full-index files for date range.
        
        Args:
            start_date: Start date (default: START_DATE from config)
            end_date: End date (default: END_DATE from config)
            
        Returns:
            List of all filing records
        """
        if start_date is None:
            start_date = START_DATE
        if end_date is None:
            end_date = END_DATE
        
        all_filings = []
        current_date = start_date
        
        while current_date <= end_date:
            year = current_date.year
            quarter = (current_date.month - 1) // 3 + 1
            
            filings = self.download_full_index(year, quarter)
            if filings:
                all_filings.extend(filings)
            
            # Move to next quarter
            if quarter == 4:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, quarter * 3 + 1, 1)
        
        logger.info(f"Total filings found: {len(all_filings)}")
        return all_filings
    
    def download_filings_for_companies(self, cik_mapping: Dict[str, str] = None, 
                                      limit: Optional[int] = None,
                                      year: Optional[int] = None,
                                      quarter: Optional[int] = None) -> Dict[str, int]:
        """
        Download filings for all companies in mapping.
        
        Args:
            cik_mapping: CIK to GVKEY mapping (if None, loads from file)
            limit: Limit number of filings to download (for testing)
            year: Specific year to download (if None, downloads all in date range)
            quarter: Specific quarter to download (if None, downloads all in date range)
            
        Returns:
            Dictionary mapping CIK to count of filings downloaded
        """
        if cik_mapping is None:
            cik_mapping = self.load_cik_mapping()
        
        cik_set = set(cik_mapping.keys())
        logger.info(f"Looking for filings for {len(cik_set)} companies")
        
        # Get filings from indexes
        if year and quarter:
            logger.info(f"Downloading index for {year} Q{quarter}...")
            all_filings = self.download_full_index(year, quarter) or []
        else:
            logger.info("Downloading full-index files for date range...")
            all_filings = self.download_quarter_indexes()
        
        # Filter to companies in our mapping
        relevant_filings = [f for f in all_filings if f['cik'] in cik_set]
        logger.info(f"Found {len(relevant_filings)} filings for companies in mapping")
        
        # Apply limit if specified
        if limit:
            relevant_filings = relevant_filings[:limit]
            logger.info(f"Limited to {limit} filings for testing")
        
        # Download filings
        download_counts = {}
        total = len(relevant_filings)
        
        if total == 0:
            logger.warning("No filings to download")
            return download_counts
        
        logger.info(f"Starting download of {total} filings...")
        for i, filing in enumerate(relevant_filings, 1):
            cik = filing['cik']
            result = self.download_filing(
                cik,
                filing['accession_number'],
                filing['form_type'],
                filing['filename'],
                filing['date_filed']
            )
            
            if result:
                download_counts[cik] = download_counts.get(cik, 0) + 1
            
            # Progress logging more frequently for small batches
            if total <= 100:
                if i % 10 == 0 or i == total:
                    logger.info(f"Progress: {i}/{total} filings downloaded ({i/total*100:.1f}%)")
            else:
                if i % 100 == 0 or i == total:
                    logger.info(f"Progress: {i}/{total} filings downloaded ({i/total*100:.1f}%)")
        
        logger.info(f"Download complete. Processed {len(relevant_filings)} filings for {len(download_counts)} companies")
        return download_counts
