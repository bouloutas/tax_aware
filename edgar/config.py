"""
Configuration for EDGAR to Compustat replication project.
"""
from pathlib import Path
from datetime import date, timedelta

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_FILINGS_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MAPPING_FILE = PROJECT_ROOT / "cik_to_gvkey_mapping.csv"

# Database paths
COMPUSTAT_SOURCE_DB = Path("/home/tasos/compustat.duckdb")
COMPUSTAT_EDGAR_DB = PROJECT_ROOT / "compustat_edgar.duckdb"

# Date range for filings
# Compustat ends on 9/30/2025, go back 5 years
END_DATE = date(2025, 9, 30)
START_DATE = END_DATE - timedelta(days=5 * 365)  # Approximately 5 years

# SEC EDGAR configuration
SEC_EDGAR_BASE_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
SEC_EDGAR_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"
SEC_EDGAR_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index"

# Filing types to download
FILING_TYPES = [
    "10-K",      # Annual report
    "10-Q",      # Quarterly report
    "8-K",       # Current report
    "DEF 14A",   # Proxy statement
    "10-K/A",    # Amended annual report
    "10-Q/A",    # Amended quarterly report
]

# Rate limiting (SEC requires delays between requests)
REQUEST_DELAY_SECONDS = 0.1  # 100ms delay between requests
USER_AGENT = "Tax Aware Portfolio Management contact@example.com"  # SEC requires identification

# Database schema
COMPUSTAT_SCHEMA = "main"

# Logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_LEVEL = "INFO"

# Daily Update Settings
DAILY_UPDATE_LOOKBACK_DAYS = 1  # How many days to look back for new filings
DAILY_UPDATE_SCHEDULE = "0 6 * * *"  # 6 AM ET daily

# SEC EDGAR RSS Feeds for daily updates
SEC_RSS_FEEDS = {
    '10-K': 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=10-K&company=&dateb=&owner=include&count=100&output=atom',
    '10-Q': 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=10-Q&company=&dateb=&owner=include&count=100&output=atom',
    '10-K/A': 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=10-K%2FA&company=&dateb=&owner=include&count=100&output=atom',
    '10-Q/A': 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=10-Q%2FA&company=&dateb=&owner=include&count=100&output=atom',
}

# Reference database for validation
COMPUSTAT_REFERENCE_DB = PROJECT_ROOT / "legacy" / "compustat.duckdb"

