# Task: Download SEC Filings

**Purpose:** Download SEC filings from EDGAR for companies  
**Status:** ✅ Complete  
**Last Updated:** November 18, 2025

## Overview

Download SEC filings (10-K, 10-Q, 8-K) from EDGAR for specified companies and time periods.

## Process Steps

### 1. Setup

**Required Files:**
- `cik_to_gvkey_mapping.csv` - Maps SEC CIK to Compustat GVKEY
- `config.py` - Configuration settings

**Configuration:**
```python
FILING_TYPES = ['10-K', '10-Q', '8-K']
START_DATE = '2020-01-01'
END_DATE = '2025-09-30'
REQUEST_DELAY_SECONDS = 0.1  # SEC rate limiting
USER_AGENT = 'Company Name email@example.com'  # SEC requires identification
```

### 2. Download Process

**Script:** `download_filings.py` or `download_targets.py`

**Steps:**
1. Load CIK-to-GVKEY mapping from `cik_to_gvkey_mapping.csv`
2. For each company (CIK):
   - Query EDGAR API for filings in date range
   - Filter by filing types (10-K, 10-Q, 8-K)
   - Download each filing
   - Save to `data/raw/{YEAR}/Q{QUARTER}/{CIK}/{FILING_TYPE}_{accession_number}.txt`
   - Log download metadata

**EDGAR API:**
- Base URL: `https://data.sec.gov/submissions/CIK{CIK}.json`
- Filing URL: `https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_number}/{filename}`

**Rate Limiting:**
- SEC requires: 10 requests/second max
- Use `REQUEST_DELAY_SECONDS` between requests
- Include proper User-Agent header

### 3. File Structure

```
data/raw/
  {YEAR}/
    Q{QUARTER}/
      {CIK}/
        {FILING_TYPE}_{accession_number}.txt
```

**Example:**
```
data/raw/2024/Q3/789019/10-K_0000950170-24-087843.txt
```

### 4. Download Metadata

**Manifest File:** `data/manifests/{YEAR}_Q{QUARTER}_manifest.csv`

**Columns:**
- CIK
- GVKEY
- Filing Type (10-K, 10-Q, 8-K)
- Filing Date
- Accession Number
- File Path
- Download Status
- File Size
- Checksum (optional)

### 5. Error Handling

**Common Issues:**
- 404 Not Found: Filing may have been withdrawn or URL incorrect
- Rate Limiting: Too many requests - increase delay
- Network Errors: Retry with exponential backoff

**Retry Logic:**
- Max 3 attempts per filing
- Exponential backoff: 1s, 2s, 4s
- Log failures for manual review

### 6. Validation

**After Download:**
- Verify file exists and is not empty
- Check file size (should be > 0)
- Validate file format (should contain SEC header)
- Compare manifest counts with expected filings

### 7. Code Example

```python
from src.edgar_downloader import EdgarDownloader
from pathlib import Path

downloader = EdgarDownloader()

# Download for specific companies
ciks = ['789019', '1045810']  # MSFT, NVDA
downloader.download_filings_for_companies(
    ciks=ciks,
    start_date='2024-01-01',
    end_date='2024-12-31',
    filing_types=['10-K', '10-Q', '8-K']
)
```

### 8. Scaling to All Companies

**For Full Universe (37,071 companies):**
1. Load all CIKs from `cik_to_gvkey_mapping.csv`
2. Process in batches (e.g., 100 companies at a time)
3. Use parallel processing with rate limiting
4. Monitor progress and handle failures
5. Generate comprehensive manifest

**Estimated Time:**
- ~37,071 companies × ~4 filings/year × 5 years = ~741,420 filings
- At 10 requests/second = ~20 hours (plus processing time)

### 9. Dependencies

- `requests` - HTTP library
- `time` - Rate limiting delays
- `pathlib` - File path handling
- `csv` - Manifest generation

### 10. Output Files

- Raw filings: `data/raw/{YEAR}/Q{QUARTER}/{CIK}/*.txt`
- Manifest: `data/manifests/{YEAR}_Q{QUARTER}_manifest.csv`
- Logs: `logs/download_{timestamp}.log`

## Notes

- SEC requires User-Agent identification
- Respect rate limits (10 req/sec)
- Handle filing withdrawals gracefully
- Store metadata for traceability

