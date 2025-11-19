# Task: Scale to All Companies

**Purpose:** Scale the extraction and mapping process to all 37,071 companies  
**Status:** 📋 Planned  
**Last Updated:** November 18, 2025

## Overview

Scale the SEC filing download, parsing, and mapping process from pilot companies (MSFT, NVDA) to all companies in the CIK-to-GVKEY mapping file.

## Prerequisites

**Completed Tasks:**
- ✅ Download process working for MSFT/NVDA
- ✅ Parsing process working for MSFT/NVDA
- ✅ Mapping process working for MSFT/NVDA
- ✅ Validation process working for MSFT/NVDA
- ✅ All core tables populated correctly

**Documentation:**
- `TASK_DOWNLOAD_FILINGS.md` - Download process
- `TASK_PARSE_FILINGS.md` - Parsing process
- `TASK_MAP_TO_COMPUSTAT.md` - Mapping process
- `TASK_EXTRACT_FINANCIAL_ITEMS.md` - Financial extraction
- `TASK_VALIDATE_DATA.md` - Validation process

## Process Steps

### 1. Preparation

**Load Company List:**
```python
import csv
from pathlib import Path

companies = []
with open('cik_to_gvkey_mapping.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        companies.append({
            'cik': row['CIK'].strip().lstrip('0') or '0',
            'gvkey': row['GVKEY']
        })

print(f"Total companies: {len(companies)}")  # Should be 37,071
```

### 2. Batch Processing Strategy

**Approach:**
- Process companies in batches (e.g., 100 at a time)
- Monitor progress and handle failures
- Resume from last successful batch if interrupted

**Batch Size:**
- Download: 100 companies per batch
- Parsing: 1000 filings per batch
- Mapping: 1000 filings per batch

**Code:**
```python
BATCH_SIZE = 100
total_batches = (len(companies) + BATCH_SIZE - 1) // BATCH_SIZE

for batch_num in range(total_batches):
    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(companies))
    batch = companies[start_idx:end_idx]
    
    print(f"Processing batch {batch_num + 1}/{total_batches}")
    process_batch(batch)
```

### 3. Download All Filings

**Script:** `run_full_pipeline.py` or `download_all_companies.py`

**Steps:**
1. Load all companies from mapping file
2. For each company (in batches):
   - Download filings for 5-year period (2020-2025)
   - Download 10-K, 10-Q, 8-K filings
   - Save to `data/raw/{YEAR}/Q{QUARTER}/{CIK}/`
   - Update manifest

**Estimated Time:**
- 37,071 companies × ~4 filings/year × 5 years = ~741,420 filings
- At 10 requests/second = ~20 hours (plus processing time)
- With batching and error handling: ~24-48 hours

**Code:**
```python
from src.edgar_downloader import EdgarDownloader
import csv

downloader = EdgarDownloader()

# Load all companies
companies = []
with open('cik_to_gvkey_mapping.csv', 'r') as f:
    reader = csv.DictReader(f)
    companies = [row['CIK'].strip().lstrip('0') or '0' for row in reader]

# Process in batches
BATCH_SIZE = 100
for i in range(0, len(companies), BATCH_SIZE):
    batch = companies[i:i+BATCH_SIZE]
    downloader.download_filings_for_companies(
        ciks=batch,
        start_date='2020-01-01',
        end_date='2025-09-30',
        filing_types=['10-K', '10-Q', '8-K']
    )
```

### 4. Parse All Filings

**Script:** `parse_all_filings.py`

**Steps:**
1. Find all downloaded filings
2. For each filing (in batches):
   - Detect format (XBRL, HTML, Text)
   - Parse using appropriate parser
   - Extract all available data
   - Save parsed data (or process immediately)

**Estimated Time:**
- ~741,420 filings × ~2 seconds/filing = ~1,482,840 seconds = ~17 days
- With parallel processing (8 cores): ~2-3 days

**Code:**
```python
from src.data_extractor import DataExtractor
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

extractor = DataExtractor()

# Find all filings
filing_dir = Path('data/raw')
all_filings = list(filing_dir.rglob('*.txt'))

# Process in batches
BATCH_SIZE = 1000
for i in range(0, len(all_filings), BATCH_SIZE):
    batch = all_filings[i:i+BATCH_SIZE]
    extracted_data = extractor.extract_from_directory(
        filing_dir,
        limit=BATCH_SIZE
    )
    # Process extracted data
```

### 5. Map All Data

**Script:** `map_all_data.py`

**Steps:**
1. Load all parsed data
2. Group by GVKEY for COMPANY/SECURITY tables
3. Group by COIFND_ID for CSCO_IKEY/CSCO_IFNDQ tables
4. Populate all tables

**Code:**
```python
from src.data_extractor import DataExtractor

extractor = DataExtractor()

# Load all extracted data (or process incrementally)
all_data = load_all_parsed_data()

# Populate tables
extractor.populate_company_table(all_data)
extractor.populate_security_table(all_data)
extractor.populate_sec_idcurrent_table(all_data)
extractor.populate_financial_tables(all_data)
```

### 6. Validate Sample

**Before Full Validation:**
1. Validate sample companies (100-1000 companies)
2. Fix issues identified
3. Re-process sample
4. Validate again

**Full Validation:**
1. Compare record counts
2. Compare field population
3. Compare values for sample companies
4. Generate validation report

### 7. Error Handling

**Common Issues:**
- Missing filings (withdrawn, not available)
- Parsing failures (malformed files)
- Mapping failures (missing GVKEY, etc.)
- Database errors (constraints, etc.)

**Handling:**
- Log all errors
- Continue processing other companies
- Generate error report
- Retry failed items

**Code:**
```python
errors = []
for company in companies:
    try:
        process_company(company)
    except Exception as e:
        errors.append({
            'company': company,
            'error': str(e),
            'timestamp': datetime.now()
        })
        logger.error(f"Error processing {company}: {e}")

# Generate error report
with open('logs/errors.csv', 'w') as f:
    writer = csv.DictWriter(f, fieldnames=['company', 'error', 'timestamp'])
    writer.writeheader()
    writer.writerows(errors)
```

### 8. Progress Tracking

**Track:**
- Companies processed
- Filings downloaded
- Filings parsed
- Records inserted
- Errors encountered

**Report:**
```python
progress = {
    'companies_total': 37071,
    'companies_processed': 0,
    'filings_downloaded': 0,
    'filings_parsed': 0,
    'records_inserted': 0,
    'errors': 0
}

# Update and save progress
def save_progress(progress):
    with open('progress.json', 'w') as f:
        json.dump(progress, f, indent=2)
```

### 9. Performance Optimization

**Optimizations:**
- Parallel processing (multiple cores)
- Batch database inserts
- Incremental processing (process as downloaded)
- Database indexing
- Connection pooling

**Code:**
```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

# Parallel parsing
with ProcessPoolExecutor(max_workers=8) as executor:
    results = executor.map(parse_filing, all_filings)

# Batch database inserts
def batch_insert(records, batch_size=1000):
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        conn.executemany("INSERT ...", batch)
```

### 10. Monitoring

**Monitor:**
- Processing rate (companies/hour, filings/hour)
- Error rate
- Database growth
- Disk space
- Memory usage

**Alerts:**
- Error rate > 5%
- Processing rate < threshold
- Disk space < 10GB
- Memory usage > 80%

## Estimated Timeline

| Phase | Duration | Notes |
|-------|----------|-------|
| Download | 24-48 hours | With rate limiting |
| Parsing | 2-3 days | With parallel processing |
| Mapping | 1-2 days | Batch inserts |
| Validation | 1 day | Sample validation |
| **Total** | **4-7 days** | Continuous processing |

## Dependencies

- All task documentation files
- CIK-to-GVKEY mapping file
- Sufficient disk space (~500GB for filings)
- Database capacity
- Network bandwidth for downloads

## Output Files

- Processed filings: `data/raw/{YEAR}/Q{QUARTER}/{CIK}/`
- Database: `compustat_edgar.duckdb`
- Progress: `progress.json`
- Logs: `logs/scale_{timestamp}.log`
- Error report: `logs/errors_{timestamp}.csv`

## Notes

- Start with small batches to validate process
- Monitor closely for first 1000 companies
- Scale up batch sizes as confidence increases
- Keep detailed logs for debugging
- Generate progress reports regularly

## Success Criteria

- [ ] All 37,071 companies processed
- [ ] All available filings downloaded
- [ ] All filings parsed successfully
- [ ] All tables populated
- [ ] Validation shows >95% accuracy
- [ ] Error rate < 5%

