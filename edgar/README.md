# EDGAR to Compustat Database Replication

## Overview

This project replicates the Compustat database (`compustat.duckdb`) by extracting data from SEC EDGAR filings and populating a new database (`compustat_edgar.duckdb`). The goal is to create a 5-year historical dataset (back to ~10/1/2020) that mirrors the structure and content of the Compustat database, ending on 9/30/2025.

**Status:** Pipeline Operational ✅ | Scaling Up ⏳

## Latest Update — 2025-11-19

- Added chronological sorting plus fiscal YTD-to-quarter conversion so quarterly inserts now include 10-K (fiscal Q4) filings and subtract prior YTD values.
- Enabled 10-K ingestion in the financial loader while still filtering out filings without a document period end date.
- Reset YTD history per population run to prevent cross-company bleed-through.
- Removed heuristic fillers for thinly-defined Compustat items (for example CSTKQ, FCAQ, OLMIQ) so only filing-sourced values persist.
- Added preferred-source mappings for RCDQ/PRCRAQ/CSHOPQ/TXDITCQ/OCI fields to pull the exact XBRL tags present in MSFT/NVDA filings.

- Recomputed core EPS items (basic, diluted, fully diluted) from quarter-level net income and share counts so annualized values stop inheriting YTD noise.
- Added signed receivable/allowance mapping (RCDQ/PRCRAQ/RCPQ/RCAQ) and pushed negative contra-asset handling into the parser.
- Wired OCI + operating lease tags directly into Compustat items (AOCIDERGLQ/CIDERGLQ/OLMIQ/OLMTQ/MSAQ) and introduced par-value driven CSTKQ/CSTKCVQ derivations.

## Documentation

- **[PRD.md](PRD.md)** - Product Requirements Document with detailed specifications
- **[PROJECT_STATUS.md](PROJECT_STATUS.md)** - Current project status and progress tracking
- **[PROJECT_STATE_2025_11_19.md](PROJECT_STATE_2025_11_19.md)** - **CURRENT STATE** - Detailed state snapshot for session continuation

## Quick Start

### Prerequisites

```bash
pip install -r requirements.txt
```

### Initialize Database

```bash
python initialize_database.py
```

This creates the target database `compustat_edgar.duckdb` with schema matching the source Compustat database.

## Project Structure

```
edgar/
├── README.md                      # This file
├── PRD.md                         # Product Requirements Document
├── PROJECT_STATUS.md              # Current status and progress
├── config.py                      # Configuration settings
├── requirements.txt               # Python dependencies
├── initialize_database.py         # Database initialization script
├── download_filings.py            # SEC filing download script
├── parse_filings.py               # SEC filing parser script
├── download_and_process.py        # Combined download and process script
├── run_full_pipeline.py           # Full 5-year pipeline script
├── cik_to_gvkey_mapping.csv       # CIK → GVKEY mapping (37,071 companies)
├── compustat_edgar.duckdb         # Target database (created on init)
├── src/
│   ├── __init__.py
│   ├── schema_mapper.py           # Schema replication utilities
│   ├── database_builder.py        # Database initialization
│   ├── edgar_downloader.py        # SEC filing downloader
│   ├── filing_parser.py           # Filing parsers (XBRL, HTML, text)
│   └── data_extractor.py          # Data extraction and Compustat mapping
├── data/
│   ├── raw/                       # Raw SEC filings
│   └── processed/                 # Processed filing data
└── logs/                          # Application logs
```

## Key Components

### 1. CIK-to-GVKEY Mapping ✅

- **File:** `cik_to_gvkey_mapping.csv`
- **Companies:** 37,071 with CIK values
- **Purpose:** Links SEC CIK identifiers to Compustat GVKEYs
- **Status:** Complete

### 2. Schema Mapping ✅

- **Module:** `src/schema_mapper.py`
- **Purpose:** Replicates Compustat table schemas in target database
- **Status:** Complete

### 3. Database Builder ✅

- **Module:** `src/database_builder.py`
- **Purpose:** Initializes and manages target database
- **Status:** Complete

### 4. EDGAR Downloader ✅

- **Module:** `src/edgar_downloader.py`
- **Script:** `download_filings.py`
- **Purpose:** Downloads SEC filings from EDGAR
- **Status:** Complete and ready for use
- **Features:**
  - Full-index file download (quarterly indexes)
  - Rate limiting (100ms delay) with exponential backoff
  - CIK mapping integration
  - Filing type filtering (10-K, 10-Q, 8-K, etc.)
  - Error handling and retries
  - Progress tracking
  - Dry-run mode for testing

### 5. Filing Parser ✅

- **Module:** `src/filing_parser.py`
- **Script:** `parse_filings.py`
- **Purpose:** Parses SEC filings (XBRL, HTML, text)
- **Status:** Implemented and Enhanced
- **Features:**
  - XBRL parser for structured data with comprehensive financial tags
  - HTML parser with BeautifulSoup and table extraction
  - Text parser as fallback with regex patterns
  - Automatic parser selection based on filing format
  - Metadata extraction (CIK, company name, filing date)
  - Financial data extraction: revenue, assets, liabilities, equity, net income, EPS, cash, debt, shares outstanding
  - Security identifier extraction (ticker symbols from multiple sources)

### 6. Data Extractor ✅

- **Module:** `src/data_extractor.py`
- **Purpose:** Extract and map data from parsed filings to Compustat schema
- **Status:** Implemented and Enhanced
- **Features:**
  - Maps extracted data to Compustat COMPANY table
  - Maps extracted data to Compustat SECURITY table
  - Maps extracted data to Compustat SEC_IDCURRENT table
  - CIK to GVKEY mapping integration
  - Batch processing of multiple filings
  - `populate_all_tables()` method for complete population

## Key Compustat Tables to Replicate

Based on schema analysis of the source database (211 tables total):

| Table | Rows (Source) | Purpose |
|-------|---------------|---------|
| `COMPANY` | 56,321 | Company master data with CIK |
| `SECURITY` | 74,779 | Security/issue level data |
| `SEC_IDCURRENT` | 74,699 | Current identifiers |
| `SEC_DPRC` | 159M+ | Daily price data |
| `FUNDA` | - | Annual fundamentals |
| `FUNDQ` | - | Quarterly fundamentals |
| `FUNDY` | - | Year-to-date fundamentals |

## Configuration

Edit `config.py` to customize:

- **Date Range:** Start and end dates for filings
- **Filing Types:** Which filing types to download (10-K, 10-Q, 8-K, etc.)
- **Rate Limiting:** Request delays (SEC requires respectful usage)
- **Paths:** Database and data directory paths

## Date Range

- **Start Date:** October 1, 2020 (5 years back from end date)
- **End Date:** September 30, 2025
- **Source Compustat:** Extends to 2025-10-27

## Implementation Phases

### Phase 1: Foundation ✅ (COMPLETED)
- [x] Create CIK-to-GVKEY mapping
- [x] Analyze Compustat schema
- [x] Set up project structure
- [x] Create configuration system
- [x] Build schema mapper infrastructure
- [x] Create database initialization scripts

### Phase 2: EDGAR Download Infrastructure ✅ (COMPLETED)
- [x] Implement SEC full-index file download
- [x] Build company-specific filing index retrieval
- [x] Create filing downloader with rate limiting
- [x] Implement retry logic and error handling
- [x] Add progress tracking and logging
- [x] Create command-line download script

### Phase 3: Filing Parsers ✅ (COMPLETED)
- [x] XBRL parser for structured data
- [x] HTML parser for unstructured filings
- [x] Text parser as fallback
- [x] Extract financial statement data
- [x] Extract company metadata
- [x] Create data extractor for Compustat mapping
- [x] Create parsing script

### Phase 4: Data Transformation ✅ (IN PROGRESS)
- [x] Map EDGAR data to Compustat COMPANY table
- [x] Map EDGAR data to Compustat SECURITY table
- [x] Map EDGAR data to Compustat SEC_IDCURRENT table
- [x] Enhanced financial data extraction (revenue, assets, liabilities, equity, net income, EPS, etc.)
- [x] Security identifier extraction (ticker symbols)
- [ ] Handle data type conversions comprehensively
- [ ] Apply business rules and validations

### Phase 5: Database Population ✅ (IN PROGRESS)
- [x] Populate COMPANY table (30+ companies)
- [x] Populate SECURITY table (29+ securities)
- [x] Populate SEC_IDCURRENT table (95+ identifiers)
- [ ] Populate SEC_DPRC (daily prices)
- [ ] Populate FUNDA, FUNDQ, FUNDY (fundamentals)
- [ ] Create indexes for performance
- [ ] Validate data integrity

### Phase 6: Validation & Testing (PENDING)
- [ ] Compare with source Compustat database
- [ ] Validate data completeness and accuracy
- [ ] Performance testing

## Usage Examples

### Initialize Database Schema

```bash
python initialize_database.py
```

Or programmatically:

```python
from src.database_builder import DatabaseBuilder

builder = DatabaseBuilder()
builder.initialize_schema(['COMPANY', 'SECURITY', 'SEC_IDCURRENT'])
builder.close()
```

### Download SEC Filings

**Download all filings (full run):**
```bash
python download_filings.py
```

**Test with limited filings:**
```bash
python download_filings.py --limit 100
```

**Download specific year/quarter:**
```bash
python download_filings.py --year 2024 --quarter 1
```

**Dry-run to see what would be downloaded:**
```bash
python download_filings.py --dry-run
```

**Programmatic usage:**
```python
from src.edgar_downloader import EdgarDownloader

downloader = EdgarDownloader()
# Download all filings for companies in mapping
download_counts = downloader.download_filings_for_companies(limit=100)
```

### Parse Downloaded Filings

**Parse all filings in data/raw:**
```bash
python parse_filings.py
```

**Parse specific filing types:**
```bash
python parse_filings.py --filing-types 10-K 10-Q
```

**Parse with limit (for testing):**
```bash
python parse_filings.py --limit 10
```

**Parse specific directory:**
```bash
python parse_filings.py --directory data/raw/2024/Q1
```

**Programmatic usage:**
```python
from src.data_extractor import DataExtractor
from pathlib import Path

extractor = DataExtractor()
data = extractor.extract_from_directory(Path("data/raw"))
extractor.populate_company_table(data)
extractor.close()
```

## SEC EDGAR Requirements

- **User-Agent:** Must identify your organization (configured in `config.py`)
- **Rate Limiting:** Respectful delays between requests (100ms default)
- **Terms of Use:** Comply with SEC EDGAR terms of use

## Current Status

**Pipeline Status:** ✅ **OPERATIONAL**

- **Companies in Database:** 53+
- **Securities in Database:** 49+
- **Identifiers in Database:** 178+
- **Filings Downloaded:** 200+
- **Tables Populated:** COMPANY, SECURITY, SEC_IDCURRENT

See [PIPELINE_STATUS.md](PIPELINE_STATUS.md) and [COMPLETION_STATUS.md](COMPLETION_STATUS.md) for detailed status.

## Notes

- CIK values in Compustat include leading zeros (e.g., '0001434614')
- SEC EDGAR CIKs may or may not have leading zeros - handle both formats
- XBRL filings are preferred (structured data) but HTML/text fallbacks needed
- Handle amendments (10-K/A, 10-Q/A) and restatements correctly
- Pipeline successfully extracts ticker symbols from XBRL-embedded HTML
- Database population uses INSERT/UPDATE pattern for data integrity

## Contributing

See [PRD.md](PRD.md) for detailed requirements and architecture.

## License

[Add license information]

## References

- [SEC EDGAR](https://www.sec.gov/edgar.shtml)
- [SEC EDGAR API Documentation](https://www.sec.gov/edgar/sec-api-documentation)
- [XBRL Standards](https://www.xbrl.org/)
