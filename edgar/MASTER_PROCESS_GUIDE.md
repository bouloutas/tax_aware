# Master Process Guide: Replicating Compustat from SEC Filings

**Purpose:** Complete guide for replicating Compustat database from SEC EDGAR filings  
**Status:** 📋 Reference Guide  
**Last Updated:** November 18, 2025

## Overview

This guide provides a complete, step-by-step process for replicating the Compustat database (`compustat.duckdb`) by extracting data from SEC EDGAR filings and populating `compustat_edgar.duckdb`.

## Quick Start

**For New Companies:**
1. Read this guide
2. Follow task documentation files in order
3. Execute scripts as documented
4. Validate results

**Task Documentation Files:**
- `TASK_DOWNLOAD_FILINGS.md` - Download SEC filings
- `TASK_PARSE_FILINGS.md` - Parse filings and extract data
- `TASK_MAP_TO_COMPUSTAT.md` - Map to Compustat schema
- `TASK_EXTRACT_FINANCIAL_ITEMS.md` - Extract financial items
- `TASK_VALIDATE_DATA.md` - Validate against source
- `TASK_SCALE_TO_ALL_COMPANIES.md` - Scale to full universe

## Complete Process Flow

### Phase 1: Setup

1. **Prerequisites:**
   - Python 3.10+
   - Required packages (see `requirements.txt`)
   - CIK-to-GVKEY mapping file (`cik_to_gvkey_mapping.csv`)
   - Source Compustat database (`compustat.duckdb`)

2. **Configuration:**
   - Update `config.py` with settings
   - Set date ranges, filing types, etc.
   - Configure database paths

### Phase 2: Download Filings

**Task:** `TASK_DOWNLOAD_FILINGS.md`

**Steps:**
1. Load companies from CIK mapping file
2. For each company:
   - Query EDGAR API for filings
   - Download 10-K, 10-Q, 8-K filings
   - Save to `data/raw/{YEAR}/Q{QUARTER}/{CIK}/`
   - Log download metadata

**Scripts:**
- `download_filings.py` - General downloader
- `download_targets.py` - Targeted downloader (MSFT/NVDA)

**Output:**
- Raw filings in `data/raw/`
- Manifest files in `data/manifests/`

### Phase 3: Parse Filings

**Task:** `TASK_PARSE_FILINGS.md`

**Steps:**
1. Detect filing format (XBRL, HTML, Text)
2. Parse using appropriate parser
3. Extract:
   - Company metadata (address, fiscal year, etc.)
   - Security identifiers (ticker, CUSIP, etc.)
   - Financial data (income statement, balance sheet, etc.)

**Scripts:**
- `parse_filings.py` - Parse downloaded filings
- `process_pilot.py` - Process pilot companies

**Output:**
- Parsed data dictionaries
- Logs of extraction results

### Phase 4: Map to Compustat Schema

**Task:** `TASK_MAP_TO_COMPUSTAT.md`

**Steps:**
1. Map company data to COMPANY table
2. Map security data to SECURITY table
3. Map identifiers to SEC_IDCURRENT table
4. Map financial data to CSCO_IKEY and CSCO_IFNDQ tables

**Scripts:**
- `src/data_extractor.py` - Data extraction and mapping
- `src/financial_mapper.py` - Financial data mapping

**Output:**
- Populated database tables
- Mapping logs

### Phase 5: Extract Financial Items

**Task:** `TASK_EXTRACT_FINANCIAL_ITEMS.md`

**Steps:**
1. Extract all us-gaap tags from XBRL
2. Map to Compustat item codes
3. Handle calculated items (EBITDA, etc.)
4. Store in CSCO_IFNDQ table

**Current Status:**
- 25 items extracted
- Target: 50+ items

**Output:**
- Financial items in CSCO_IFNDQ table
- Item extraction reports

### Phase 6: Validate Data

**Task:** `TASK_VALIDATE_DATA.md`

**Steps:**
1. Compare record counts
2. Compare field population
3. Compare values (with tolerance)
4. Generate validation report

**Scripts:**
- `validate_pilot.py` - Validate COMPANY, SECURITY, SEC_IDCURRENT
- `validate_financial.py` - Validate CSCO_IKEY, CSCO_IFNDQ
- `compare_tables_detailed.py` - Detailed comparison

**Output:**
- Validation reports
- Discrepancy logs

### Phase 7: Scale to All Companies

**Task:** `TASK_SCALE_TO_ALL_COMPANIES.md`

**Steps:**
1. Process all 37,071 companies in batches
2. Download all filings (5 years)
3. Parse all filings
4. Map all data
5. Validate sample

**Estimated Time:** 4-7 days

**Output:**
- Complete `compustat_edgar.duckdb` database
- Processing logs
- Validation reports

## Key Files and Scripts

### Configuration
- `config.py` - Configuration settings
- `cik_to_gvkey_mapping.csv` - CIK to GVKEY mapping

### Source Code
- `src/edgar_downloader.py` - Download SEC filings
- `src/filing_parser.py` - Parse filings (XBRL, HTML, Text)
- `src/data_extractor.py` - Extract and map data
- `src/financial_mapper.py` - Map financial data
- `src/schema_mapper.py` - Schema utilities

### Scripts
- `download_filings.py` - Download filings
- `download_targets.py` - Download for specific companies
- `parse_filings.py` - Parse filings
- `process_pilot.py` - Process pilot companies
- `validate_pilot.py` - Validate pilot data
- `validate_financial.py` - Validate financial data
- `compare_tables_detailed.py` - Detailed comparison

### Documentation
- `TASK_*.md` - Task-specific guides
- `FIELD_MAPPING_GUIDE.md` - Field-by-field mapping
- `MAPPING_RESEARCH.md` - Research findings
- `STEP1_COMPLETE_PLAN.md` - Implementation plan
- `PHASE1_PROGRESS.md` - Progress tracking

## Database Schema

### Key Tables

**COMPANY** (39 fields)
- GVKEY, CIK, CONM, CONML
- ADD1, ADD2, CITY, STATE, ADDZIP
- FYRC, SIC, PHONE, WEBURL, EIN
- GGROUP, GIND, GSECTOR, GSUBIND
- And 20+ more fields

**SECURITY** (15 fields)
- GVKEY, IID, TIC, TPCI
- CUSIP, ISIN, SEDOL
- EXCHG, EXCNTRY
- And 5+ more fields

**CSCO_IKEY** (16 fields)
- GVKEY, DATADATE, COIFND_ID
- CQTR, CYEARQ, CURCDQ, FDATEQ, RDQ
- FQTR, FYEARQ, PDATEQ
- INDFMT, CONSOL, POPSRC, FYR, DATAFMT

**CSCO_IFNDQ** (7 fields)
- COIFND_ID, EFFDATE, ITEM, VALUEI
- DATACODE, RST_TYPE, THRUDATE

**Total:** 211 tables in Compustat

## Mapping Reference

### XBRL Tags → Compustat Items

See `FIELD_MAPPING_GUIDE.md` for comprehensive mapping.

**Common Mappings:**
- `us-gaap:Revenues` → REVTQ
- `us-gaap:Assets` → ATQ
- `us-gaap:NetIncomeLoss` → NIQ
- `us-gaap:OperatingIncomeLoss` → OIADPQ
- `dei:EntityAddressAddressLine1` → COMPANY.ADD1
- `dei:DocumentPeriodEndDate` → COMPANY.FYRC (month)

### Field Extraction Methods

**Company Metadata:**
- Address: Extract from `dei:EntityAddress*` tags
- Fiscal Year: Parse month from `dei:DocumentPeriodEndDate`
- Legal Name: Extract from `dei:EntityRegistrantName`

**Financial Data:**
- Extract from `us-gaap:*` tags
- Parse context for period information
- Parse unitRef for currency and scale

## Common Issues and Solutions

### Issue 1: Missing Address Fields
**Solution:** Check HTML-embedded XBRL patterns, improve text cleaning

### Issue 2: Missing Financial Items
**Solution:** Expand tag extraction, handle tag variations

### Issue 3: Date Parsing Errors
**Solution:** Try multiple date formats, clean HTML entities first

### Issue 4: Database Errors
**Solution:** Use upsert logic (UPDATE or INSERT), handle NULLs

### Issue 5: Rate Limiting
**Solution:** Increase REQUEST_DELAY_SECONDS, use proper User-Agent

## Validation Checklist

Before scaling to all companies:
- [ ] Download process works for sample
- [ ] Parsing process works for sample
- [ ] Mapping process works for sample
- [ ] Validation shows >95% accuracy
- [ ] Error handling robust
- [ ] Logging comprehensive
- [ ] Documentation complete

## Success Metrics

**For Pilot (MSFT/NVDA):**
- ✅ COMPANY: 5-6/39 fields populated
- ✅ CSCO_IKEY: 11/16 fields populated
- ✅ CSCO_IFNDQ: 7/7 fields populated
- ✅ Financial items: 25 extracted

**For Full Universe:**
- Target: 100% field population
- Target: 50+ financial items per company
- Target: 5 years of historical data
- Target: >95% accuracy vs source Compustat

## Next Steps

1. Expand financial item extraction to 50+ items
2. Complete remaining COMPANY fields
3. Complete SECURITY table fields
4. Process 5 years of historical data
5. Scale to all 37,071 companies
6. Validate and fix discrepancies

## Notes

- Always refer to task documentation files for detailed steps
- Keep logs of all operations
- Validate frequently during development
- Document any deviations from standard process
- Update documentation as process evolves

