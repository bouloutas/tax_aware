# Product Requirements Document (PRD)
## EDGAR to Compustat Database Replication

**Version:** 1.0  
**Date:** November 17, 2025  
**Status:** In Progress

---

## Executive Summary

This project aims to replicate the Compustat database (`compustat.duckdb`) by extracting data from SEC EDGAR filings and populating a new database (`compustat_edgar.duckdb`). The goal is to create a 5-year historical dataset (back to ~10/1/2020) that mirrors the structure and content of the Compustat database, ending on 9/30/2025.

**Primary Objective:** Build a complete Compustat-equivalent database using publicly available SEC filings, enabling independent access to financial data without relying on proprietary Compustat subscriptions.

---

## 1. Background & Motivation

### 1.1 Problem Statement
- Compustat is a proprietary database requiring expensive subscriptions
- SEC EDGAR filings contain the same underlying data but in unstructured formats
- Need for an open-source alternative that replicates Compustat's structure and coverage

### 1.2 Use Case
- Tax-aware portfolio management system requires Compustat data
- Historical analysis needs 5+ years of data
- Data must match Compustat schema for compatibility with existing systems

---

## 2. Scope & Requirements

### 2.1 Data Coverage

**Time Range:**
- Start Date: October 1, 2020 (5 years back from end date)
- End Date: September 30, 2025
- All filings within this period must be captured

**Company Coverage:**
- All companies with CIK (Central Index Key) identifiers
- Current mapping: 37,071 companies with CIK values
- Must handle companies that change CIKs or merge/acquire

**Filing Types:**
- **10-K**: Annual reports (primary source for annual fundamentals)
- **10-Q**: Quarterly reports (primary source for quarterly fundamentals)
- **8-K**: Current reports (material events)
- **DEF 14A**: Proxy statements (governance data)
- **10-K/A, 10-Q/A**: Amended filings (must track amendments)

### 2.2 Database Schema Requirements

**Core Tables to Replicate:**

1. **COMPANY** (56,321 rows in source)
   - Company master data
   - Key fields: GVKEY, CIK, CONM (company name), LOC, INCORP, SIC, GSECTOR, GIND
   - Must maintain CIK → GVKEY mapping

2. **SECURITY** (74,779 rows in source)
   - Security/issue level data
   - Key fields: GVKEY, IID, CUSIP, TIC, EXCHG, TPCI
   - Links companies to their securities

3. **SEC_IDCURRENT** (74,699 rows in source)
   - Current identifiers for securities
   - Key fields: GVKEY, IID, ITEM, ITEMVALUE
   - Maps various identifiers (TIC, CUSIP, etc.)

4. **SEC_DPRC** (159M+ rows in source)
   - Daily price data
   - Key fields: GVKEY, IID, DATADATE, PRCCD (close), PRCOD (open), PRCHD (high), PRCLD (low), CSHTRD (volume)
   - Critical for time-series analysis

5. **FUNDA** (Annual Fundamentals)
   - Annual financial statement data
   - Revenue, assets, liabilities, equity, etc.

6. **FUNDQ** (Quarterly Fundamentals)
   - Quarterly financial statement data
   - Same structure as FUNDA but quarterly frequency

7. **FUNDY** (Year-to-Date Fundamentals)
   - Year-to-date financial data

**Additional Tables:**
- All other Compustat tables should be replicated as needed
- Total: 211 tables in source database

### 2.3 Data Quality Requirements

**Completeness:**
- Must capture all filings for companies in scope
- Handle missing or delayed filings gracefully
- Track data gaps and report them

**Accuracy:**
- Extracted values must match source filings
- Handle restatements and amendments correctly
- Maintain point-in-time accuracy (PIT)

**Consistency:**
- Schema must exactly match Compustat structure
- Data types must be compatible
- Foreign key relationships must be maintained

---

## 3. Technical Architecture

### 3.1 System Components

```
┌─────────────────────────────────────────────────────────┐
│              SEC EDGAR Data Sources                      │
│  • Full-index files  • Company-specific endpoints       │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│            EDGAR Downloader Module                       │
│  • Rate-limited requests  • Filing retrieval            │
│  • Index parsing  • File management                     │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│            Filing Parser Module                          │
│  • XBRL parsing  • HTML parsing  • Text extraction      │
│  • Data validation  • Error handling                    │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│         Schema Mapper & Data Transformer                 │
│  • Map EDGAR data to Compustat schema                   │
│  • Data type conversion  • Field mapping                │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│         Database Builder Module                          │
│  • Schema creation  • Data insertion                    │
│  • Indexing  • Validation                               │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│         compustat_edgar.duckdb                           │
│  • Replicated Compustat structure                       │
│  • 5 years of historical data                           │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Data Flow

1. **Mapping Phase**
   - Load CIK → GVKEY mapping from Compustat
   - Validate mapping completeness

2. **Download Phase**
   - Download SEC full-index files for date range
   - Identify all filings for companies in scope
   - Download filing documents (XBRL, HTML, text)

3. **Parsing Phase**
   - Parse XBRL filings (preferred, structured)
   - Fallback to HTML/text parsing
   - Extract financial statement data
   - Extract company metadata

4. **Transformation Phase**
   - Map extracted data to Compustat schema
   - Convert data types
   - Handle missing values
   - Apply business rules

5. **Loading Phase**
   - Insert data into target database
   - Create indexes
   - Validate data integrity
   - Generate reports

### 3.3 Technology Stack

**Languages & Frameworks:**
- Python 3.10+
- DuckDB for database
- Requests for HTTP
- BeautifulSoup/lxml for HTML parsing
- XBRL libraries for structured data

**Data Storage:**
- DuckDB database files
- CSV for intermediate data
- Raw filings stored in `data/raw/`
- Processed data in `data/processed/`

---

## 4. Implementation Phases

### Phase 1: Foundation ✅ (COMPLETED)
- [x] Create CIK-to-GVKEY mapping
- [x] Analyze Compustat schema
- [x] Set up project structure
- [x] Create configuration system
- [x] Build schema mapper infrastructure
- [x] Create database initialization scripts

### Phase 2: EDGAR Download Infrastructure (IN PROGRESS)
- [ ] Implement SEC full-index file download
- [ ] Build company-specific filing index retrieval
- [ ] Create filing downloader with rate limiting
- [ ] Implement retry logic and error handling
- [ ] Add progress tracking and logging

### Phase 3: Filing Parsers
- [ ] XBRL parser for structured data
- [ ] HTML parser for unstructured filings
- [ ] Text parser as fallback
- [ ] Extract financial statement data
- [ ] Extract company metadata
- [ ] Handle amendments and restatements

### Phase 4: Data Transformation
- [ ] Map EDGAR data to Compustat COMPANY table
- [ ] Map to SECURITY table
- [ ] Map to SEC_IDCURRENT table
- [ ] Extract and map daily price data (SEC_DPRC)
- [ ] Extract and map fundamental data (FUNDA, FUNDQ, FUNDY)
- [ ] Handle data type conversions
- [ ] Apply business rules and validations

### Phase 5: Database Population
- [ ] Initialize target database schema
- [ ] Populate COMPANY table
- [ ] Populate SECURITY table
- [ ] Populate SEC_IDCURRENT table
- [ ] Populate SEC_DPRC (daily prices)
- [ ] Populate FUNDA, FUNDQ, FUNDY
- [ ] Create indexes for performance
- [ ] Validate data integrity

### Phase 6: Validation & Testing
- [ ] Compare compustat_edgar.duckdb with compustat.duckdb
- [ ] Validate data completeness
- [ ] Validate data accuracy
- [ ] Test date range coverage
- [ ] Performance testing
- [ ] Generate validation reports

### Phase 7: Documentation & Maintenance
- [ ] Complete API documentation
- [ ] Create user guide
- [ ] Document data quality issues
- [ ] Set up monitoring and alerts
- [ ] Create update/maintenance procedures

---

## 5. Key Challenges & Solutions

### 5.1 Challenge: SEC Rate Limiting
**Solution:** Implement rate limiting (100ms delay), use full-index files when possible, batch requests

### 5.2 Challenge: Filing Format Variations
**Solution:** Support multiple formats (XBRL, HTML, text), implement robust parsers with fallbacks

### 5.3 Challenge: Data Mapping Complexity
**Solution:** Create comprehensive mapping tables, validate mappings, handle edge cases

### 5.4 Challenge: Large Data Volume
**Solution:** Incremental processing, efficient storage, indexing strategy

### 5.5 Challenge: Data Quality & Completeness
**Solution:** Validation rules, error tracking, gap reporting, manual review process

---

## 6. Success Criteria

### 6.1 Functional Requirements
- ✅ CIK-to-GVKEY mapping established (37,071 companies)
- ⏳ All key tables replicated with correct schema
- ⏳ 5 years of historical data (10/1/2020 - 9/30/2025)
- ⏳ Data completeness > 95% for key tables
- ⏳ Data accuracy validated against source Compustat

### 6.2 Performance Requirements
- Download and process filings efficiently
- Database queries performant (< 1s for typical queries)
- Incremental updates possible

### 6.3 Quality Requirements
- Schema matches Compustat exactly
- Data types compatible
- Foreign key relationships maintained
- Point-in-time accuracy preserved

---

## 7. Risks & Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| SEC API changes | High | Medium | Monitor SEC announcements, implement versioning |
| Parsing errors | High | High | Multiple parser fallbacks, validation rules |
| Data quality issues | Medium | Medium | Comprehensive validation, manual review process |
| Storage requirements | Medium | Low | Efficient storage, compression, archiving |
| Rate limiting issues | Medium | Medium | Respectful rate limiting, use bulk downloads |

---

## 8. Future Enhancements

- Real-time updates as new filings are published
- Support for additional filing types
- Enhanced data quality checks
- API for querying the database
- Integration with other data sources
- Machine learning for improved parsing accuracy

---

## 9. References

- SEC EDGAR: https://www.sec.gov/edgar.shtml
- SEC EDGAR API: https://www.sec.gov/edgar/sec-api-documentation
- Compustat Documentation: Internal reference
- XBRL Standards: https://www.xbrl.org/

---

## 10. Appendices

### Appendix A: Key Tables Schema
See `PROJECT_STATUS.md` for detailed schema information.

### Appendix B: CIK Mapping Statistics
- Total companies with CIK: 37,071
- Unique CIKs: 37,071
- Unique GVKEYs: 37,071
- Mapping file: `cik_to_gvkey_mapping.csv`

### Appendix C: Configuration
See `config.py` for all configuration settings.

