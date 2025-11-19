# PRD Step 1 – MSFT & NVDA Pilot

## 1. Objectives

1. Stand up a **clean, deterministic download loop** for two large-cap issuers (Microsoft – `MSFT`, NVIDIA – `NVDA`).
2. **Map every downloaded field** required for Compustat replication into `compustat_edgar.duckdb`.
3. **Validate** each mapped field against the canonical `compustat.duckdb` for the same GVKEYs and date ranges.
4. Iterate until the per-field mapping is lossless, then use this blueprint to scale to the full universe.

## 2. Scope

| Item | In Scope | Notes |
|------|----------|-------|
| Filings | 10-K / 10-Q / 8-K (FY 2023–FY 2024) | Expand later once mapping is proven |
| Issuers | Microsoft (`MSFT`, GVKEY TBD), NVIDIA (`NVDA`, GVKEY TBD) | Use actual GVKEY/CIK from mapping file |
| Tables | `COMPANY`, `SECURITY`, `SEC_IDCURRENT`, key fundamentals (revenue, net income, EPS, assets, liabilities, equity, shares outstanding) | Price tables (SEC_DPRC) out of scope for Step 1 |
| Validation | Direct comparison against `compustat.duckdb` | Focus on same dates and GVKEYs |

## 3. Deliverables

1. **Downloader (pilot mode)** – pulls only MSFT & NVDA filings, with reproducible logs & manifests.
2. **Mapping workbook** – field-by-field mapping between EDGAR source (XBRL/HTML) and Compustat target columns.
3. **Pilot database slices** – `compustat_edgar.duckdb` populated for MSFT/NVDA with validated values.
4. **Validation report** – proof that each populated column matches `compustat.duckdb` for the pilot names.

## 4. Work Breakdown

### Step 1 – Controlled Download
1. Extract MSFT/NVDA GVKEY & CIK from `cik_to_gvkey_mapping.csv`.
2. Author a **targeted downloader** that:
   - Accepts explicit CIK/GVKEY list
   - Fetches filings only for FY 2023–FY 2024
   - Stores metadata manifest (CIK, accession, form, date, source URL, file path, checksum)
3. Dry-run to ensure zero 404s/ambiguous URLs; capture logs.

### Step 2 – Mapping & Population
1. Parse downloaded filings (XBRL preferred, HTML fallback).
2. For each target Compustat column:
   - Document source XBRL tag / HTML selector / transformation
   - Normalize units (millions vs actuals, scaling, currency)
3. Populate `compustat_edgar.duckdb` tables **only for MSFT & NVDA**.
4. Record data lineage inside mapping workbook.

### Step 3 – Validation Loop
1. Query `compustat.duckdb` for the same GVKEYs/date range.
2. Run diffs per column (absolute & percentage difference).
3. Fix extraction/mapping issues, re-run Step 2 until diffs = 0 (or explain discrepancies).
4. Once parity achieved, freeze mapping spec for scale-up.

## 5. Success Criteria

| Area | Metric |
|------|--------|
| Downloader | 100% success rate for MSFT/NVDA filings in FY 2023–2024 |
| Mapping | Every target column documented with source + transform |
| Database | `compustat_edgar.duckdb` contains accurate rows for the two issuers |
| Validation | Zero unexplained differences vs `compustat.duckdb` |

## 6. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| XBRL inconsistencies | Maintain HTML/text fallback, log extraction provenance |
| Unit/magnitude differences | Standardize units (USD, actuals) & track scaling factors |
| Time drift (filing date vs effective date) | Align on Compustat `DATADATE` logic; store raw filing dates |
| Scope creep | Pilot strictly limited to MSFT/NVDA until mapping parity achieved |

## 7. Next Action Items

1. Query mapping file for MSFT/NVDA GVKEY & CIK.
2. Build focused downloader & metadata manifest.
3. Begin Step 1 execution per this plan.

---

## 8. Extended Goal: Populate ALL Tables and Fields

**Current Status:** 31.7% of fields populated across key tables (26/82 fields)

**Objective:** Populate 100% of fields across ALL 211 tables for MSFT and NVDA

### 8.1 Database Structure Analysis

**Total Tables:** 211 tables in compustat.duckdb

**Tables with MSFT/NVDA Data:** 90 tables contain data for these securities

**Table Categories:**
- **Core Tables (3):** COMPANY, SECURITY, SEC_IDCURRENT
- **CSCO_ Tables (14):** CSCO_IKEY, CSCO_IFNDQ, CSCO_ITXT, CSCO_AFND, CSCO_AKEY, etc.
- **SEC_ Tables (25):** SEC_DPRC, SEC_DIVID, SEC_MTH*, SEC_AFND, etc.
- **ACO_ Tables (22):** ACO_AMDA, ACO_NOTESA/Q, ACO_TRANSA/Q, ACO_PNFNDA/Q, etc.
- **SEG_ Tables (7):** SEG_ANNFUND, SEG_GEO, SEG_PRODUCT, etc.
- **CO_ Tables (30+):** CO_AFND1/2, CO_IFNDQ, CO_IMKT, CO_INDUSTRY, etc.
- **BANK_ Tables (13):** Bank-specific tables
- **Other Tables (100+):** Various specialized tables

### 8.2 Comprehensive Mapping Plan

See **[STEP1_COMPLETE_PLAN.md](STEP1_COMPLETE_PLAN.md)** for detailed plan covering:

#### Phase 1: Complete Core Tables
- **COMPANY:** Complete 36 missing fields (address, business description, industry classification, etc.)
- **SECURITY:** Complete 11 missing fields (CUSIP, ISIN, SEDOL, exchange info, etc.)
- **SEC_IDCURRENT:** Complete 1 missing field (PACVERTOFEEDPOP)
- **CSCO_IKEY:** Complete 5 missing fields (CQTR, CURCDQ, CYEARQ, FDATEQ, RDQ)
- **CSCO_IFNDQ:** Complete 3 missing fields (DATACODE, RST_TYPE, THRUDATE) + extract 50+ additional financial items

#### Phase 2: Enhanced Data Tables
- **CSCO_ITXT:** Extract text fields from filings
- **SEC_DPRC:** Extract daily price data
- **SEC_DIVID:** Extract dividend information
- **Historical Data:** Download and process 5 years of filings

#### Phase 3: All Remaining Tables
- **CSCO_* Tables:** CSCO_INDFNTA/Q/YTD, CSCO_INDSTA/Q/YTD, CSCO_PNFNTA/Q/YTD, CSCO_TRANSA/Q, CSCO_NOTESA/Q/SA/YTD
- **ACO_* Tables:** ACO_AMDA, ACO_NOTESA/Q, ACO_TRANSA/Q, ACO_PNFNDA/Q, etc.
- **SEG_* Tables:** SEG_ANNFUND, SEG_GEO, SEG_PRODUCT, etc.
- **CO_* Tables:** CO_AFND1/2, CO_IFNDQ, CO_IMKT, etc.
- **SEC_* Tables:** SEC_MTH*, SEC_AFND, SEC_IFND, etc.

### 8.3 Implementation Strategy

1. **Enhance Parsers:**
   - Extract all us-gaap tags comprehensively
   - Extract company metadata (address, business description)
   - Extract security identifiers (CUSIP, ISIN, SEDOL)
   - Extract text fields and notes

2. **Create Field Mappers:**
   - COMPANY field mapper (36 fields)
   - SECURITY field mapper (11 fields)
   - CSCO_IKEY field mapper (5 fields)
   - CSCO_IFNDQ field mapper (3 fields + 50+ items)
   - CSCO_ITXT field mapper
   - SEC_DPRC field mapper
   - SEC_DIVID field mapper
   - And 80+ more table mappers

3. **Create Table Builders:**
   - Builders for all 211 tables
   - Handle table-specific logic
   - Link records via GVKEY, COIFND_ID, etc.

4. **Historical Data:**
   - Download 5 years of filings (2020-2025)
   - Process all 10-K, 10-Q, 8-K filings
   - Extract point-in-time data

5. **Validation:**
   - Compare each table field-by-field
   - Validate data accuracy
   - Fix discrepancies

### 8.4 Priority Order

**High Priority (Core Functionality):**
1. Complete COMPANY table (36 fields)
2. Complete SECURITY table (11 fields)
3. Complete CSCO_IKEY table (5 fields)
4. Complete CSCO_IFNDQ table (3 fields + 50+ items)
5. Extract comprehensive financial items

**Medium Priority (Enhanced Data):**
6. CSCO_ITXT table
7. SEC_DPRC table
8. SEC_DIVID table
9. Historical data (5 years)

**Low Priority (Advanced Features):**
10. All remaining CSCO_* tables
11. All ACO_* tables
12. All SEG_* tables
13. All CO_* tables
14. All SEC_* tables
15. All other tables

### 8.5 Success Criteria

**Phase 1 Complete When:**
- ✅ COMPANY: 39/39 fields populated
- ✅ SECURITY: 15/15 fields populated
- ✅ SEC_IDCURRENT: 5/5 fields populated
- ✅ CSCO_IKEY: 16/16 fields populated
- ✅ CSCO_IFNDQ: 7/7 fields populated + 50+ financial items

**Phase 2 Complete When:**
- ✅ CSCO_ITXT table populated
- ✅ SEC_DPRC table populated
- ✅ SEC_DIVID table populated
- ✅ 5 years of historical data

**Phase 3 Complete When:**
- ✅ All 211 tables populated
- ✅ 100% field population for MSFT & NVDA
- ✅ Data matches compustat.duckdb

### 8.6 Estimated Effort

- **Phase 1:** 2-3 days (core tables completion)
- **Phase 2:** 3-5 days (enhanced data extraction)
- **Phase 3:** 5-10 days (all tables)
- **Total:** 10-18 days of focused development

### 8.7 Current Field Population Status

| Table | Populated | Total | Percentage |
|-------|-----------|-------|------------|
| COMPANY | 3 | 39 | 7.7% |
| SECURITY | 4 | 15 | 26.7% |
| SEC_IDCURRENT | 4 | 5 | 80.0% |
| CSCO_IKEY | 11 | 16 | 68.8% |
| CSCO_IFNDQ | 4 | 7 | 57.1% |
| **TOTAL** | **26** | **82** | **31.7%** |

**Remaining:** 56 fields to populate across these 5 tables, plus 200+ additional tables


