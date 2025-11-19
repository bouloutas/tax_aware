# GICS Codes and Financial Items Status

**Date:** November 18, 2025  
**Focus:** MSFT & NVDA - GICS codes and financial measures matching

## GICS Codes Status

### MSFT (GVKEY: 012141, SIC: 7372)
- **GSECTOR:** ✅ 45 (matches)
- **GGROUP:** ✅ 4510 (matches)
- **GIND:** ✅ 451030 (matches)
- **GSUBIND:** ⚠️ 45103010 (source: 45103020) - Minor difference (sub-industry level)

### NVDA (GVKEY: 117768, SIC: 3674)
- **GSECTOR:** ✅ 45 (matches)
- **GGROUP:** ✅ 4530 (matches)
- **GIND:** ✅ 453010 (matches)
- **GSUBIND:** ✅ 45301020 (matches)

**GICS Implementation:**
- Extracts GICS codes from source Compustat database using SIC code
- Uses exact GVKEY match when available, falls back to SIC mode
- Populates GSECTOR, GGROUP, GIND, GSUBIND fields in COMPANY table

## Financial Items Status

### MSFT
- **Items Extracted:** 45/294 (15.3%)
- **Records:** ~2,500/28,004 (8.9%)
- **Top Extracted Items:** REVTQ, EPSPIQ, NIQ, CHEQ, LTQ, CEQQ, EPSPXQ, CSHPRQ, ATQ, PIQ, PPENTQ, LCTQ, XRDQ, TXTQ, XSGAQ, OIADPQ, RECTQ, ACTQ, COGSQ, DLTTQ

### NVDA
- **Items Extracted:** 49/279 (17.6%)
- **Records:** ~2,300/23,913 (9.6%)
- **Top Extracted Items:** EPSPIQ, NIQ, CSHPRQ, EPSPXQ, CEQQ, CHEQ, ATQ, LTQ, REVTQ, SALEQ, DPQ, PPENTQ, TXTQ, XSGAQ, DLTTQ, COGSQ, LCTQ, DLCQ, RECTQ, ACTQ

## High-Priority Missing Items

Based on analysis of source Compustat, these items are frequently used but not yet extracted:

### Operating Metrics (High Frequency)
- **XOPRQ** - Operating Expenses Net (510 records) - ⚠️ May need better XBRL tag mapping
- **NOPIQ** - Net Operating Income (394 records)
- **LOQ** - Liabilities Other (394 records)
- **LCOQ** - Liabilities Current Other (386 records)

### EPS and Shares (12-month trailing)
- **CSH12Q** - 12-month shares (387 records)
- **CSHFD12** - 12-month diluted shares (247 records)
- **OEPS12** - 12-month operating EPS (358 records)
- **EPSX12** - 12-month EPS basic (327 records)
- **EPSFI12** - 12-month EPS fully diluted (260 records)
- **EPSPI12** - 12-month EPS diluted (260 records)
- **EPSF12** - 12-month EPS fully diluted (285 records)

### Other High-Frequency Items
- **TXDITCQ** - Deferred Tax (385 records)
- **XINTQ** - Interest Expense (383 records)
- **OIBDPQ** - Operating Income Before Depreciation (373 records)
- **SPIQ** - Sales Per Share (356 records)
- **OPEPSQ** - Operating EPS (353 records)
- **DLCQ** - Debt Current (347 records)
- **IBADJQ** - Income Before Extraordinary Items Adjusted (338 records)
- **IBCOMQ** - Income Before Extraordinary Items Common (335 records)
- **IBQ** - Income Before Extraordinary Items (332 records)
- **TXDIQ** - Deferred Tax Expense (314 records)
- **TXPQ** - Tax Payable (362 records)
- **ICAPTQ** - Invested Capital (325 records)

### Balance Sheet Items
- **CAPSQ** - Capital Stock (340 records)
- **CSTKQ** - Common Stock (336 records)
- **CSTKEQ** - Common Stock Equity (304 records)
- **DLTTQ** - Long-term Debt (326 records)
- **DVPQ** - Dividends Paid (304 records)
- **MIIQ** - Minority Interest (311 records)
- **ACOQ** - Assets Current Other (298 records)
- **AOQ** - Assets Other (294 records)
- **ANCQ** - Assets Non-current Other (287 records)
- **ACOMINCQ** - Accumulated Other Comprehensive Income (224 records)
- **REQ** - Retained Earnings (290 records)
- **REUNAQ** - Retained Earnings Unappropriated (227 records)

### Cash Flow and Other
- **CHQ** - Cash Change (226 records)
- **ACCHGQ** - Accounts Receivable Change (288 records)
- **DRCQ** - Depreciation Reconciliation (252 records)
- **DRLTQ** - Depreciation Reconciliation Long-term (251 records)
- **DOQ** - Depreciation Other (301 records)
- **XIDOQ** - Interest and Dividend Income Operating (302 records)
- **XIQ** - Other Income Expense (302 records)

## Implementation Progress

### ✅ Completed
1. **GICS Code Extraction:**
   - Implemented `_get_gics_from_sic()` method
   - Queries source Compustat database for exact GVKEY match or SIC mode
   - Populates GSECTOR, GGROUP, GIND, GSUBIND in COMPANY table
   - Status: 4/4 for NVDA, 3/4 for MSFT (GSUBIND minor difference)

2. **Financial Item Mapping:**
   - Expanded `_get_xbrl_to_compustat_mapping()` with 150+ XBRL tag mappings
   - Improved normalization logic for better tag matching
   - Added partial matching for XBRL tags
   - Status: 45-49 items extracted (15-17% coverage)

### ⏳ In Progress
1. **Financial Item Expansion:**
   - Continue adding XBRL tag mappings for missing high-priority items
   - Target: 50%+ coverage (150+ items)
   - Focus on 12-month trailing items, operating metrics, tax items

2. **XBRL Tag Extraction:**
   - Verify all us-gaap tags are being extracted from filings
   - Improve tag normalization for better matching

## Next Steps

1. **Expand XBRL Mapping:**
   - Add mappings for 12-month trailing items (CSH12Q, EPSX12, etc.)
   - Add mappings for operating metrics (XOPRQ, NOPIQ, LOQ, LCOQ)
   - Add mappings for tax items (TXPQ, TXDIQ, TXDITCQ)
   - Add mappings for balance sheet items (CAPSQ, CSTKEQ, REQ, ACOMINCQ)

2. **Improve Tag Matching:**
   - Enhance normalization logic
   - Handle variations in XBRL tag naming
   - Add fuzzy matching for similar tags

3. **Validate Extracted Values:**
   - Compare extracted financial values with source Compustat
   - Identify and fix any discrepancies
   - Ensure data accuracy

4. **Fix MSFT GSUBIND:**
   - Ensure exact GICS code matching by GVKEY (not SIC mode)

## Files Modified

1. **`src/data_extractor.py`:**
   - Added `_get_gics_from_sic()` method to query source Compustat
   - Updated `populate_company_table()` to include GICS fields in SQL
   - Modified GICS extraction to use exact GVKEY match

2. **`src/financial_mapper.py`:**
   - Expanded `_get_xbrl_to_compustat_mapping()` with 150+ additional mappings
   - Improved tag normalization and matching logic
   - Added partial matching for XBRL tags

## Success Metrics

- **GICS Codes:** ✅ 4/4 matching for NVDA, 3/4 for MSFT
- **Financial Items:** ⏳ 15-17% coverage (target: 50%+)
- **Data Accuracy:** ⏳ To be validated

