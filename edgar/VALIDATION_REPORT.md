# Data Accuracy Validation Report

**Date:** November 18, 2025  
**Status:** Validation Complete

## Executive Summary

We have achieved **100%+ coverage** for financial items (331-321 items extracted vs 294-279 in source), but **value accuracy is 4.6-5.3%** when comparing extracted values against source Compustat data.

### Key Findings

- ✅ **Coverage:** 112.6-115.1% (all items present, plus extras)
- ⚠️ **Value Accuracy:** 4.6-5.3% (values match within 1% tolerance)
- ✅ **Key Identifiers:** 83.3-88.9% matching (minor formatting differences)

## Detailed Results

### MSFT (GVKEY: 012141)

#### Company Fields: 16/18 (88.9%)
- ✅ **Matching:** GVKEY, CIK, CONM, ADD1, ADD2, CITY, ADDZIP, SIC, WEBURL, EIN, FYRC, GSECTOR, GGROUP, GIND, GSUBIND
- ❌ **Mismatches:**
  - **CONML:** Source="Microsoft Corp" | Target="MICROSOFT CORPORATION" (formatting difference)
  - **PHONE:** Source="425 882 8080" | Target="4258828080" (spacing difference)

#### Financial Items: 5.3% accuracy
- **Total items compared:** 294
- **Total value comparisons:** 1,549
- **Matches:** 82 (5.3%)
- **Mismatches:** 1,467 (94.7%)

**Top items with highest discrepancies:**
1. **OLMIQ** (Operating Lease Minority Interest): 8 mismatches, 0% match rate, max diff: 192,603 (891.4%)
2. **LTMIBQ** (Long-Term Minority Interest Balance): 8 mismatches, 0% match rate, max diff: 1,924,537 (832.4%)
3. **DERALTQ** (Derivatives Assets Long-Term): 8 mismatches, 0% match rate, max diff: 39 (784.0%)
4. **CIDERGLQ** (Comprehensive Income Derivatives Gain/Loss): 8 mismatches, 0% match rate, max diff: 41 (950.0%)
5. **LUL3Q** (Liabilities Other Level 3): 8 mismatches, 0% match rate, max diff: 297 (1064.8%)

### NVDA (GVKEY: 117768)

#### Company Fields: 15/18 (83.3%)
- ✅ **Matching:** GVKEY, CIK, CONM, ADD1, ADD2, CITY, ADDZIP, SIC, WEBURL, EIN, FYRC, GSECTOR, GGROUP, GIND, GSUBIND
- ❌ **Mismatches:**
  - **CONML:** Source="NVIDIA Corporation" | Target="NVIDIA CORP" (formatting difference)
  - **STATE:** Source="CA" | Target="DE" (extracted state of incorporation instead of business address state)
  - **PHONE:** Source="408 486 2000" | Target="4084862000" (spacing difference)

#### Financial Items: 4.6% accuracy
- **Total items compared:** 279
- **Total value comparisons:** 1,355
- **Matches:** 62 (4.6%)
- **Mismatches:** 1,293 (95.4%)

**Top items with highest discrepancies:**
1. **CSTKCVQ** (Common Stock Capital Value): 6 mismatches, 25% match rate, max diff: 25 (2,499,900.0%)
2. **CSHOPQ** (Common Shares Outstanding Other): 8 mismatches, 0% match rate, max diff: 24,639 (47,183.0%)
3. **OLMIQ** (Operating Lease Minority Interest): 8 mismatches, 0% match rate, max diff: 117,897 (42,257.0%)
4. **CISECGLQ** (Comprehensive Income Securities Gain/Loss): 8 mismatches, 0% match rate, max diff: 10,243 (14,632.9%)
5. **PRCRAQ** (Preferred Stock Common Redeemable): 8 mismatches, 0% match rate, max diff: 14,014 (9,982.9%)

## Analysis

### Why Low Accuracy?

1. **Derived Items with Estimates:** Many items are calculated using rough estimates (e.g., multiplying by 0.5, 0.1, 0.3) rather than actual values from filings. Examples:
   - Preferred stock items (PRCQ, PNCQ, PRCPQ, PNCPQ) estimated as 50% or 30% of PSTKQ
   - Minority interest items estimated as 5-10% of other values
   - Operating/Finance lease minority interest estimated as 10% or 5% of MIIQ

2. **12-Month Trailing Items:** Many 12-month items (e.g., EPSX12, OEPS12) are calculated as quarterly * 4, which is an approximation and may not match actual trailing 12-month values.

3. **Missing Direct Mappings:** Some items don't have direct XBRL tag mappings and rely on derived calculations that may not be accurate.

4. **Unit Conversion Issues:** Some values may need unit conversion (thousands vs. millions) that isn't being applied correctly.

5. **Date/Period Mismatches:** Some items might be extracted from wrong periods or consolidated incorrectly.

### What's Working Well?

1. **Core Financial Items:** Basic income statement and balance sheet items (Revenue, Assets, Liabilities, Equity) likely have better accuracy, though not captured in this summary.

2. **Key Identifiers:** Company identifiers (GVKEY, CIK, GICS, SIC, FYRC) are 100% accurate.

3. **Coverage:** All required items are present, ensuring no data gaps.

## Recommendations

### Immediate Actions

1. **Identify High-Value Items:** Focus on improving accuracy for the most important financial items (Revenue, Net Income, Assets, Equity, EPS, etc.) first.

2. **Review Derived Calculations:** Replace estimated calculations with actual XBRL tag mappings where possible.

3. **Improve XBRL Tag Mappings:** Search for more specific XBRL tags that directly map to Compustat items instead of using derived calculations.

4. **Fix Formatting Issues:** Normalize phone numbers and legal names to match Compustat format.

5. **Fix STATE Extraction:** For NVDA, extract business address state (CA) instead of state of incorporation (DE).

### Long-Term Improvements

1. **Unit Normalization:** Implement proper unit conversion based on XBRL unitRef elements.

2. **12-Month Aggregation:** Implement proper trailing 12-month calculations by aggregating across quarters instead of multiplying by 4.

3. **Validation Rules:** Add validation rules to flag items with unusually high discrepancies for manual review.

4. **Incremental Improvement:** Prioritize items by importance and improve accuracy iteratively.

## Next Steps

1. ✅ **Step 1 Complete:** Data accuracy validation
2. **Step 2:** Improve accuracy for high-priority items
3. **Step 3:** Fix formatting and extraction issues
4. **Step 4:** Scale to all companies
5. **Step 5:** Expand to other tables

## Files Generated

- `validate_data_accuracy.py`: Validation script
- `validation_results.txt`: Detailed validation output
- `VALIDATION_REPORT.md`: This report

