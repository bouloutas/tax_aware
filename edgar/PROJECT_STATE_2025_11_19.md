# Project State - November 19, 2025

## Current Status Overview

**Project Goal:** Replicate Compustat database (`compustat.duckdb`) by extracting data from SEC EDGAR filings and populating `compustat_edgar.duckdb` for MSFT and NVDA.

**Current Accuracy:**
- **MSFT:** 17.2% financial accuracy (249/1,444 matches)
- **NVDA:** 4.7% financial accuracy (61/1,294 matches)
- **Company Fields:** 17/18 (94.4%) for both companies (only CONML mismatch)

## Recent Work Completed (Last Session)

### 1. CSHOPQ Unit Conversion ✅
- **Issue:** CSHOPQ was in millions (7519.0) but Compustat expects billions (6.642)
- **Fix:** Modified `_calculate_share_eps_metrics()` in `src/financial_mapper.py` to convert from millions to billions
- **Result:** Now showing 7.496 vs Compustat 6.642 (much closer, slight difference due to using weighted average vs shares outstanding)
- **Location:** `src/financial_mapper.py` lines 2781-2796

### 2. Removed Problematic OCI Rough Estimates ✅
- **Issue:** Derived calculations were creating incorrect large values for CISECGLQ (17,777) and AOCIDERGLQ (1,268)
- **Fix:** Removed rough estimate calculations (`CIQ * 0.2` for CISECGLQ, `ANOQ * 0.2` for AOCIDERGLQ)
- **Result:** Large incorrect values no longer appear, but OCI items are now missing (need proper extraction)
- **Location:** `src/financial_mapper.py` lines 2024-2034

### 3. OCI Large Value Filtering ✅
- **Issue:** OCI tags from 10-K filings contain large accumulated/YTD values instead of quarterly period changes
- **Fix:** Added filtering logic to skip values >1000 for CISECGLQ and >100 for AOCIDERGLQ
- **Result:** Prevents incorrect large values, but correct quarterly values still need to be found
- **Location:** `src/financial_mapper.py` lines 2683-2708

## Current Issues & Next Steps

### Priority 1: OCI Period Change Extraction (Goal 3)

**Problem:**
- CISECGLQ, CIDERGLQ, AOCIDERGLQ are not being populated correctly
- 10-K filings report annual/YTD values, not quarterly period changes
- Need to find quarterly OCI tags or calculate period changes from accumulated values

**Current State:**
- Large values (>1000) are filtered out
- Small values (<100) are accepted but may not be correct
- For 2024-06-30 (10-K quarter), Compustat shows:
  - CISECGLQ: 88.0
  - CIDERGLQ: -4.0
  - AOCIDERGLQ: -3.0
- Extracted values are missing or incorrect

**Next Steps:**
1. Investigate 10-K filing structure to find quarterly OCI breakdown
2. Check if Statement of Comprehensive Income has quarterly columns
3. Calculate period changes by comparing to previous quarter's accumulated values
4. Look for period-specific XBRL tags (e.g., "currentperiod", "periodchange")

**Code Location:** `src/financial_mapper.py` lines 2656-2740 (`_ensure_oci_breakouts`)

### Priority 2: Operating Lease Items (Goal 4)

**Problem:**
- OLMIQ, OLMTQ, MSAQ values are closer but still not matching Compustat
- For 2024-06-30:
  - Extracted: OLMIQ=-3580, OLMTQ=15497, MSAQ=-18961
  - Compustat: OLMIQ=-2493, OLMTQ=21570, MSAQ=-2625

**Current State:**
- Logic calculates current from total - noncurrent
- Sign conventions may need adjustment
- May need to check for additional operating lease tags

**Next Steps:**
1. Verify sign conventions (negative for liabilities, negative for assets)
2. Check if Compustat uses different tag combinations
3. Investigate if there are period-change vs balance sheet value differences
4. Test with NVDA filings to see if patterns are consistent

**Code Location:** `src/financial_mapper.py` lines 2642-2655 (`_ensure_operating_lease_items`)

### Priority 3: Common Stock Values (Goal 5)

**Problem:**
- CSTKQ and CSTKCVQ are not being populated
- Par value per share tags not found in XBRL filings
- Compustat shows very small values (CSTKQ: 0.046, CSTKCVQ: 0.0)

**Current State:**
- Logic attempts to find par value per share or calculate from total par value
- Falls back to explicit common stock value if available
- Shares outstanding is available (CSHOPQ working)

**Next Steps:**
1. Search for alternative par value tags in filings
2. Check if par value can be calculated from common stock value / shares
3. Investigate if Compustat uses a different definition
4. May need to extract from text/HTML if not in XBRL

**Code Location:** `src/financial_mapper.py` lines 2731-2779 (`_ensure_common_stock_values`)

### Priority 4: Other Accuracy Issues

**Top Mismatches (from validation):**
1. CSHOPQ: 7.496 vs 6.642 (using weighted average vs shares outstanding)
2. REVTQ: Large differences (YTD conversion may need refinement)
3. COGSQ: Large differences
4. Various share/EPS metrics: Unit or calculation issues

**Next Steps:**
1. Review YTD to quarterly conversion logic
2. Verify unit conversions for all share-related items
3. Check preferred tag mappings for revenue and COGS

## Key Files & Locations

### Main Code Files
- **`src/financial_mapper.py`** (2,952 lines)
  - `map_financial_data()`: Main mapping function (lines ~1300-1600)
  - `_ensure_oci_breakouts()`: OCI extraction (lines 2656-2740)
  - `_ensure_operating_lease_items()`: Lease extraction (lines 2642-2655)
  - `_ensure_common_stock_values()`: Stock value extraction (lines 2731-2779)
  - `_calculate_share_eps_metrics()`: Share/EPS calculations (lines 2781-2820)
  - `_convert_ytd_items()`: YTD to quarterly conversion (lines 2607-2624)

- **`src/filing_parser.py`**: XBRL/HTML/Text parsing
- **`src/data_extractor.py`**: Orchestrates extraction and database population
- **`validate_data_accuracy.py`**: Validation script

### Database Files
- **Source:** `/home/tasos/compustat.duckdb`
- **Target:** `/home/tasos/tax_aware/edgar/compustat_edgar.duckdb`
- **Raw Filings:** `/home/tasos/tax_aware/edgar/data/raw/`

### Key Data
- **MSFT:** GVKEY='012141', CIK='789019'
- **NVDA:** GVKEY='117768', CIK='1045810'
- **Filing Period:** 2023-2024 (53 filings extracted)

## Validation Commands

```bash
cd /home/tasos/tax_aware/edgar

# Rebuild database
python3 <<'EOF'
import sys
sys.path.insert(0,'.')
from src.data_extractor import DataExtractor
from pathlib import Path
import logging
logging.basicConfig(level=logging.WARNING)
extractor = DataExtractor()
filing_dir = Path('data/raw')
all_data = []
for year in ['2023','2024']:
    year_path = filing_dir/year
    if not year_path.exists():
        continue
    for quarter in ['Q1','Q2','Q3','Q4']:
        quarter_path = year_path/quarter
        if not quarter_path.exists():
            continue
        for cik in ['789019','1045810']:
            path = quarter_path/cik
            if not path.exists():
                continue
            for filing in sorted(path.glob('*.txt')):
                data = extractor.extract_from_filing(filing)
                if data:
                    all_data.append(data)
print('Extracted', len(all_data), 'filings')
extractor.populate_all_tables(all_data)
extractor.close()
EOF

# Run validation
python3 validate_data_accuracy.py

# Check specific values
python3 <<'EOF'
import duckdb
conn=duckdb.connect('compustat_edgar.duckdb')
rows = conn.execute("""
    SELECT datadate, item, valuei 
    FROM main.CSCO_IFNDQ f 
    JOIN main.CSCO_IKEY k USING(coifnd_id) 
    WHERE k.gvkey='012141' AND k.datadate='2024-06-30' 
    AND item IN ('CISECGLQ','CIDERGLQ','AOCIDERGLQ','CSHOPQ','OLMIQ','OLMTQ','MSAQ')
    ORDER BY item
""").fetchall()
for d, item, val in rows:
    print(f"{item}: {val}")
conn.close()
EOF
```

## Testing Specific Issues

### Test OCI Extraction
```bash
cd /home/tasos/tax_aware/edgar
python3 <<'EOF'
import sys
sys.path.insert(0,'.')
from src.filing_parser import XBRLParser
from pathlib import Path

# Check 10-K filing for OCI tags
filing = Path('data/raw/2024/Q3/789019/10-K_0000950170-24-087843.txt')
parser = XBRLParser(filing)
parser.load()
data = parser.parse()
fin = data.get('financial_data', {})

print('OCI tags in 10-K:')
for k in sorted(fin):
    if 'comprehensive' in k.lower() or 'oci' in k.lower():
        val = fin[k]
        print(f"  {k}: {val}")
EOF
```

### Test Operating Lease
```bash
cd /home/tasos/tax_aware/edgar
python3 <<'EOF'
import sys
sys.path.insert(0,'.')
from src.filing_parser import XBRLParser
from pathlib import Path

filing = Path('data/raw/2024/Q4/789019/10-Q_0000950170-24-118967.txt')
parser = XBRLParser(filing)
parser.load()
data = parser.parse()
fin = data.get('financial_data', {})

print('Operating lease tags:')
for k in sorted(fin):
    if 'lease' in k.lower():
        print(f"  {k}: {fin[k]}")
EOF
```

## Known Code Patterns

### Preferred Sources Mapping
- Located in `map_financial_data()` around line 1472
- Overrides less specific matches for critical items
- Includes CSHPRQ, CSHFDQ, CSHOQ, EPSPXQ, etc.

### YTD Conversion
- Handles 10-Q filings by subtracting previous quarter's YTD value
- Defined in `YTD_ITEMS` set (line 12-16)
- Conversion logic in `_convert_ytd_items()` (lines 2607-2624)

### Chronological Sorting
- Filings are sorted by GVKEY, fiscal_year, fiscal_quarter, datadate before insertion
- Ensures YTD calculations are deterministic
- Located in `data_extractor.py` `populate_financial_tables()` (lines 489-511)

## Next Session Work Plan

1. **Start with OCI extraction:**
   - Examine 10-K filing structure for quarterly OCI breakdown
   - Test period change calculation from accumulated values
   - Update `_ensure_oci_breakouts()` with correct logic

2. **Then operating leases:**
   - Verify sign conventions with Compustat data
   - Test with both MSFT and NVDA
   - Refine `_ensure_operating_lease_items()`

3. **Then common stock:**
   - Search for par value in text/HTML if not in XBRL
   - Test alternative calculation methods
   - Update `_ensure_common_stock_values()`

4. **Run validation after each fix:**
   - Check specific items improved
   - Monitor overall accuracy
   - Document any regressions

## Important Notes

- **10-K vs 10-Q:** 10-K filings report annual/YTD values, need special handling for quarterly extraction
- **Sign Conventions:** Compustat uses specific sign conventions (negative for liabilities, negative for assets)
- **Unit Conversions:** Some items in millions, some in billions - verify each item's expected unit
- **Chronological Order:** Critical for YTD calculations - always sort before insertion
- **Preferred Tags:** Use preferred_sources to override less specific matches

## TODO List Status

- ✅ Goal 1: Implement precise share/EPS mapping
- ✅ Goal 2: Map receivable + allowance fields
- 🔄 Goal 3: Map OCI/equity sub-accounts (in progress - need quarterly OCI extraction)
- 🔄 Goal 4: Populate operating lease + minority interest fields (in progress - need sign/calculation refinement)
- 🔄 Goal 5: Reintroduce CSTK buckets (in progress - need par value extraction)
- ✅ CSHOPQ fix: Unit conversion completed

---

**Last Updated:** November 19, 2025, 6:17 PM
**Session Status:** Ready for continuation
**Next Priority:** OCI period change extraction from 10-K filings

