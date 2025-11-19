# Task: Validate Extracted Data Against Compustat

**Purpose:** Validate extracted data against source Compustat database  
**Status:** 🔄 In Progress  
**Last Updated:** November 18, 2025

## Overview

Compare extracted data in `compustat_edgar.duckdb` against source `compustat.duckdb` to ensure accuracy and completeness.

## Process Steps

### 1. Validation Approach

**Comparison Levels:**
1. **Record Counts** - Compare number of records per table
2. **Field Population** - Compare which fields are populated
3. **Value Comparison** - Compare actual values field-by-field
4. **Completeness** - Compare data coverage over time

### 2. COMPANY Table Validation

**Validation Script:** `validate_pilot.py` or custom validation

**Fields to Compare:**
- CONM, CONML, ADD1, CITY, STATE, ADDZIP, FYRC
- SIC, PHONE, WEBURL, EIN
- GGROUP, GIND, GSECTOR, GSUBIND

**Code:**
```python
source = source_db.execute("""
    SELECT CONM, CONML, ADD1, CITY, STATE, ADDZIP, FYRC
    FROM main.COMPANY WHERE GVKEY = ?
""", [gvkey]).fetchone()

target = target_db.execute("""
    SELECT CONM, CONML, ADD1, CITY, STATE, ADDZIP, FYRC
    FROM main.COMPANY WHERE GVKEY = ?
""", [gvkey]).fetchone()

# Compare field by field
for i, field in enumerate(['CONM', 'CONML', 'ADD1', ...]):
    if source[i] == target[i]:
        print(f"✅ {field}: Match")
    else:
        print(f"⚠️ {field}: Source={source[i]}, Target={target[i]}")
```

### 3. CSCO_IKEY Table Validation

**Fields to Compare:**
- Record counts per GVKEY
- DATADATE coverage
- Calendar fields (CQTR, CYEARQ, CURCDQ, FDATEQ, RDQ)
- Fiscal fields (FQTR, FYEARQ, PDATEQ)

**Code:**
```python
# Compare record counts
source_count = source_db.execute("""
    SELECT COUNT(*) FROM main.CSCO_IKEY WHERE GVKEY = ?
""", [gvkey]).fetchone()[0]

target_count = target_db.execute("""
    SELECT COUNT(*) FROM main.CSCO_IKEY WHERE GVKEY = ?
""", [gvkey]).fetchone()[0]

print(f"Records: Source={source_count}, Target={target_count}")

# Compare field population
source_fields = source_db.execute("""
    SELECT COUNT(CQTR), COUNT(CYEARQ), COUNT(CURCDQ), COUNT(FDATEQ), COUNT(RDQ)
    FROM main.CSCO_IKEY WHERE GVKEY = ?
""", [gvkey]).fetchone()

target_fields = target_db.execute("""
    SELECT COUNT(CQTR), COUNT(CYEARQ), COUNT(CURCDQ), COUNT(FDATEQ), COUNT(RDQ)
    FROM main.CSCO_IKEY WHERE GVKEY = ?
""", [gvkey]).fetchone()
```

### 4. CSCO_IFNDQ Table Validation

**Fields to Compare:**
- Record counts per GVKEY
- Unique items extracted
- Item values (with tolerance for rounding)
- Metadata fields (DATACODE, RST_TYPE, THRUDATE)

**Code:**
```python
# Compare item counts
source_items = source_db.execute("""
    SELECT COUNT(DISTINCT f.ITEM)
    FROM main.CSCO_IFNDQ f
    JOIN main.CSCO_IKEY k ON f.COIFND_ID = k.COIFND_ID
    WHERE k.GVKEY = ?
""", [gvkey]).fetchone()[0]

target_items = target_db.execute("""
    SELECT COUNT(DISTINCT f.ITEM)
    FROM main.CSCO_IFNDQ f
    JOIN main.CSCO_IKEY k ON f.COIFND_ID = k.COIFND_ID
    WHERE k.GVKEY = ?
""", [gvkey]).fetchone()[0]

# Compare values for specific items
for item in ['REVTQ', 'ATQ', 'NIQ']:
    source_value = source_db.execute("""
        SELECT AVG(f.VALUEI)
        FROM main.CSCO_IFNDQ f
        JOIN main.CSCO_IKEY k ON f.COIFND_ID = k.COIFND_ID
        WHERE k.GVKEY = ? AND f.ITEM = ?
    """, [gvkey, item]).fetchone()[0]
    
    target_value = target_db.execute("""
        SELECT AVG(f.VALUEI)
        FROM main.CSCO_IFNDQ f
        JOIN main.CSCO_IKEY k ON f.COIFND_ID = k.COIFND_ID
        WHERE k.GVKEY = ? AND f.ITEM = ?
    """, [gvkey, item]).fetchone()[0]
    
    if abs(source_value - target_value) < 100:  # Tolerance
        print(f"✅ {item}: Match")
    else:
        print(f"⚠️ {item}: Source={source_value}, Target={target_value}")
```

### 5. Validation Scripts

**Scripts:**
- `validate_pilot.py` - Validate COMPANY, SECURITY, SEC_IDCURRENT
- `validate_financial.py` - Validate CSCO_IKEY, CSCO_IFNDQ
- `compare_tables_detailed.py` - Detailed field-by-field comparison

**Usage:**
```bash
python validate_pilot.py
python validate_financial.py
python compare_tables_detailed.py
```

### 6. Validation Report

**Report Format:**
```
================================================================================
VALIDATION REPORT: MSFT (GVKEY: 012141)
================================================================================

COMPANY Table:
  ✅ CONM: Match
  ⚠️ CONML: Source=Microsoft Corp, Target=MICROSOFT CORPORATION
  ⚠️ ADD1: Source=One Microsoft Way, Target=ONE MICROSOFT WAY
  ✅ CITY: Match
  ⚠️ STATE: Source=WA, Target=N/A
  ✅ ADDZIP: Match
  ✅ FYRC: Match

CSCO_IKEY Table:
  Records: Source=166, Target=10
  Field Population:
    ✅ CQTR: Source=166, Target=5
    ✅ CYEARQ: Source=166, Target=5
    ✅ CURCDQ: Source=166, Target=5
    ✅ FDATEQ: Source=157, Target=5
    ✅ RDQ: Source=159, Target=5

CSCO_IFNDQ Table:
  Records: Source=28004, Target=104
  Unique Items: Source=294, Target=23
  Value Comparison:
    ✅ REVTQ: Match (within tolerance)
    ✅ ATQ: Match (within tolerance)
    ⚠️ NIQ: Source=12345, Target=12300 (difference: 45)
```

### 7. Discrepancy Handling

**Types of Discrepancies:**
1. **Missing Data** - Field not extracted
2. **Value Differences** - Values don't match (within tolerance)
3. **Format Differences** - Same data, different format (e.g., case)
4. **Missing Records** - Records not extracted

**Resolution:**
1. Identify root cause (parsing issue, mapping issue, etc.)
2. Fix extraction/mapping logic
3. Re-process filings
4. Re-validate

### 8. Scaling to All Companies

**Process:**
1. Validate sample companies first (MSFT, NVDA)
2. Fix issues identified
3. Validate larger sample (100 companies)
4. Fix remaining issues
5. Validate all companies

**Automation:**
- Generate validation reports automatically
- Flag discrepancies above threshold
- Generate fix recommendations

### 9. Dependencies

- Source database: `compustat.duckdb`
- Target database: `compustat_edgar.duckdb`
- DuckDB for queries

### 10. Output Files

- Validation reports: `reports/validation_{timestamp}.md`
- Discrepancy logs: `logs/discrepancies_{timestamp}.csv`
- Fix recommendations: `reports/fix_recommendations_{timestamp}.md`

## Notes

- Use tolerance for numeric comparisons (rounding differences)
- Handle NULL values appropriately
- Compare case-insensitive for text fields
- Log all discrepancies for review
- Prioritize critical fields (revenue, assets, net income)

## Validation Checklist

- [ ] COMPANY table: All fields populated and match source
- [ ] SECURITY table: All fields populated and match source
- [ ] CSCO_IKEY table: All fields populated and match source
- [ ] CSCO_IFNDQ table: All items extracted and values match
- [ ] Record counts match (within expected range)
- [ ] Date coverage matches (same time periods)
- [ ] No data quality issues (NULLs, outliers, etc.)

