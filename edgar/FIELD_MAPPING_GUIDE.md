# Field-by-Field Mapping Guide: SEC EDGAR → Compustat

**Date:** November 18, 2025  
**Purpose:** Comprehensive guide for mapping every field from SEC filings to Compustat tables

## COMPANY Table Mapping (39 fields)

### ✅ Already Populated (3 fields)
- **GVKEY:** From CIK mapping file
- **CIK:** From SEC filings (dei:EntityCentralIndexKey)
- **CONM:** From SEC filings (dei:EntityRegistrantName)

### 🔄 To Populate (36 fields)

#### Address Fields (7 fields)
| Field | XBRL Tag | HTML Location | Example | Status |
|-------|----------|---------------|---------|--------|
| ADD1 | dei:EntityAddressAddressLine1 | Cover page | "One Microsoft Way" | ✅ Tag found |
| ADD2 | dei:EntityAddressAddressLine2 | Cover page | (optional) | ⏳ |
| ADD3 | dei:EntityAddressAddressLine3 | Cover page | (optional) | ⏳ |
| ADD4 | dei:EntityAddressAddressLine4 | Cover page | (optional) | ⏳ |
| ADDZIP | dei:EntityAddressPostalZipCode | Cover page | "98052-6399" | ✅ Tag found |
| CITY | dei:EntityAddressCityOrTown | Cover page | "Redmond" | ✅ Tag found |
| COUNTY | (parse from address) | Cover page | (optional) | ⏳ |

**Extraction Method:**
```python
# XBRL extraction
address_line1 = extract_xbrl_tag('dei:EntityAddressAddressLine1')
city = extract_xbrl_tag('dei:EntityAddressCityOrTown')
state = extract_xbrl_tag('dei:EntityAddressStateOrProvince')
zip_code = extract_xbrl_tag('dei:EntityAddressPostalZipCode')
```

#### Business Description (2 fields)
| Field | XBRL Tag | HTML Location | Example | Status |
|-------|----------|---------------|---------|--------|
| BUSDESC | (text extraction) | Item 1 "Business" section | First 2-3 paragraphs | ⏳ |
| CONML | dei:EntityRegistrantName | Cover page | "Microsoft Corporation" | ✅ Tag found |

**Extraction Method:**
```python
# HTML extraction - Item 1 Business section
business_desc = extract_section('Item 1', 'Business', first_n_paragraphs=3)
```

#### Status Fields (3 fields)
| Field | Source | Method | Status |
|-------|--------|--------|--------|
| COSTAT | Active if filings exist | Set to 'A' if current | ⏳ |
| DLDTE | 8-K delisting notices | Extract date from 8-K | ⏳ |
| DLRSN | 8-K delisting notices | Extract reason from 8-K | ⏳ |

#### Identifiers (2 fields)
| Field | XBRL Tag | HTML Location | Status |
|-------|----------|---------------|--------|
| EIN | (company info section) | Cover page or company info | ⏳ |
| FIC | Default to 'USA' | Set default | ⏳ |

#### Industry Classification (4 fields)
| Field | Source | Method | Status |
|-------|--------|--------|--------|
| GGROUP | SIC mapping or GICS in filing | Map SIC to GICS Group | ⏳ |
| GIND | SIC mapping or GICS in filing | Map SIC to GICS Industry | ⏳ |
| GSECTOR | SIC mapping or GICS in filing | Map SIC to GICS Sector | ⏳ |
| GSUBIND | SIC mapping or GICS in filing | Map SIC to GICS Sub-Industry | ⏳ |

**Extraction Method:**
```python
# Get SIC from filing, then map to GICS
sic = extract_sic_code()  # From filing
gics = map_sic_to_gics(sic)  # Use Compustat mapping table
```

#### Fiscal Year (1 field)
| Field | XBRL Tag | Method | Status |
|-------|----------|--------|--------|
| FYRC | dei:DocumentPeriodEndDate | Extract month (1-12) | ✅ Tag found |

**Extraction Method:**
```python
period_end = extract_xbrl_tag('dei:DocumentPeriodEndDate')
fiscal_month = extract_month(period_end)  # 1-12
```

#### Other Fields (18 fields)
| Field | Source | Method | Status |
|-------|--------|--------|--------|
| SIC | Company info section | Extract SIC code | ⏳ |
| PHONE | Company info section | Extract phone number | ⏳ |
| WEBURL | Company info section | Extract website URL | ⏳ |
| INCORP | State of incorporation | Extract from filing | ⏳ |
| LOC | Default to 'USA' | Set default | ⏳ |
| NAICS | Company info section | Extract NAICS code | ⏳ |
| STATE | Parse from address | Extract state | ⏳ |
| IPODATE | Historical data | Extract IPO date | ⏳ |
| And 10 more... | Various sources | Various methods | ⏳ |

## SECURITY Table Mapping (15 fields)

### ✅ Already Populated (4 fields)
- **GVKEY:** From CIK mapping
- **IID:** Default to '01'
- **TIC:** From SEC filings (dei:TradingSymbol)
- **TPCI:** Default to '0'

### 🔄 To Populate (11 fields)

#### Security Identifiers (3 fields)
| Field | Source | Method | Status |
|-------|--------|--------|--------|
| CUSIP | Security info section | Extract CUSIP (9 chars) | ⏳ |
| ISIN | Construct or extract | US + CUSIP + check digit | ⏳ |
| SEDOL | External mapping | UK securities only | ⏳ |

**Extraction Method:**
```python
# Look for CUSIP in security information section
cusip = extract_cusip()  # Pattern: XXXXX-XXX or XXXXX XXX
isin = construct_isin(cusip)  # US + CUSIP + check digit
```

#### Exchange Information (2 fields)
| Field | Source | Method | Status |
|-------|--------|--------|--------|
| EXCHG | Ticker mapping | Map ticker to exchange code | ⏳ |
| EXCNTRY | Default to 'USA' | Set default | ⏳ |

**Exchange Code Mapping:**
- NASDAQ = 3
- NYSE = 1
- NYSE American = 2
- Other = various codes

#### Status Fields (4 fields)
| Field | Source | Method | Status |
|-------|--------|--------|--------|
| DLDTEI | 8-K delisting notices | Extract date | ⏳ |
| DLRSNI | 8-K delisting notices | Extract reason | ⏳ |
| DSCI | Security description | Extract description | ⏳ |
| SECSTAT | Security status | Set based on current status | ⏳ |

#### Other Fields (2 fields)
| Field | Source | Method | Status |
|-------|--------|--------|--------|
| EPF | Exchange price flag | Set default | ⏳ |
| IBTIC | IBES ticker | External mapping | ⏳ |

## CSCO_IKEY Table Mapping (16 fields)

### ✅ Already Populated (11 fields)
- GVKEY, COIFND_ID, DATADATE, INDFMT, CONSOL, POPSRC, FYR, DATAFMT, FQTR, FYEARQ, PDATEQ

### 🔄 To Populate (5 fields)

| Field | Source | Method | Status |
|-------|--------|--------|--------|
| CQTR | DATADATE | Extract calendar quarter (1-4) | ⏳ |
| CURCDQ | XBRL unitRef | Extract currency code | ⏳ |
| CYEARQ | DATADATE | Extract calendar year | ⏳ |
| FDATEQ | XBRL DocumentPeriodEndDate | Extract fiscal period end date | ✅ Tag found |
| RDQ | Filing date | Use EFFDATE value | ✅ Available |

**Extraction Methods:**
```python
# Calendar quarter from DATADATE
cqtr = (month(datadate) - 1) // 3 + 1

# Currency from XBRL unitRef
currency = extract_unit_ref()  # e.g., "USD"

# Calendar year from DATADATE
cyearq = year(datadate)

# Fiscal date from XBRL
fdateq = extract_xbrl_tag('dei:DocumentPeriodEndDate')

# Report date (filing date)
rdq = effdate  # Already have this
```

## CSCO_IFNDQ Table Mapping (7 fields)

### ✅ Already Populated (4 fields)
- COIFND_ID, EFFDATE, ITEM, VALUEI

### 🔄 To Populate (3 fields)

| Field | Source | Method | Status |
|-------|--------|--------|--------|
| DATACODE | Default or XBRL | Set to 0 (standard) | ⏳ |
| RST_TYPE | Restatement indicator | Extract from XBRL | ⏳ |
| THRUDATE | XBRL context period | Extract period end date | ⏳ |

**Extraction Methods:**
```python
# Data code (usually 0 for standard data)
datacode = 0  # Default

# Restatement type (R=Restated, N=Not restated)
rst_type = extract_restatement_indicator() or 'N'

# Through date from XBRL context
thrudate = extract_context_period_end(context_ref)
```

### Additional Financial Items to Extract (50+ items)

**Currently Extracting:** REVTQ, ATQ, LTQ, CEQQ, NIQ, CHEQ, EPSPXQ, EPSPIQ, CSHPRQ

**Need to Extract (from Compustat analysis):**
- **Operating Items:** OIADPQ, OIBDPQ, OEPS12, OPEPSQ, OEPSXQ, OEPF12
- **Expense Items:** XSGAQ, COGSQ, XINTQ, XRDQ, XOPRQ, XIDOQ, XIQ
- **Asset Items:** ACTQ, PPENTQ, INVTQ, RECTQ, CHEQ (already have)
- **Liability Items:** LCTQ, DLTTQ, DLCQ, LOQ, LCOQ, LTQ (already have)
- **Equity Items:** SEQQ, CSTKEQ, CSTKQ, CAPSQ, CEQQ (already have)
- **Income Items:** PIQ, IBQ, IBCOMQ, IBADJQ, NOPIQ, NIQ (already have)
- **Tax Items:** TXTQ, TXPQ, TXDIQ, TXDITCQ
- **Share Items:** CSHOQ, CSH12Q, CSHPRQ (already have)
- **EPS Items:** EPSPXQ (already have), EPSPIQ (already have), EPSFIQ, EPSFXQ, EPSX12
- **Other Items:** DPQ, DOQ, DVPQ, MIIQ, ICAPTQ, SPIQ, LTMIBQ

**XBRL Tag Mapping:**
```python
# Comprehensive mapping
XBRL_TO_COMPUSTAT = {
    'RevenueFromContractWithCustomerExcludingAssessedTax': 'REVTQ',
    'Revenues': 'REVTQ',
    'SalesRevenueNet': 'SALEQ',
    'Assets': 'ATQ',
    'AssetsCurrent': 'ACTQ',
    'Liabilities': 'LTQ',
    'LiabilitiesCurrent': 'LCTQ',
    'StockholdersEquity': 'CEQQ',
    'NetIncomeLoss': 'NIQ',
    'CashAndCashEquivalentsAtCarryingValue': 'CHEQ',
    'OperatingIncomeLoss': 'OIADPQ',
    'CostOfGoodsAndServicesSold': 'COGSQ',
    'SellingGeneralAndAdministrativeExpense': 'XSGAQ',
    'ResearchAndDevelopmentExpense': 'XRDQ',
    'InterestExpenseDebt': 'XINTQ',
    'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest': 'PIQ',
    'IncomeTaxExpenseBenefit': 'TXTQ',
    'LongTermDebtAndCapitalLeaseObligations': 'DLTTQ',
    'DebtCurrent': 'DLCQ',
    'PropertyPlantAndEquipmentNet': 'PPENTQ',
    'DepreciationDepletionAndAmortization': 'DPQ',
    'CommonStockSharesOutstanding': 'CSHOQ',
    'WeightedAverageNumberOfSharesOutstandingBasic': 'CSHPRQ',
    'EarningsPerShareBasic': 'EPSPXQ',
    'EarningsPerShareDiluted': 'EPSPIQ',
    # ... 30+ more mappings
}
```

## CSCO_ITXT Table Mapping

**Purpose:** Text fields from filings

**Structure:**
- COIFND_ID (link to CSCO_IKEY)
- EFFDATE
- ITEM (e.g., 'UPDQ', 'BUSDESC')
- RST_TYPE
- THRUDATE
- VALUED (text value)

**Items to Extract:**
- **UPDQ:** Update date (filing date)
- **BUSDESC:** Business description (from Item 1)
- **Other text fields:** As needed

**Extraction Method:**
```python
# Extract text fields
text_items = {
    'UPDQ': filing_date,
    'BUSDESC': business_description,
    # ... other text items
}
```

## SEC_DPRC Table Mapping

**Purpose:** Daily price data

**Structure:** 18 columns including GVKEY, IID, DATADATE, PRCCD, PRCHD, PRCLD, PRCOD, CSHOC, etc.

**Source:** External price data (not in SEC filings)
- May need to use external API (Yahoo Finance, Alpha Vantage, etc.)
- Or use Compustat price data if available

**Note:** This table requires external data source, not extractable from SEC filings alone.

## SEC_DIVID Table Mapping

**Purpose:** Dividend information

**Structure:** 35 columns including GVKEY, IID, DIVD, DIVDT, etc.

**Source:** SEC filings - Dividend declarations
- 8-K filings for dividend announcements
- Proxy statements
- May be in XBRL tags related to dividends

**Extraction Method:**
```python
# Look for dividend declarations in 8-K filings
dividends = extract_dividends_from_8k_filings()
```

## Implementation Priority

### Phase 1: Core Fields (High Priority)
1. ✅ Complete COMPANY address fields (7 fields)
2. ✅ Complete COMPANY business description (1 field)
3. ✅ Complete SECURITY identifiers (3 fields)
4. ✅ Complete CSCO_IKEY calendar fields (5 fields)
5. ✅ Complete CSCO_IFNDQ metadata fields (3 fields)
6. ✅ Extract 50+ additional financial items

### Phase 2: Enhanced Fields (Medium Priority)
7. ✅ CSCO_ITXT table
8. ✅ SEC_DIVID table
9. ✅ Additional COMPANY metadata fields

### Phase 3: External Data (Low Priority)
10. ✅ SEC_DPRC table (requires external API)
11. ✅ Other tables requiring external data

## Validation Checklist

For each field:
- [ ] Extract from SEC filing
- [ ] Map to Compustat field
- [ ] Transform units/formats
- [ ] Insert into database
- [ ] Validate against compustat.duckdb
- [ ] Fix discrepancies
- [ ] Document mapping

## Next Steps

1. Enhance XBRL parser to extract all dei: tags
2. Enhance HTML parser to extract Item 1 Business section
3. Create comprehensive field mappers
4. Extract all 50+ financial items
5. Validate field-by-field against compustat.duckdb

