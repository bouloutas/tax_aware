# Task: Map Extracted Data to Compustat Schema

**Purpose:** Map parsed SEC filing data to Compustat database schema  
**Status:** ✅ Complete (with enhancements)  
**Last Updated:** November 18, 2025

## Overview

Map extracted data from SEC filings to Compustat database tables:
- COMPANY table (39 fields)
- SECURITY table (15 fields)
- SEC_IDCURRENT table (5 fields)
- CSCO_IKEY table (16 fields)
- CSCO_IFNDQ table (7 fields)
- And 200+ more tables

## Process Steps

### 1. COMPANY Table Mapping

**File:** `src/data_extractor.py` - `populate_company_table()` method

**Field Mappings:**

| Compustat Field | Source | Extraction Method |
|-----------------|--------|------------------|
| GVKEY | CIK mapping file | Lookup from `cik_to_gvkey_mapping.csv` |
| CIK | SEC filing header | Extract from filing |
| CONM | SEC filing header | Extract company name |
| CONML | XBRL `dei:EntityRegistrantName` | Extract legal name |
| ADD1 | XBRL `dei:EntityAddressAddressLine1` | Extract address line 1 |
| ADD2 | XBRL `dei:EntityAddressAddressLine2` | Extract address line 2 |
| CITY | XBRL `dei:EntityAddressCityOrTown` | Extract city |
| STATE | XBRL `dei:EntityAddressStateOrProvince` | Extract state |
| ADDZIP | XBRL `dei:EntityAddressPostalZipCode` | Extract ZIP code |
| FYRC | XBRL `dei:DocumentPeriodEndDate` | Extract month (1-12) |
| BUSDESC | HTML Item 1 section | Extract business description |
| SIC | Company info section | Extract SIC code |
| PHONE | Company info section | Extract phone number |
| WEBURL | Company info section | Extract website URL |
| EIN | Company info section | Extract EIN |
| GGROUP, GIND, GSECTOR, GSUBIND | SIC mapping or filing | Map SIC to GICS codes |

**Code:**
```python
def populate_company_table(self, extracted_data):
    companies = {}
    for data in extracted_data:
        gvkey = data.get('gvkey')
        company_metadata = data.get('company_metadata', {})
        
        companies[gvkey] = {
            'GVKEY': gvkey,
            'CIK': data.get('cik', '').zfill(10),
            'CONM': data.get('company_name', ''),
            'CONML': company_metadata.get('legal_name', ''),
            'ADD1': company_metadata.get('address_line1', ''),
            'CITY': company_metadata.get('city', ''),
            'STATE': company_metadata.get('state', ''),
            'ADDZIP': company_metadata.get('zip_code', ''),
            'FYRC': company_metadata.get('fiscal_year_end_month'),
        }
    # Insert/Update database
```

### 2. SECURITY Table Mapping

**File:** `src/data_extractor.py` - `populate_security_table()` method

**Field Mappings:**

| Compustat Field | Source | Extraction Method |
|-----------------|--------|------------------|
| GVKEY | CIK mapping | Lookup from mapping |
| IID | Default | Use '01' for primary security |
| TIC | XBRL `dei:TradingSymbol` | Extract ticker |
| TPCI | Default | Use '0' for primary issue |
| CUSIP | Security info section | Extract CUSIP (9 chars) |
| ISIN | Construct or extract | US + CUSIP + check digit |
| SEDOL | External mapping | UK securities only |
| EXCHG | Ticker mapping | Map ticker to exchange code |
| EXCNTRY | Default | 'USA' for US securities |

**Exchange Code Mapping:**
- NASDAQ = 3
- NYSE = 1
- NYSE American = 2

### 3. CSCO_IKEY Table Mapping

**File:** `src/financial_mapper.py` - `map_financial_data()` and `insert_financial_data()` methods

**Field Mappings:**

| Compustat Field | Source | Calculation |
|-----------------|--------|-------------|
| GVKEY | CIK mapping | Lookup |
| DATADATE | Filing date | Use filing date |
| COIFND_ID | Generate | Hash of `{gvkey}_{filing_date}_{filing_type}` |
| INDFMT | Default | 'INDL' (Industrial) |
| CONSOL | Default | 'C' (Consolidated) |
| POPSRC | Default | 'D' (Domestic) |
| FYR | Fiscal year | Extract from fiscal_year_end_month |
| DATAFMT | Default | 'STD' (Standard) |
| FQTR | Fiscal quarter | Calculate from filing date |
| FYEARQ | Fiscal year | Extract from filing date |
| PDATEQ | Period date | Use filing date |
| **CQTR** | Calendar quarter | `(MONTH(DATADATE) - 1) // 3 + 1` |
| **CYEARQ** | Calendar year | `YEAR(DATADATE)` |
| **CURCDQ** | Currency | Extract from XBRL `unitRef` |
| **FDATEQ** | Fiscal date | Extract from `document_period_end_date` |
| **RDQ** | Report date | Use filing date (EFFDATE) |

**Code:**
```python
def map_financial_data(self, extracted_data):
    # Calculate calendar fields
    calendar_quarter = (filing_date.month - 1) // 3 + 1
    calendar_year = filing_date.year
    
    # Extract fiscal date
    fiscal_date = document_period_end_date if available else filing_date
    
    # Extract currency
    currency = company_metadata.get('currency', 'USD')
    
    return {
        'calendar_quarter': calendar_quarter,
        'calendar_year': calendar_year,
        'fiscal_date': fiscal_date,
        'currency': currency,
        # ... other fields
    }
```

### 4. CSCO_IFNDQ Table Mapping

**File:** `src/financial_mapper.py` - `map_financial_data()` and `insert_financial_data()` methods

**Field Mappings:**

| Compustat Field | Source | Value |
|-----------------|--------|-------|
| COIFND_ID | Link to CSCO_IKEY | Same as CSCO_IKEY.COIFND_ID |
| EFFDATE | Filing date | Use filing date |
| ITEM | Compustat item code | Map from extracted field |
| VALUEI | Extracted value | Financial data value |
| **DATACODE** | Default | 0 (standard data) |
| **RST_TYPE** | Default | 'N' (not restated) |
| **THRUDATE** | Fiscal date | Use `document_period_end_date` |

**Financial Item Mapping:**

| Extracted Field | Compustat Item | XBRL Tag |
|-----------------|----------------|----------|
| revenue | REVTQ | us-gaap:Revenues |
| sales | SALEQ | us-gaap:SalesRevenueNet |
| cost_of_revenue | COGSQ | us-gaap:CostOfGoodsAndServicesSold |
| operating_income | OIADPQ | us-gaap:OperatingIncomeLoss |
| net_income | NIQ | us-gaap:NetIncomeLoss |
| tax_expense | TXTQ | us-gaap:IncomeTaxExpenseBenefit |
| sga_expense | XSGAQ | us-gaap:SellingGeneralAndAdministrativeExpense |
| rd_expense | XRDQ | us-gaap:ResearchAndDevelopmentExpense |
| interest_expense | XINTQ | us-gaap:InterestExpenseDebt |
| depreciation | DPQ | us-gaap:DepreciationDepletionAndAmortization |
| assets | ATQ | us-gaap:Assets |
| current_assets | ACTQ | us-gaap:AssetsCurrent |
| cash | CHEQ | us-gaap:CashAndCashEquivalentsAtCarryingValue |
| receivables | RECTQ | us-gaap:AccountsReceivableNetCurrent |
| inventory | INVTQ | us-gaap:InventoryNet |
| ppe_net | PPENTQ | us-gaap:PropertyPlantAndEquipmentNet |
| liabilities | LTQ | us-gaap:Liabilities |
| current_liabilities | LCTQ | us-gaap:LiabilitiesCurrent |
| short_term_debt | DLCQ | us-gaap:DebtCurrent |
| long_term_debt | DLTTQ | us-gaap:LongTermDebtAndCapitalLeaseObligations |
| equity | CEQQ | us-gaap:StockholdersEquity |
| shares_outstanding | CSHPRQ | us-gaap:WeightedAverageNumberOfSharesOutstandingBasic |
| eps_basic | EPSPXQ | us-gaap:EarningsPerShareBasic |
| eps_diluted | EPSPIQ | us-gaap:EarningsPerShareDiluted |

**Code:**
```python
COMPUSTAT_ITEM_MAPPING = {
    'revenue': 'REVTQ',
    'assets': 'ATQ',
    'net_income': 'NIQ',
    # ... 30+ more mappings
}

def map_financial_data(self, extracted_data):
    financial_data = extracted_data.get('financial_data', {})
    items = {}
    
    for key, value in financial_data.items():
        item_code = COMPUSTAT_ITEM_MAPPING.get(key)
        if item_code and value is not None:
            items[item_code] = float(value)
    
    return {'items': items}
```

### 5. SEC_IDCURRENT Table Mapping

**File:** `src/data_extractor.py` - `populate_sec_idcurrent_table()` method

**Field Mappings:**

| Compustat Field | Source | Value |
|-----------------|--------|-------|
| GVKEY | CIK mapping | Lookup |
| IID | Default | '01' for primary security |
| ITEM | Identifier type | 'TIC' for ticker |
| ITEMVALUE | Extracted ticker | From `dei:TradingSymbol` |
| PACVERTOFEEDPOP | Default | 0 or 1 |

### 6. Database Operations

**Upsert Logic:**
- Check if record exists (SELECT COUNT(*))
- If exists: UPDATE
- If not exists: INSERT

**Code Pattern:**
```python
existing = conn.execute("""
    SELECT COUNT(*) FROM table WHERE key = ?
""", [key]).fetchone()[0]

if existing > 0:
    conn.execute("UPDATE ...")
else:
    conn.execute("INSERT ...")
```

### 7. Scaling to All Companies

**Process:**
1. Load CIK-to-GVKEY mapping
2. For each parsed filing:
   - Map to COMPANY table (once per company)
   - Map to SECURITY table (once per security)
   - Map to SEC_IDCURRENT table (once per identifier)
   - Map to CSCO_IKEY table (once per filing period)
   - Map to CSCO_IFNDQ table (once per financial item)

**Batch Processing:**
- Process filings in batches
- Group by GVKEY for COMPANY/SECURITY tables
- Group by COIFND_ID for CSCO_IKEY/CSCO_IFNDQ tables

### 8. Dependencies

- `duckdb` - Database operations
- `csv` - CIK mapping file
- `hash` - Generate COIFND_ID

### 9. Output Files

- Database: `compustat_edgar.duckdb`
- Logs: `logs/map_{timestamp}.log`
- Mapping reports: `logs/mapping_report_{timestamp}.csv`

## Notes

- Always use upsert logic (UPDATE or INSERT)
- Handle missing data gracefully (use defaults where appropriate)
- Log mapping failures for review
- Validate mapped data against source Compustat

## Field-by-Field Mapping Reference

See `FIELD_MAPPING_GUIDE.md` for comprehensive field-by-field mapping details.

