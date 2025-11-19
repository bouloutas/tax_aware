# Mapping Research & Field Extraction Guide

**Date:** November 18, 2025  
**Purpose:** Research findings and guides for populating all Compustat tables

## Research Sources

### Web Research Findings

1. **Compustat Database Structure:**
   - Compustat provides 3,000+ standardized financial items
   - Includes annual, quarterly, year-to-date, and semi-annual data
   - Key identifiers: GVKEY, CUSIP, PERMNO
   - Use linking tables (CCM) for accurate mapping

2. **XBRL to Compustat Mapping:**
   - XBRL standardizes financial reporting but variations exist
   - Map XBRL tags to Compustat data items systematically
   - Validate mapping by comparing with Compustat records
   - Handle unit conversions (millions vs actuals)

3. **Data Extraction Best Practices:**
   - Extract all us-gaap tags comprehensively
   - Parse XBRL contexts for period information
   - Handle multiple reporting periods
   - Validate data accuracy against source filings

### Actual Data Analysis

**Found in MSFT 10-K:**
- 92 unique us-gaap tags
- 36 unique dei tags
- Address: "One Microsoft Way" (found in filing)
- Business description: Available in Item 1 section

**Compustat Items Used for MSFT/NVDA:**
- 50+ unique items in CSCO_IFNDQ
- Top items: XOPRQ, XSGAQ, COGSQ, DPQ, CSHOQ, LOQ, NOPIQ, OIADPQ, etc.
- Each item has 300-500 records for these companies

**COMPANY Table Sample Values:**
- ADD1: "One Microsoft Way", "2788 San Tomas Expressway"
- ADDZIP: "98052-6399", "95051"
- CITY: "Redmond", "Santa Clara"
- BUSDESC: Business descriptions available
- EIN: "91-1144442", "94-3177549"
- FYRC: 1 (January), 6 (June) - Fiscal year end month
- GGROUP, GIND, GSECTOR, GSUBIND: Industry classification codes
- SIC: "7372", "3674"
- PHONE: "425 882 8080", "408 486 2000"
- WEBURL: "www.microsoft.com", "www.nvidia.com"

## Research Sources

### 1. Compustat Database Structure
- **Total Tables:** 211 tables
- **Tables with MSFT/NVDA Data:** 90 tables
- **Key Table Categories:**
  - Core: COMPANY, SECURITY, SEC_IDCURRENT
  - CSCO_*: Quarterly fundamentals (14 tables)
  - SEC_*: Security-level data (25 tables)
  - ACO_*: Annual company operations (22 tables)
  - SEG_*: Segment data (7 tables)
  - CO_*: Company-level data (30+ tables)

### 2. Field Extraction Sources

#### COMPANY Table Fields (39 total, 3 populated, 36 missing)

**Address Fields (7 fields):**
- **ADD1, ADD2, ADD3, ADD4, ADDZIP:** Company address lines
  - **Source:** SEC filings - Company information section
  - **XBRL Tag:** `dei:EntityAddressAddressLine1`, `dei:EntityAddressAddressLine2`, `dei:EntityAddressCityOrTown`, `dei:EntityAddressStateOrProvince`, `dei:EntityAddressPostalZipCode`
  - **HTML Location:** Cover page of 10-K/10-Q filings
  - **Example:** "One Microsoft Way, Redmond, WA 98052"
  - **Verified:** Found "ONE MICROSOFT WAY" in MSFT 10-K filing
  - **DEI Tags Found:** `dei:EntityAddressAddressLine1`, `dei:EntityAddressCityOrTown`, `dei:EntityAddressPostalZipCode`, `dei:EntityAddressStateOrProvince`

- **CITY, COUNTY:** City and county
  - **Source:** Parsed from address or separate fields
  - **XBRL Tag:** `dei:EntityAddressCityOrTown`, `dei:EntityAddressCountry`

**Business Description (2 fields):**
- **BUSDESC:** Business description
  - **Source:** SEC filings - Item 1 "Business" section of 10-K
  - **XBRL Tag:** Not typically in XBRL, extract from HTML/text
  - **Location:** First few paragraphs of Item 1 in 10-K filing

- **CONML:** Company legal name
  - **Source:** SEC filings - Cover page
  - **XBRL Tag:** `dei:EntityRegistrantName`
  - **Location:** Usually in filing header

**Status Fields (3 fields):**
- **COSTAT:** Company status code
  - **Source:** Compustat status codes (A=Active, I=Inactive)
  - **Mapping:** Active if current filings exist

- **DLDTE:** Delisting date
  - **Source:** SEC filings or delisting notices
  - **Location:** 8-K filings for delisting events

- **DLRSN:** Delisting reason
  - **Source:** SEC filings
  - **Location:** 8-K filings for delisting events

**Identifiers (2 fields):**
- **EIN:** Employer Identification Number
  - **Source:** SEC filings - Company information section
  - **XBRL Tag:** May be in company information
  - **Location:** Cover page or company information section

- **FIC:** Fitch Industry Code
  - **Source:** External mapping or Compustat assignment
  - **Note:** May need external data source

**Industry Classification (4 fields):**
- **GGROUP:** GICS Group (4-digit)
- **GIND:** GICS Industry (6-digit)
- **GSECTOR:** GICS Sector (2-digit)
- **GSUBIND:** GICS Sub-Industry (8-digit)
  - **Source:** SEC filings or SIC mapping
  - **XBRL Tag:** May be in company information
  - **Alternative:** Map from SIC code using Compustat mapping tables
  - **Location:** Company information section or industry classification

**Fiscal Year (1 field):**
- **FYRC:** Fiscal year end month
  - **Source:** SEC filings - Fiscal year end date
  - **XBRL Tag:** `dei:DocumentPeriodEndDate`
  - **Extraction:** Extract month from fiscal year end date

**Other Fields:**
- **FAX:** Fax number (rarely in filings)
- **SIC:** Standard Industrial Classification
  - **Source:** SEC filings
  - **XBRL Tag:** May be in company information

#### SECURITY Table Fields (15 total, 4 populated, 11 missing)

**Security Identifiers (3 fields):**
- **CUSIP:** CUSIP identifier
  - **Source:** SEC filings - Security information section
  - **XBRL Tag:** `dei:Security12Title` or security details
  - **Location:** Cover page or security information section
  - **Note:** May need to parse from security description

- **ISIN:** ISIN identifier
  - **Source:** SEC filings or external mapping
  - **XBRL Tag:** May be in security information
  - **Alternative:** Construct from CUSIP + country code

- **SEDOL:** SEDOL identifier
  - **Source:** External mapping (primarily UK securities)
  - **Note:** May not be available for US securities

**Exchange Information (2 fields):**
- **EXCHG:** Exchange code
  - **Source:** SEC filings or ticker mapping
  - **Mapping:** 
    - NASDAQ = 3
    - NYSE = 1
    - AMEX = 2
    - Other = various codes
  - **Location:** Trading information in filings

- **EXCNTRY:** Exchange country
  - **Source:** Usually "USA" for US-listed securities
  - **Default:** "USA" for US companies

**Status Fields (4 fields):**
- **DLDTEI:** Delisting date for issue
- **DLRSNI:** Delisting reason for issue
- **DSCI:** Description
- **SECSTAT:** Security status
  - **Source:** SEC filings - Security status changes
  - **Location:** 8-K filings for security changes

**Other Fields (2 fields):**
- **EPF:** Exchange price flag
- **IBTIC:** IBES ticker
  - **Source:** External mapping or IBES database

#### CSCO_IKEY Table Fields (16 total, 11 populated, 5 missing)

**Calendar Fields (5 fields):**
- **CQTR:** Calendar Quarter (1-4)
  - **Extraction:** Extract quarter from DATADATE
  - **Formula:** `(MONTH(DATADATE) - 1) / 3 + 1`

- **CYEARQ:** Calendar Year Quarterly
  - **Extraction:** Extract year from DATADATE
  - **Formula:** `YEAR(DATADATE)`

- **CURCDQ:** Currency Code Quarterly
  - **Source:** XBRL filings - unitRef attribute
  - **XBRL Location:** `unitRef` in nonFraction elements
  - **Common Values:** "USD", "EUR", etc.

- **FDATEQ:** Fiscal Date Quarterly
  - **Source:** SEC filings - Fiscal period end date
  - **XBRL Tag:** `dei:DocumentPeriodEndDate`
  - **Extraction:** Parse from XBRL context

- **RDQ:** Report Date Quarterly
  - **Source:** Filing date (already have as EFFDATE)
  - **Mapping:** Use EFFDATE value

#### CSCO_IFNDQ Table Fields (7 total, 4 populated, 3 missing)

**Metadata Fields (3 fields):**
- **DATACODE:** Data code
  - **Source:** Compustat data code system
  - **Default:** May be 0 or 1 (standard vs adjusted)
  - **Mapping:** Usually 0 for standard data

- **RST_TYPE:** Restatement type
  - **Source:** SEC filings - Restatement indicators
  - **XBRL Tag:** May be in context or metadata
  - **Values:** "R" = Restated, "N" = Not restated
  - **Default:** "N" if not specified

- **THRUDATE:** Through date
  - **Source:** XBRL context - period end date
  - **XBRL Location:** Context period end date
  - **Extraction:** Parse from contextRef in XBRL

**Additional Financial Items to Extract (50+ items):**

Based on Compustat structure, need to extract:
- **Operating Income:** OIADPQ
- **EBITDA:** OIBDPQ
- **Current Assets:** ACTQ
- **Current Liabilities:** LCTQ
- **Long-term Debt:** DLTTQ
- **Short-term Debt:** DLCQ
- **PPE Net:** PPENTQ
- **SGA Expense:** XSGAQ
- **Interest Expense:** XINTQ
- **Cost of Revenue:** COGSQ
- **Pretax Income:** PIQ
- **R&D Expense:** XRDQ
- **Tax:** TXTQ
- **Book Income:** IBQ
- **Deferred Taxes:** TXDITCQ
- **Preferred Stock:** PSTKQ
- **Minority Interest:** MIIQ
- **Receivables:** RECTQ
- **Inventory:** INVTQ
- **Depreciation:** DPQ
- **And 30+ more items...**

### 3. XBRL Tag Mapping

**Common us-gaap Tags to Compustat Items:**

| Compustat Item | XBRL Tag(s) | Notes |
|----------------|-------------|-------|
| REVTQ | RevenueFromContractWithCustomerExcludingAssessedTax, Revenues, SalesRevenueNet | Revenue |
| ATQ | Assets | Total Assets |
| ACTQ | AssetsCurrent | Current Assets |
| LCTQ | LiabilitiesCurrent | Current Liabilities |
| LTQ | Liabilities | Total Liabilities |
| CEQQ | StockholdersEquity, Equity | Common Equity |
| NIQ | NetIncomeLoss | Net Income |
| CHEQ | CashAndCashEquivalentsAtCarryingValue, Cash | Cash |
| OIADPQ | OperatingIncomeLoss | Operating Income |
| OIBDPQ | OperatingIncomeLoss + DepreciationAmortization | EBITDA (calculated) |
| DLTTQ | LongTermDebtAndCapitalLeaseObligations, LongTermDebt | Long-term Debt |
| DLCQ | DebtCurrent, ShortTermBorrowings | Short-term Debt |
| PPENTQ | PropertyPlantAndEquipmentNet | PPE Net |
| XSGAQ | SellingGeneralAndAdministrativeExpense | SGA Expense |
| XINTQ | InterestExpenseDebt | Interest Expense |
| COGSQ | CostOfGoodsAndServicesSold, CostOfRevenue | Cost of Revenue |
| PIQ | IncomeLossFromContinuingOperationsBeforeIncomeTaxes | Pretax Income |
| XRDQ | ResearchAndDevelopmentExpense | R&D Expense |
| TXTQ | IncomeTaxExpenseBenefit | Tax Expense |
| IBQ | IncomeLossFromContinuingOperations | Book Income |

**Additional Items Found in Compustat for MSFT/NVDA:**
- XOPRQ (510 records) - Operating expenses
- XSGAQ (475 records) - SGA expense
- COGSQ (463 records) - Cost of goods sold
- DPQ (449 records) - Depreciation
- CSHOQ (399 records) - Common shares outstanding
- LOQ (394 records) - Liabilities other
- NOPIQ (394 records) - Net operating income
- OIADPQ (390 records) - Operating income
- CSH12Q (387 records) - Common shares 12-month
- LCOQ (386 records) - Liabilities current other
- TXDITCQ (385 records) - Deferred taxes
- XINTQ (383 records) - Interest expense
- OIBDPQ (373 records) - EBITDA
- TXPQ (362 records) - Tax payable
- OEPS12 (358 records) - Operating EPS 12-month
- SPIQ (356 records) - Sales per share
- OPEPSQ (353 records) - Operating EPS
- DLCQ (347 records) - Debt current
- CAPSQ (340 records) - Capital stock
- IBADJQ (338 records) - Income before adj
- CSTKQ (336 records) - Common stock
- IBCOMQ (335 records) - Income before common
- IBQ (332 records) - Income before
- NIQ (332 records) - Net income
- TXTQ (330 records) - Tax total
- EPSFIQ (329 records) - EPS fully diluted
- REVTQ (328 records) - Revenue
- SALEQ (328 records) - Sales
- EPSFXQ (328 records) - EPS fully diluted
- EPSX12 (327 records) - EPS 12-month
- CSHPRQ (326 records) - Common shares price
- DLTTQ (326 records) - Debt long-term
- EPSPIQ (326 records) - EPS primary
- ICAPTQ (325 records) - Invested capital
- EPSPXQ (324 records) - EPS primary
- PIQ (324 records) - Pretax income
- XRDQ (321 records) - R&D expense
- CEQQ (318 records) - Common equity
- OEPF12 (314 records) - Operating EPS fully diluted 12-month
- TXDIQ (314 records) - Tax deferred income
- MIIQ (311 records) - Minority interest
- DVPQ (304 records) - Dividends paid
- CSTKEQ (304 records) - Common stock equity
- XIDOQ (302 records) - Interest/dividend other
- XIQ (302 records) - Interest expense
- DOQ (301 records) - Depreciation other
- LTMIBQ (299 records) - Liabilities total minus income before
- LTQ (299 records) - Liabilities total
- SEQQ (299 records) - Shareholders equity
- OEPSXQ (299 records) - Operating EPS primary

### 4. HTML/Text Extraction Patterns

**Company Address:**
- Pattern: Look for "Principal Executive Offices" or "Company Address"
- Location: Cover page of 10-K/10-Q
- Format: Usually in structured format with street, city, state, zip

**Business Description:**
- Pattern: Item 1 "Business" section
- Location: First major section after cover page in 10-K
- Extraction: First 2-3 paragraphs of Item 1

**Security Identifiers:**
- CUSIP: Usually in format "XXXXX-XXX" or "XXXXX XXX"
- ISIN: Format "US" + CUSIP + check digit
- SEDOL: 7-character alphanumeric (UK securities)

**Exchange Information:**
- Pattern: "Trading Symbol" or "Listed on"
- Location: Cover page
- Values: NASDAQ, NYSE, NYSE American, etc.

### 5. Table-Specific Extraction Guides

#### CSCO_ITXT Table
- **Purpose:** Text fields from filings
- **Key Fields:** COIFND_ID, ITEM, VALUE
- **Items:** UPDQ (update date), BUSDESC (business description), etc.
- **Source:** Extract text sections from filings

#### SEC_DPRC Table
- **Purpose:** Daily price data
- **Key Fields:** GVKEY, IID, DATADATE, PRCCD, etc.
- **Source:** External price data (not in SEC filings)
- **Note:** May need to use external API or price database

#### SEC_DIVID Table
- **Purpose:** Dividend information
- **Key Fields:** GVKEY, IID, DIVD, DIVDT, etc.
- **Source:** SEC filings - Dividend declarations
- **Location:** 8-K filings or proxy statements
- **XBRL Tag:** May be in dividend-related tags

### 6. Implementation Notes

**Parser Enhancements Needed:**
1. Extract all XBRL tags (not just financial)
2. Parse XBRL contexts for period dates
3. Extract unitRef for currency
4. Handle multiple contexts/periods
5. Extract text sections (business description)
6. Parse structured data (addresses)
7. Extract security identifiers

**Mapping Challenges:**
1. **Unit Conversion:** XBRL may be in millions, thousands, or actuals
2. **Period Alignment:** Fiscal vs calendar periods
3. **Restatements:** Handle restated vs original data
4. **Multiple Periods:** Some filings have multiple periods
5. **Missing Data:** Not all fields available in all filings

**Validation Approach:**
1. Compare field-by-field with compustat.duckdb
2. Check data types match
3. Validate ranges (e.g., dates, amounts)
4. Check for null vs missing vs zero
5. Verify relationships (e.g., GVKEY consistency)

### 7. Resources & Tools

**XBRL Parsing:**
- Use `xml.etree.ElementTree` for XBRL parsing
- Handle namespaces properly (`us-gaap:`, `dei:`, etc.)
- Parse contextRef for period information
- Parse unitRef for currency/units

**HTML Parsing:**
- Use BeautifulSoup with lxml parser
- Extract structured sections (addresses, business description)
- Parse tables for financial data
- Handle embedded XBRL in HTML

**Text Extraction:**
- Use regex patterns for structured data
- Extract sections by headers (Item 1, Item 2, etc.)
- Parse addresses, dates, identifiers

**External Data Sources:**
- Price data: May need external API
- Exchange codes: Mapping table
- Industry classification: SIC to GICS mapping
- Security identifiers: CUSIP/ISIN lookup services

### 8. Next Steps

1. **Enhance XBRL Parser:**
   - Extract all us-gaap tags
   - Parse contexts for dates
   - Extract unitRef for currency
   - Handle multiple periods

2. **Enhance HTML Parser:**
   - Extract company address
   - Extract business description
   - Extract security identifiers
   - Extract exchange information

3. **Create Field Mappers:**
   - COMPANY mapper (36 fields)
   - SECURITY mapper (11 fields)
   - CSCO_IKEY mapper (5 fields)
   - CSCO_IFNDQ mapper (3 fields + 50+ items)

4. **Create Table Builders:**
   - CSCO_ITXT builder
   - SEC_DPRC builder (may need external data)
   - SEC_DIVID builder

5. **Validate & Iterate:**
   - Compare with compustat.duckdb
   - Fix discrepancies
   - Achieve 100% match

