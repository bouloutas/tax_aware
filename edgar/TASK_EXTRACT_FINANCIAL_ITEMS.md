# Task: Extract Financial Items from SEC Filings

**Purpose:** Extract comprehensive financial data from SEC filings and map to Compustat items  
**Status:** 🔄 In Progress (25/50+ items extracted)  
**Last Updated:** November 18, 2025

## Overview

Extract all financial statement items from SEC filings (XBRL, HTML, Text) and map to Compustat item codes. Target: 50+ financial items per filing.

## Current Status

**Extracted:** 25 unique items  
**Target:** 50+ items (Compustat has 294 items for MSFT)

**Currently Extracting:**
- REVTQ, SALEQ, ATQ, ACTQ, LTQ, LCTQ, CEQQ, NIQ, CHEQ
- EPSPXQ, EPSPIQ, CSHPRQ
- COGSQ, XSGAQ, XINTQ, XRDQ, DPQ
- DLTTQ, DLCQ, PIQ, OIADPQ, TXTQ
- PPENTQ, RECTQ, INVTQ

## Process Steps

### 1. XBRL Tag Extraction

**File:** `src/filing_parser.py` - `XBRLParser._extract_first_numeric()`

**Method:**
1. Try direct element names (e.g., `Revenues`)
2. Try namespaced tags (e.g., `us-gaap:Revenues`)
3. Search in HTML-embedded XBRL (`ix:nonFraction`, `ix:nonNumeric`)
4. Parse context and unit information

**Code:**
```python
financial_tags = {
    # Income Statement
    'revenue': ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax'],
    'sales': ['SalesRevenueNet', 'SalesRevenueServicesNet'],
    'cost_of_revenue': ['CostOfGoodsAndServicesSold', 'CostOfRevenue'],
    'operating_income': ['OperatingIncomeLoss', 'IncomeLossFromOperations'],
    'net_income': ['NetIncomeLoss', 'ProfitLoss'],
    'pretax_income': ['IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'],
    'tax_expense': ['IncomeTaxExpenseBenefit', 'ProvisionForIncomeTaxes'],
    
    # Expenses
    'sga_expense': ['SellingGeneralAndAdministrativeExpense'],
    'rd_expense': ['ResearchAndDevelopmentExpense'],
    'interest_expense': ['InterestExpenseDebt', 'InterestExpense'],
    'depreciation': ['DepreciationDepletionAndAmortization'],
    
    # Balance Sheet - Assets
    'assets': ['Assets', 'AssetsCurrent', 'AssetsNoncurrent'],
    'current_assets': ['AssetsCurrent'],
    'cash': ['CashAndCashEquivalentsAtCarryingValue', 'Cash'],
    'receivables': ['AccountsReceivableNetCurrent', 'AccountsReceivableNet'],
    'inventory': ['InventoryNet', 'Inventory'],
    'ppe_net': ['PropertyPlantAndEquipmentNet'],
    'goodwill': ['Goodwill'],
    'intangible_assets': ['IntangibleAssetsNetExcludingGoodwill'],
    
    # Balance Sheet - Liabilities
    'liabilities': ['Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent'],
    'current_liabilities': ['LiabilitiesCurrent'],
    'accounts_payable': ['AccountsPayableCurrent', 'AccountsPayable'],
    'short_term_debt': ['DebtCurrent', 'ShortTermBorrowings'],
    'long_term_debt': ['LongTermDebtAndCapitalLeaseObligations', 'LongTermDebt'],
    
    # Balance Sheet - Equity
    'equity': ['StockholdersEquity', 'Equity'],
    'common_equity': ['StockholdersEquity', 'CommonStockholdersEquity'],
    'preferred_stock': ['PreferredStockValue', 'PreferredStock'],
    'minority_interest': ['NoncontrollingInterest', 'MinorityInterest'],
    
    # Shares and EPS
    'shares_outstanding': ['WeightedAverageNumberOfSharesOutstandingBasic'],
    'shares_basic': ['WeightedAverageNumberOfSharesOutstandingBasic'],
    'shares_diluted': ['WeightedAverageNumberOfSharesOutstandingDiluted'],
    'eps_basic': ['EarningsPerShareBasic', 'EarningsPerShare'],
    'eps_diluted': ['EarningsPerShareDiluted'],
    
    # Cash Flow
    'operating_cash_flow': ['NetCashProvidedByUsedInOperatingActivities'],
    'investing_cash_flow': ['NetCashProvidedByUsedInInvestingActivities'],
    'financing_cash_flow': ['NetCashProvidedByUsedInFinancingActivities'],
    'capex': ['CapitalExpenditures'],
}
```

### 2. Compustat Item Mapping

**File:** `src/financial_mapper.py` - `COMPUSTAT_ITEM_MAPPING` dictionary

**Mapping Table:**

| Extracted Field | Compustat Item | Description |
|----------------|----------------|-------------|
| revenue | REVTQ | Revenue (quarterly) |
| sales | SALEQ | Sales (quarterly) |
| cost_of_revenue | COGSQ | Cost of Goods Sold |
| operating_income | OIADPQ | Operating Income |
| net_income | NIQ | Net Income |
| pretax_income | PIQ | Pretax Income |
| tax_expense | TXTQ | Tax Expense |
| sga_expense | XSGAQ | SGA Expense |
| rd_expense | XRDQ | R&D Expense |
| interest_expense | XINTQ | Interest Expense |
| depreciation | DPQ | Depreciation |
| assets | ATQ | Total Assets |
| current_assets | ACTQ | Current Assets |
| cash | CHEQ | Cash and Equivalents |
| receivables | RECTQ | Receivables |
| inventory | INVTQ | Inventory |
| ppe_net | PPENTQ | PPE Net |
| liabilities | LTQ | Total Liabilities |
| current_liabilities | LCTQ | Current Liabilities |
| short_term_debt | DLCQ | Short-term Debt |
| long_term_debt | DLTTQ | Long-term Debt |
| equity | CEQQ | Common Equity |
| preferred_stock | PSTKQ | Preferred Stock |
| minority_interest | MIIQ | Minority Interest |
| shares_outstanding | CSHPRQ | Shares Outstanding |
| eps_basic | EPSPXQ | EPS Basic |
| eps_diluted | EPSPIQ | EPS Diluted |

### 3. Additional Items to Extract

**From Compustat Analysis (MSFT/NVDA have 294 items):**

**Operating Items:**
- OIBDPQ - EBITDA (calculate: Operating Income + Depreciation)
- OEPS12 - Operating EPS 12-month
- OPEPSQ - Operating EPS
- OEPSXQ - Operating EPS Primary
- OEPF12 - Operating EPS Fully Diluted 12-month

**Expense Items:**
- XOPRQ - Operating Expenses
- XIDOQ - Interest/Dividend Other
- XIQ - Interest Expense (alternative)

**Asset Items:**
- CSHOQ - Common Shares Outstanding
- CSH12Q - Common Shares 12-month

**Liability Items:**
- LOQ - Liabilities Other
- LCOQ - Liabilities Current Other

**Income Items:**
- IBQ - Income Before
- IBCOMQ - Income Before Common
- IBADJQ - Income Before Adjusted
- NOPIQ - Net Operating Income

**Tax Items:**
- TXPQ - Tax Payable
- TXDIQ - Tax Deferred Income
- TXDITCQ - Deferred Taxes

**Share Items:**
- CSHPRQ - Common Shares Price
- CSTKQ - Common Stock
- CSTKEQ - Common Stock Equity
- CAPSQ - Capital Stock

**EPS Items:**
- EPSFIQ - EPS Fully Diluted
- EPSFXQ - EPS Fully Diluted
- EPSX12 - EPS 12-month

**Other Items:**
- DOQ - Depreciation Other
- DVPQ - Dividends Paid
- ICAPTQ - Invested Capital
- SPIQ - Sales Per Share
- LTMIBQ - Liabilities Total Minus Income Before
- SEQQ - Shareholders Equity

### 4. Extraction Strategy

**Step 1: Extract All us-gaap Tags**
- Parse XBRL to find all `us-gaap:*` tags
- Extract values, contexts, and units
- Store in indexed structure

**Step 2: Map to Compustat Items**
- Use mapping table to convert XBRL tags to Compustat items
- Handle multiple tag variants
- Prioritize most common tags

**Step 3: Handle Calculated Items**
- EBITDA = Operating Income + Depreciation
- Total Debt = Short-term Debt + Long-term Debt
- Gross Profit = Revenue - COGS

**Step 4: Validate and Store**
- Check for reasonable values (not zero, not negative for assets)
- Handle unit conversions (millions, thousands, actuals)
- Store in CSCO_IFNDQ table

### 5. Code Example

```python
# Extract all us-gaap tags
all_tags = extract_all_us_gaap_tags(xbrl_content)

# Map to Compustat items
for tag, value in all_tags.items():
    compustat_item = map_xbrl_to_compustat(tag)
    if compustat_item:
        financial_data[compustat_item] = value

# Handle calculated items
if 'OIADPQ' in financial_data and 'DPQ' in financial_data:
    financial_data['OIBDPQ'] = financial_data['OIADPQ'] + financial_data['DPQ']
```

### 6. Scaling to All Companies

**Process:**
1. Extract all us-gaap tags from each filing
2. Map to Compustat items using comprehensive mapping table
3. Handle company-specific tag variations
4. Calculate derived items where needed
5. Store all extracted items in CSCO_IFNDQ table

**Tag Variations:**
- Companies may use different tag names for same concept
- Maintain comprehensive tag variant list
- Update mapping table as new tags discovered

### 7. Dependencies

- XBRL parser (`src/filing_parser.py`)
- Financial mapper (`src/financial_mapper.py`)
- Mapping table (`COMPUSTAT_ITEM_MAPPING`)

### 8. Output Files

- Financial data: Stored in `CSCO_IFNDQ` table
- Mapping log: `logs/financial_extraction_{timestamp}.log`
- Item report: `logs/extracted_items_{timestamp}.csv`

## Notes

- Extract all available tags, not just common ones
- Handle tag name variations across companies
- Calculate derived items when components available
- Validate values before storing
- Log extraction failures for review

## Next Steps

1. Expand tag extraction to capture all us-gaap tags
2. Add calculated items (EBITDA, etc.)
3. Handle tag variations across companies
4. Extract cash flow statement items
5. Extract segment data items

