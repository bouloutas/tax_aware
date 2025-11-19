# Task: Parse SEC Filings

**Purpose:** Parse downloaded SEC filings and extract structured data  
**Status:** ✅ Complete (with enhancements)  
**Last Updated:** November 18, 2025

## Overview

Parse SEC filings (XBRL, HTML, Text) and extract:
- Company metadata (address, fiscal year, legal name)
- Security identifiers (ticker, CUSIP, ISIN)
- Financial data (income statement, balance sheet, cash flow)
- Filing metadata (dates, types, periods)

## Process Steps

### 1. Filing Format Detection

**Supported Formats:**
- XBRL (preferred) - Structured XML with us-gaap tags
- HTML - Semi-structured with embedded XBRL
- Text - Plain text fallback

**Detection Logic:**
```python
def get_parser(filing_path):
    if '<XBRL>' in content or 'xmlns="http://www.xbrl.org' in content:
        return XBRLParser(filing_path)
    elif '<HTML>' in content or '<html>' in content:
        return HTMLParser(filing_path)
    else:
        return TextParser(filing_path)
```

### 2. XBRL Parser

**File:** `src/filing_parser.py` - `XBRLParser` class

**Extraction Methods:**

#### Company Metadata
- **Address:** `dei:EntityAddressAddressLine1`, `dei:EntityAddressCityOrTown`, etc.
- **Fiscal Year:** `dei:DocumentPeriodEndDate` → extract month
- **Legal Name:** `dei:EntityRegistrantName`
- **Currency:** Extract from `unitRef` in XBRL units

**Code:**
```python
def _extract_company_metadata(self, data):
    # Extract address fields
    address_line1 = self._extract_text_value([
        'EntityAddressAddressLine1',
        'dei:EntityAddressAddressLine1'
    ])
    # ... extract other fields
```

#### Financial Data
- **Tags:** us-gaap:* tags (e.g., `us-gaap:Revenues`, `us-gaap:Assets`)
- **Context:** Parse `contextRef` for period information
- **Units:** Parse `unitRef` for currency and scale (millions, thousands, actuals)

**Financial Tags Extracted:**
- Income Statement: Revenue, Sales, COGS, Operating Income, Net Income, Tax
- Expenses: SGA, R&D, Interest, Depreciation
- Balance Sheet: Assets, Liabilities, Equity, Cash, Debt
- Shares & EPS: Shares Outstanding, EPS Basic, EPS Diluted

**Code:**
```python
financial_tags = {
    'revenue': ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax'],
    'assets': ['Assets', 'AssetsCurrent'],
    # ... 30+ more tags
}
```

#### Security Data
- **Ticker:** `dei:TradingSymbol`
- **CUSIP:** Extract from security information section
- **ISIN:** Construct from CUSIP or extract

### 3. HTML Parser

**File:** `src/filing_parser.py` - `HTMLParser` class

**Extraction Methods:**

#### Address Extraction
- Pattern: "Principal Executive Offices" or "Company Address"
- Parse: Street, City, State, ZIP from cover page

#### Business Description
- Location: Item 1 "Business" section
- Extract: First 2-3 paragraphs (500-1000 chars)

#### Financial Tables
- Parse HTML tables for financial data
- Handle unit conversions (millions, thousands)
- Map table labels to financial items

### 4. Text Parser

**File:** `src/filing_parser.py` - `TextParser` class

**Extraction Methods:**
- Regex patterns for common fields
- Pattern matching for financial data
- Fallback when XBRL/HTML not available

### 5. Output Structure

**Parsed Data Dictionary:**
```python
{
    'cik': '789019',
    'company_name': 'MICROSOFT CORP',
    'filing_date': date(2024, 10, 30),
    'filing_type': '10-K',
    'financial_data': {
        'revenue': 211915000000.0,
        'assets': 470558000000.0,
        # ... more items
    },
    'company_metadata': {
        'address_line1': 'ONE MICROSOFT WAY',
        'city': 'REDMOND',
        'state': 'WA',
        'zip_code': '98052-6399',
        'fiscal_year_end_month': 6,
        'legal_name': 'MICROSOFT CORPORATION',
        'currency': 'USD'
    },
    'security_data': {
        'ticker': 'MSFT'
    },
    'document_period_end_date': '2024-06-30'
}
```

### 6. Parsing Challenges & Solutions

#### Challenge 1: HTML-Embedded XBRL
**Problem:** XBRL tags embedded in HTML (`ix:nonFraction`, `ix:nonNumeric`)  
**Solution:** Regex patterns to extract content from HTML-embedded tags

#### Challenge 2: Multiple Namespaces
**Problem:** XBRL uses various namespaces (`us-gaap:`, `dei:`, etc.)  
**Solution:** Try multiple namespace variants, handle both prefixed and unprefixed tags

#### Challenge 3: Unit Conversion
**Problem:** Values may be in millions, thousands, or actuals  
**Solution:** Parse `unitRef` to determine scale, normalize to actuals

#### Challenge 4: Date Parsing
**Problem:** Dates in various formats ("June 30, 2024", "2024-06-30", etc.)  
**Solution:** Try multiple date formats, clean HTML entities first

#### Challenge 5: Text Cleaning
**Problem:** HTML entities (`&#160;`), tags, extra whitespace  
**Solution:** Regex patterns to remove HTML, normalize whitespace

### 7. Code Example

```python
from src.filing_parser import get_parser
from pathlib import Path

filing_path = Path('data/raw/2024/Q3/789019/10-K_0000950170-24-087843.txt')
parser = get_parser(filing_path)

if parser.load():
    data = parser.parse()
    # data contains extracted information
```

### 8. Scaling to All Companies

**Process:**
1. Iterate through all downloaded filings
2. Detect format and use appropriate parser
3. Extract all available data
4. Handle parsing errors gracefully
5. Log extraction results

**Error Handling:**
- Log parsing failures
- Continue processing other filings
- Generate report of failed extractions

### 9. Dependencies

- `xml.etree.ElementTree` - XBRL parsing
- `BeautifulSoup` (lxml parser) - HTML parsing
- `re` - Regex patterns
- `datetime` - Date parsing

### 10. Output Files

- Parsed data: Stored in memory, passed to data extractor
- Logs: `logs/parse_{timestamp}.log`
- Error reports: `logs/parse_errors_{timestamp}.csv`

## Notes

- XBRL format preferred (most structured)
- HTML format common (embedded XBRL)
- Text format fallback (least reliable)
- Always handle missing data gracefully
- Log extraction provenance for debugging

