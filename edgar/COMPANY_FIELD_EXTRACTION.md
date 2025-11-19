# COMPANY Table Field Extraction Guide

**Purpose:** Document extraction methods for all COMPANY table fields  
**Last Updated:** November 18, 2025

## Field Extraction Status

### ✅ Populated Fields (14 fields)

| Field | Source | Extraction Method | Status |
|-------|--------|------------------|--------|
| GVKEY | CIK mapping | Lookup from `cik_to_gvkey_mapping.csv` | ✅ |
| CIK | SEC filing header | Extract from filing header | ✅ |
| CONM | SEC filing header | Extract company name | ✅ |
| CONML | XBRL `dei:EntityRegistrantName` | Extract legal name | ✅ |
| ADD1 | XBRL `dei:EntityAddressAddressLine1` | Extract address line 1 | ✅ |
| ADD2 | XBRL `dei:EntityAddressAddressLine2` | Extract address line 2 | ⏳ |
| CITY | XBRL `dei:EntityAddressCityOrTown` | Extract city | ✅ |
| STATE | XBRL `dei:EntityAddressStateOrProvince` | Extract state | ⚠️ |
| ADDZIP | XBRL `dei:EntityAddressPostalZipCode` | Extract ZIP code | ✅ |
| FYRC | XBRL `dei:DocumentPeriodEndDate` | Extract month (1-12) | ✅ |
| SIC | HTML cover page | Regex: `SIC[:\s]+(\d{4})` | ⏳ |
| PHONE | HTML cover page | Regex: `(\d{3})[-\s](\d{3})[-\s](\d{4})` | ⏳ |
| WEBURL | HTML cover page | Regex: `www\.([a-zA-Z0-9\-\.]+)` | ⏳ |
| EIN | HTML cover page | Regex: `(\d{2}-\d{7})` | ⏳ |
| BUSDESC | HTML Item 1 section | Extract first 2-3 paragraphs | ⏳ |

### ⏳ Remaining Fields (24 fields)

#### Address Fields (3 fields)
- **ADD3, ADD4:** Additional address lines (optional, rarely used)
- **COUNTY:** County name (parse from address or separate field)

#### Status Fields (3 fields)
- **COSTAT:** Company status (A=Active, I=Inactive) - Set to 'A' if current filings exist
- **DLDTE:** Delisting date - Extract from 8-K delisting notices
- **DLRSN:** Delisting reason - Extract from 8-K delisting notices

#### Identifiers (1 field)
- **FIC:** Fitch Industry Code - External mapping or Compustat assignment

#### Industry Classification (4 fields)
- **GGROUP:** GICS Group (4-digit) - Map from SIC using Compustat mapping
- **GIND:** GICS Industry (6-digit) - Map from SIC using Compustat mapping
- **GSECTOR:** GICS Sector (2-digit) - Map from SIC using Compustat mapping
- **GSUBIND:** GICS Sub-Industry (8-digit) - Map from SIC using Compustat mapping

#### Other Fields (13 fields)
- **FAX:** Fax number - Extract from company info section
- **INCORP:** State of incorporation - Extract from filing
- **LOC:** Location code - Default to 'USA'
- **NAICS:** NAICS code - Extract from company info section
- **IPODATE:** IPO date - Historical data extraction
- **PRICAN, PRIROW, PRIUSA:** Priority flags - Compustat assignment
- **SPCINDCD, SPCSECCD:** S&P industry/sector codes - External mapping
- **SPCSRC:** S&P source - Compustat assignment
- **STKO:** Stock option flag - Compustat assignment
- **IDBFLAG:** IDB flag - Compustat assignment

## Extraction Methods

### SIC Code Extraction

**Pattern:** `SIC[:\s]+(\d{4})`

**Location:** Cover page of 10-K/10-Q filings

**Code:**
```python
sic_patterns = [
    r'SIC[:\s]+(\d{4})',
    r'Standard Industrial Classification[:\s]+(\d{4})',
    r'Industry Classification Code[:\s]+(\d{4})',
]
for pattern in sic_patterns:
    match = re.search(pattern, content, re.IGNORECASE)
    if match:
        sic = match.group(1)
        break
```

### Phone Number Extraction

**Pattern:** `(\d{3})[-\s](\d{3})[-\s](\d{4})`

**Location:** Cover page of 10-K/10-Q filings

**Code:**
```python
phone_patterns = [
    r'Phone[:\s]+([\d\s\-\(\)]+)',
    r'Telephone[:\s]+([\d\s\-\(\)]+)',
    r'\((\d{3})\)\s*(\d{3})-(\d{4})',
    r'(\d{3})[-\s](\d{3})[-\s](\d{4})',
]
for pattern in phone_patterns:
    matches = re.findall(pattern, content, re.IGNORECASE)
    if matches:
        if isinstance(matches[0], tuple):
            phone = ''.join(matches[0])
        else:
            phone = re.sub(r'[^\d]', '', matches[0])
        if len(phone) == 10:
            phone_number = phone
            break
```

### Website URL Extraction

**Pattern:** `www\.([a-zA-Z0-9\-\.]+)`

**Location:** Cover page of 10-K/10-Q filings

**Code:**
```python
website_patterns = [
    r'www\.([a-zA-Z0-9\-\.]+)',
    r'http[s]?://([a-zA-Z0-9\-\.]+)',
]
for pattern in website_patterns:
    matches = re.findall(pattern, content, re.IGNORECASE)
    if matches:
        # Filter out third-party domains
        for match in matches:
            domain = match.lower()
            if not any(x in domain for x in ['sec.gov', 'edgar', 'xbrl', 'dfinsolutions']):
                website = f'www.{domain}'
                break
```

### EIN Extraction

**Pattern:** `(\d{2}-\d{7})`

**Location:** Cover page of 10-K/10-Q filings

**Code:**
```python
ein_patterns = [
    r'EIN[:\s]+(\d{2}-\d{7})',
    r'Employer Identification Number[:\s]+(\d{2}-\d{7})',
    r'Federal Tax ID[:\s]+(\d{2}-\d{7})',
    r'Tax ID[:\s]+(\d{2}-\d{7})',
]
for pattern in ein_patterns:
    match = re.search(pattern, content, re.IGNORECASE)
    if match:
        ein = match.group(1)
        break
```

### Business Description Extraction

**Location:** Item 1 "Business" section of 10-K filings

**Code:**
```python
# Find Item 1 Business section
item1_match = re.search(r'Item\s+1[\.\s]+Business(.*?)(?=Item\s+2|$)', content, re.IGNORECASE | re.DOTALL)
if item1_match:
    item1_content = item1_match.group(1)
    # Extract first few paragraphs (500-1000 chars)
    paragraphs = re.split(r'\n\s*\n', item1_content)
    business_desc = ' '.join(paragraphs[:3])[:1000].strip()
    # Clean up HTML tags and entities
    business_desc = re.sub(r'<[^>]+>', ' ', business_desc)
    business_desc = re.sub(r'&#\d+;', ' ', business_desc)
    business_desc = re.sub(r'\s+', ' ', business_desc).strip()
```

## SIC to GICS Mapping

**Source:** Compustat mapping tables or external GICS mapping

**Method:**
1. Extract SIC code from filing
2. Look up GICS codes in mapping table
3. Populate GGROUP, GIND, GSECTOR, GSUBIND

**Note:** May need to create or obtain SIC-to-GICS mapping table.

## Implementation Status

**Current Implementation:**
- ✅ XBRL extraction for address, legal name, fiscal year
- ✅ HTML extraction for address, business description
- ⏳ HTML extraction for SIC, phone, website, EIN (patterns added, needs testing)

**Next Steps:**
1. Test SIC, phone, website, EIN extraction on actual filings
2. Refine patterns based on actual filing formats
3. Extract business description from Item 1 section
4. Create SIC-to-GICS mapping table
5. Extract remaining fields (FAX, INCORP, NAICS, etc.)

## Notes

- SIC codes are typically 4 digits
- Phone numbers are typically 10 digits (US format)
- Website URLs should exclude third-party domains (sec.gov, edgar, etc.)
- EIN format is XX-XXXXXXX (2 digits, dash, 7 digits)
- Business description should be 500-1000 characters from Item 1

