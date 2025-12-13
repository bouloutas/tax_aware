"""
Parse SEC filings (XBRL, HTML, text) and extract data.
"""
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Iterable, Tuple
from datetime import date
from collections import defaultdict
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


STATE_ABBREVIATIONS = {
    'ALABAMA': 'AL',
    'ALASKA': 'AK',
    'ARIZONA': 'AZ',
    'ARKANSAS': 'AR',
    'CALIFORNIA': 'CA',
    'COLORADO': 'CO',
    'CONNECTICUT': 'CT',
    'DELAWARE': 'DE',
    'DISTRICT OF COLUMBIA': 'DC',
    'FLORIDA': 'FL',
    'GEORGIA': 'GA',
    'HAWAII': 'HI',
    'IDAHO': 'ID',
    'ILLINOIS': 'IL',
    'INDIANA': 'IN',
    'IOWA': 'IA',
    'KANSAS': 'KS',
    'KENTUCKY': 'KY',
    'LOUISIANA': 'LA',
    'MAINE': 'ME',
    'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA',
    'MICHIGAN': 'MI',
    'MINNESOTA': 'MN',
    'MISSISSIPPI': 'MS',
    'MISSOURI': 'MO',
    'MONTANA': 'MT',
    'NEBRASKA': 'NE',
    'NEVADA': 'NV',
    'NEW HAMPSHIRE': 'NH',
    'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM',
    'NEW YORK': 'NY',
    'NORTH CAROLINA': 'NC',
    'NORTH DAKOTA': 'ND',
    'OHIO': 'OH',
    'OKLAHOMA': 'OK',
    'OREGON': 'OR',
    'PENNSYLVANIA': 'PA',
    'RHODE ISLAND': 'RI',
    'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD',
    'TENNESSEE': 'TN',
    'TEXAS': 'TX',
    'UTAH': 'UT',
    'VERMONT': 'VT',
    'VIRGINIA': 'VA',
    'WASHINGTON': 'WA',
    'WEST VIRGINIA': 'WV',
    'WISCONSIN': 'WI',
    'WYOMING': 'WY',
}


class FilingParser:
    """Base class for parsing SEC filings."""
    
    def __init__(self, filing_path: Path):
        """
        Initialize parser with filing path.
        
        Args:
            filing_path: Path to filing file
        """
        self.filing_path = filing_path
        self.content = None
        self.filing_type = None
        self.cik = None
        self.company_name = None
        self.filing_date = None
        
    def load(self):
        """Load filing content from file."""
        try:
            with open(self.filing_path, 'r', encoding='utf-8', errors='ignore') as f:
                self.content = f.read()
            self._extract_metadata()
            return True
        except Exception as e:
            logger.error(f"Error loading filing {self.filing_path}: {e}")
            return False
    
    def _extract_metadata(self):
        """Extract basic metadata from filing."""
        if not self.content:
            return
        
        # Extract CIK
        cik_match = re.search(r'CENTRAL INDEX KEY:\s*(\d+)', self.content, re.IGNORECASE)
        if cik_match:
            self.cik = cik_match.group(1).lstrip('0') or '0'
        
        # Extract company name
        name_match = re.search(r'COMPANY CONFORMED NAME:\s*(.+?)\n', self.content, re.IGNORECASE)
        if name_match:
            self.company_name = name_match.group(1).strip()
        
        # Extract filing date
        date_match = re.search(r'FILED AS OF DATE:\s*(\d{8})', self.content, re.IGNORECASE)
        if date_match:
            date_str = date_match.group(1)
            try:
                self.filing_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            except ValueError:
                pass
        
        # Extract filing type (allow hyphenated identifiers like 10-K, 10-Q)
        type_match = re.search(r'<TYPE>([\w\-]+(?:/[\w\-]+)?)', self.content, re.IGNORECASE)
        if type_match:
            self.filing_type = type_match.group(1)
    
    def parse(self) -> Dict[str, Any]:
        """
        Parse filing and extract data.
        
        Returns:
            Dictionary with extracted data
        """
        raise NotImplementedError("Subclasses must implement parse()")
    
    def is_xbrl(self) -> bool:
        """Check if filing contains XBRL data."""
        if not self.content:
            return False
        return '<XBRL>' in self.content.upper() or 'xmlns="http://www.xbrl.org' in self.content


class XBRLParser(FilingParser):
    """Parser for XBRL filings."""

    def __init__(self, filing_path: Path):
        super().__init__(filing_path)
        self.xbrl_root = None
        self.context_periods = {}  # Map context ID -> {'type': 'INSTANT'|'DURATION', 'start': date, 'end': date, 'days': int}
    
    def load(self):
        """Load XBRL content."""
        if not super().load():
            return False
        
        # Try to find XBRL section
        xbrl_match = re.search(r'<XBRL>(.*?)</XBRL>', self.content, re.DOTALL | re.IGNORECASE)
        if not xbrl_match:
            # Try to find XML document
            xml_match = re.search(r'(<\?xml.*?</html>)', self.content, re.DOTALL)
            if xml_match:
                xbrl_content = xml_match.group(1)
            else:
                return False
        else:
            xbrl_content = xbrl_match.group(1)
        
        try:
            self.xbrl_root = ET.fromstring(xbrl_content.strip())
            self._index_elements()
            return True
        except ET.ParseError as e:
            logger.warning(f"Error parsing XBRL: {e}")
            return False

    def _index_elements(self) -> None:
        """Index elements by local name for quick lookup."""
        self.elements_by_local: Dict[str, List[ET.Element]] = defaultdict(list)
        for elem in self.xbrl_root.iter():
            local = self._local_name(elem.tag)
            self.elements_by_local[local].append(elem)
        # Also build context period index
        self._build_context_periods()

    def _build_context_periods(self) -> None:
        """Build a map of context IDs to their period information."""
        from datetime import datetime

        self.context_periods = {}

        # Try to find context elements using various patterns
        context_patterns = [
            './/{http://www.xbrl.org/2003/instance}context',
            './/context',
            './/{*}context',
        ]

        contexts = []
        for pattern in context_patterns:
            try:
                found = self.xbrl_root.findall(pattern)
                if found:
                    contexts = found
                    break
            except:
                continue

        for ctx in contexts:
            ctx_id = ctx.get('id', '')
            if not ctx_id:
                continue

            period_info = {'type': 'UNKNOWN', 'start': None, 'end': None, 'days': 0}

            # Find period element
            period_patterns = [
                './/{http://www.xbrl.org/2003/instance}period',
                './/period',
                './/{*}period',
            ]

            period_elem = None
            for pp in period_patterns:
                try:
                    period_elem = ctx.find(pp)
                    if period_elem is not None:
                        break
                except:
                    continue

            if period_elem is None:
                self.context_periods[ctx_id] = period_info
                continue

            # Check for instant or duration
            instant_patterns = [
                './/{http://www.xbrl.org/2003/instance}instant',
                './/instant',
                './/{*}instant',
            ]
            start_patterns = [
                './/{http://www.xbrl.org/2003/instance}startDate',
                './/startDate',
                './/{*}startDate',
            ]
            end_patterns = [
                './/{http://www.xbrl.org/2003/instance}endDate',
                './/endDate',
                './/{*}endDate',
            ]

            instant_elem = None
            for ip in instant_patterns:
                try:
                    instant_elem = period_elem.find(ip)
                    if instant_elem is not None:
                        break
                except:
                    continue

            if instant_elem is not None and instant_elem.text:
                # Point in time - balance sheet
                period_info['type'] = 'INSTANT'
                try:
                    period_info['end'] = datetime.strptime(instant_elem.text.strip(), '%Y-%m-%d').date()
                except:
                    pass
            else:
                # Duration - check start and end
                start_elem = None
                end_elem = None

                for sp in start_patterns:
                    try:
                        start_elem = period_elem.find(sp)
                        if start_elem is not None:
                            break
                    except:
                        continue

                for ep in end_patterns:
                    try:
                        end_elem = period_elem.find(ep)
                        if end_elem is not None:
                            break
                    except:
                        continue

                if start_elem is not None and start_elem.text and end_elem is not None and end_elem.text:
                    try:
                        start_date = datetime.strptime(start_elem.text.strip(), '%Y-%m-%d').date()
                        end_date = datetime.strptime(end_elem.text.strip(), '%Y-%m-%d').date()
                        days = (end_date - start_date).days

                        period_info['type'] = 'DURATION'
                        period_info['start'] = start_date
                        period_info['end'] = end_date
                        period_info['days'] = days
                    except:
                        pass

            self.context_periods[ctx_id] = period_info

        # Also parse from raw content for inline XBRL
        if self.content and len(self.context_periods) < 10:
            self._build_context_periods_from_raw()

    def _build_context_periods_from_raw(self) -> None:
        """Parse context periods from raw content (for inline XBRL)."""
        from datetime import datetime
        import re

        # Find all context blocks
        context_pattern = r'<context[^>]*id=["\']([^"\']+)["\'][^>]*>(.*?)</context>'
        matches = re.findall(context_pattern, self.content, re.DOTALL | re.IGNORECASE)

        for ctx_id, ctx_content in matches:
            if ctx_id in self.context_periods and self.context_periods[ctx_id]['type'] != 'UNKNOWN':
                continue

            period_info = {'type': 'UNKNOWN', 'start': None, 'end': None, 'days': 0}

            # Check for instant
            instant_match = re.search(r'<instant[^>]*>(\d{4}-\d{2}-\d{2})</instant>', ctx_content, re.IGNORECASE)
            if instant_match:
                period_info['type'] = 'INSTANT'
                try:
                    period_info['end'] = datetime.strptime(instant_match.group(1), '%Y-%m-%d').date()
                except:
                    pass
            else:
                # Check for startDate and endDate
                start_match = re.search(r'<startDate[^>]*>(\d{4}-\d{2}-\d{2})</startDate>', ctx_content, re.IGNORECASE)
                end_match = re.search(r'<endDate[^>]*>(\d{4}-\d{2}-\d{2})</endDate>', ctx_content, re.IGNORECASE)

                if start_match and end_match:
                    try:
                        start_date = datetime.strptime(start_match.group(1), '%Y-%m-%d').date()
                        end_date = datetime.strptime(end_match.group(1), '%Y-%m-%d').date()
                        days = (end_date - start_date).days

                        period_info['type'] = 'DURATION'
                        period_info['start'] = start_date
                        period_info['end'] = end_date
                        period_info['days'] = days
                    except:
                        pass

            self.context_periods[ctx_id] = period_info

    def get_period_type(self, context_id: str) -> str:
        """
        Determine period type from context ID.

        Returns one of:
        - 'INSTANT': Point-in-time (balance sheet)
        - 'QTD': Single quarter duration (~90 days)
        - 'YTD_H1': Year-to-date first half (~180 days)
        - 'YTD_9M': Year-to-date 9 months (~270 days)
        - 'YTD_ANNUAL': Full year (~365 days)
        - 'UNKNOWN': Cannot determine
        """
        if not context_id:
            return 'UNKNOWN'

        period_info = self.context_periods.get(context_id, {})
        period_type = period_info.get('type', 'UNKNOWN')

        if period_type == 'INSTANT':
            return 'INSTANT'

        if period_type == 'DURATION':
            days = period_info.get('days', 0)

            if days <= 100:  # ~3 months
                return 'QTD'
            elif days <= 200:  # ~6 months
                return 'YTD_H1'
            elif days <= 290:  # ~9 months
                return 'YTD_9M'
            else:  # ~12 months
                return 'YTD_ANNUAL'

        # Fallback: check context ID naming conventions
        ctx_lower = context_id.lower()
        if 'ytd' in ctx_lower or 'year' in ctx_lower or 'cumulative' in ctx_lower:
            return 'YTD_ANNUAL'  # Assume annual if marked YTD
        elif 'qtd' in ctx_lower or 'qtr' in ctx_lower or 'quarter' in ctx_lower:
            return 'QTD'

        return 'UNKNOWN'

    @staticmethod
    def _local_name(tag: str) -> str:
        if "}" in tag:
            return tag.split("}", 1)[1]
        if ":" in tag:
            return tag.split(":", 1)[1]
        return tag

    def _find_elements(self, local_name: str) -> List[ET.Element]:
        """Find elements by local name (handles both string and list)."""
        if isinstance(local_name, list):
            # If list provided, try each variant
            for variant in local_name:
                elements = self.elements_by_local.get(variant, [])
                if elements:
                    return list(elements)
            return []
        return list(self.elements_by_local.get(local_name, []))
    
    def parse(self) -> Dict[str, Any]:
        """Parse XBRL filing."""
        if not self.xbrl_root:
            return {}
        
        data = {
            'cik': self.cik,
            'company_name': self.company_name,
            'filing_date': self.filing_date,
            'filing_type': self.filing_type,
            'financial_data': {},
            'metadata': {},
            'security_data': {},
            'company_metadata': {}
        }
        
        # Extract financial statement data
        # Common XBRL tags for financial data (US-GAAP)
        # Note: Tags may appear with or without us-gaap: prefix
        financial_tags = {
            # Income Statement
            'revenue': ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'SalesRevenueServicesNet', 'Revenue'],
            'sales': ['SalesRevenueNet', 'SalesRevenueServicesNet', 'Revenues'],
            'cost_of_revenue': ['CostOfGoodsAndServicesSold', 'CostOfRevenue', 'CostOfSales'],
            'gross_profit': ['GrossProfit'],
            'operating_income': ['OperatingIncomeLoss', 'IncomeLossFromOperations'],
            'net_income': ['NetIncomeLoss', 'ProfitLoss', 'IncomeLossFromContinuingOperations', 'NetIncome'],
            'pretax_income': ['IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest', 'IncomeBeforeTax'],
            'tax_expense': ['IncomeTaxExpenseBenefit', 'ProvisionForIncomeTaxes'],
            
            # Expenses
            'sga_expense': ['SellingGeneralAndAdministrativeExpense', 'SellingAndMarketingExpense', 'GeneralAndAdministrativeExpense'],
            'rd_expense': ['ResearchAndDevelopmentExpense', 'ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost'],
            'interest_expense': ['InterestExpenseDebt', 'InterestExpense', 'InterestAndDebtExpense'],
            'depreciation': ['DepreciationDepletionAndAmortization', 'DepreciationAndAmortization'],
            
            # Balance Sheet - Assets
            'assets': ['Assets', 'AssetsCurrent', 'AssetsNoncurrent'],
            'current_assets': ['AssetsCurrent'],
            'cash': ['CashAndCashEquivalentsAtCarryingValue', 'Cash', 'CashCashEquivalentsAndShortTermInvestments'],
            'receivables': ['AccountsReceivableNetCurrent', 'AccountsReceivableNet', 'TradeAndOtherReceivables'],
            'inventory': ['InventoryNet', 'Inventory'],
            'ppe_net': ['PropertyPlantAndEquipmentNet', 'PropertyPlantAndEquipment'],
            'goodwill': ['Goodwill'],
            'intangible_assets': ['IntangibleAssetsNetExcludingGoodwill', 'FiniteLivedIntangibleAssetsNet'],
            
            # Balance Sheet - Liabilities
            'liabilities': ['Liabilities', 'LiabilitiesCurrent', 'LiabilitiesNoncurrent'],
            'current_liabilities': ['LiabilitiesCurrent'],
            'accounts_payable': ['AccountsPayableCurrent', 'AccountsPayable'],
            'short_term_debt': ['DebtCurrent', 'ShortTermBorrowings', 'CommercialPaper'],
            'long_term_debt': ['LongTermDebtAndCapitalLeaseObligations', 'LongTermDebt', 'DebtNoncurrent'],
            'total_debt': ['DebtCurrent', 'DebtNoncurrent', 'LongTermDebt'],
            
            # Balance Sheet - Equity
            'equity': ['StockholdersEquity', 'Equity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
            'common_equity': ['StockholdersEquity', 'CommonStockholdersEquity'],
            'preferred_stock': ['PreferredStockValue', 'PreferredStock'],
            'minority_interest': ['NoncontrollingInterest', 'MinorityInterest'],
            
            # Shares and EPS
            'shares_outstanding': ['WeightedAverageNumberOfSharesOutstandingBasic', 'EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding'],
            'shares_basic': ['WeightedAverageNumberOfSharesOutstandingBasic'],
            'shares_diluted': ['WeightedAverageNumberOfSharesOutstandingDiluted'],
            'eps_basic': ['EarningsPerShareBasic', 'EarningsPerShare'],
            'eps_diluted': ['EarningsPerShareDiluted'],
            
            # Cash Flow
            'operating_cash_flow': ['NetCashProvidedByUsedInOperatingActivities', 'CashFlowFromOperatingActivities'],
            'investing_cash_flow': ['NetCashProvidedByUsedInInvestingActivities', 'CashFlowFromInvestingActivities'],
            'financing_cash_flow': ['NetCashProvidedByUsedInFinancingActivities', 'CashFlowFromFinancingActivities'],
            'capex': ['CapitalExpenditures', 'PaymentsToAcquirePropertyPlantAndEquipment'],
        }

        # Extract known financial tags
        for key, tag_variants in financial_tags.items():
            value = self._extract_first_numeric(tag_variants)
            if value is not None:
                data['financial_data'][key] = value
        
        # Also extract ALL us-gaap tags for comprehensive coverage
        all_us_gaap_tags = self._extract_all_us_gaap_tags()
        if all_us_gaap_tags:
            data['financial_data'].update(all_us_gaap_tags)
        
        # Extract security identifiers (ticker, CUSIP, etc.)
        self._extract_security_data(data)
        # Extract company metadata (address, fiscal year, etc.)
        self._extract_company_metadata(data)
        
        # Extract document period end date (use parsed version if available)
        period_end_text = self._extract_text_value(['DocumentPeriodEndDate', 'dei:DocumentPeriodEndDate'])
        if period_end_text:
            data['document_period_end_date'] = period_end_text
        # Use parsed date from metadata if available
        if 'document_period_end_date_parsed' in data.get('company_metadata', {}):
            data['document_period_end_date_parsed'] = data['company_metadata']['document_period_end_date_parsed']
        
        return data
    
    def _extract_first_numeric(self, tag_variants: Iterable[str]) -> Optional[float]:
        """Extract first numeric value from tag variants."""
        # First try element names
        for variant in tag_variants:
            elements = self._find_elements(variant)
            numeric = self._select_numeric(elements)
            if numeric is not None:
                return numeric
        
        # Also check nonFraction elements with name attributes (for HTML-embedded XBRL)
        # Look for us-gaap: tags in nonFraction elements
        if self.xbrl_root is not None:
            # Try different namespace prefixes for nonFraction
            ns_patterns = [
                './/{http://www.xbrl.org/2003/instance}nonFraction',
                './/{http://www.xbrl.org/2008/inlineXBRL}nonFraction',
                './/ix:nonFraction',
                './/nonFraction',
            ]
            
            for ns_pattern in ns_patterns:
                try:
                    non_fractions = self.xbrl_root.findall(ns_pattern)
                    for variant in tag_variants:
                        # Try exact match and us-gaap: prefix
                        search_terms = [variant, f'us-gaap:{variant}', f'us-gaap:{variant}']
                        for search_term in search_terms:
                            for elem in non_fractions:
                                name_attr = elem.get('name', '')
                                if search_term.lower() in name_attr.lower() or name_attr.endswith(f':{variant}'):
                                    numeric = self._to_float(elem.text)
                                    if numeric is not None:
                                        # Handle scale attribute - convert to millions (Compustat standard)
                                        # scale="6" means value is in millions (use as-is)
                                        # scale="3" means value is in thousands (divide by 1000)
                                        # scale="0" means value is in raw units (divide by 1,000,000)
                                        # scale="9" means value is in billions (multiply by 1000)
                                        scale = elem.get('scale')
                                        if scale:
                                            try:
                                                scale_int = int(scale)
                                                # Convert to millions (scale=6)
                                                if scale_int != 6:
                                                    # Adjust relative to millions
                                                    adjustment = scale_int - 6
                                                    numeric *= (10 ** adjustment)
                                            except ValueError:
                                                pass
                                        return numeric
                except:
                    continue
        
        # Also search in raw content for nonFraction elements
        if self.content:
            import re
            for variant in tag_variants:
                # Look for nonFraction with us-gaap tag - capture attributes and value
                pattern = rf'<ix:nonFraction([^>]*name=["\'][^"\']*us-gaap:{variant}[^"\']*["\'][^>]*)>([^<]+)</ix:nonFraction>'
                matches = re.findall(pattern, self.content, re.IGNORECASE)
                if matches:
                    for attrs, value in matches:
                        numeric = self._to_float(value)
                        if numeric is not None:
                            # Extract scale from attributes and convert to millions
                            scale_match = re.search(r'scale=["\']([-\d]+)["\']', attrs)
                            if scale_match:
                                try:
                                    scale_int = int(scale_match.group(1))
                                    # Convert to millions (scale=6 is baseline)
                                    if scale_int != 6:
                                        adjustment = scale_int - 6
                                        numeric *= (10 ** adjustment)
                                except ValueError:
                                    pass

                            return numeric
        
        return None

    @staticmethod
    def _to_float(text: Optional[str]) -> Optional[float]:
        if text is None:
            return None
        stripped = text.strip().replace(',', '')
        if not stripped:
            return None
        if stripped.startswith('(') and stripped.endswith(')'):
            stripped = f"-{stripped[1:-1].strip()}"
        try:
            return float(stripped)
        except ValueError:
            return None

    def _select_numeric(self, elements: List[ET.Element]) -> Optional[float]:
        """
        Select numeric value, preferring quarterly (QTR) contexts over year-to-date (YTD).
        
        XBRL contexts can be:
        - Instant (point in time): e.g., 'FD2024Q1' (end of Q1)
        - Duration QTR (quarterly): e.g., 'FD2024Q1QTD' (start Q1 to end Q1)
        - Duration YTD (year-to-date): e.g., 'FD2024Q1YTD' (start of year to end Q1)
        
        For quarterly financial items, we prefer QTR over YTD.
        """
        values = []
        for elem in elements:
            value = self._to_float(elem.text)
            if value is None:
                continue
            context = elem.attrib.get('contextRef', '')
            unit = elem.attrib.get('unitRef', '')
            
            # Determine context type: prefer QTR over YTD
            context_lower = context.lower()
            is_ytd = 'ytd' in context_lower or 'year' in context_lower or 'cumulative' in context_lower
            is_qtr = 'qtr' in context_lower or 'quarter' in context_lower or ('duration' in context_lower and 'ytd' not in context_lower)
            
            # Priority: instant > qtr > ytd > unknown
            if is_ytd:
                priority = 1  # Lowest priority
            elif is_qtr:
                priority = 3  # High priority
            elif context:  # Has context but not clearly YTD/QTR
                priority = 2  # Medium priority
            else:
                priority = 0  # No context, lowest
                
            values.append((priority, context or '', unit or '', value))
        
        if not values:
            return None
        
        # Sort by priority (descending), then by context name
        values.sort(key=lambda x: (-x[0], x[1]))
        
        # Return highest priority value
        return values[0][3]

    def _extract_text_value(self, tag_variants: Iterable[str]) -> Optional[str]:
        # First try element names
        for variant in tag_variants:
            elements = self._find_elements(variant)
            for elem in elements:
                text = (elem.text or '').strip()
                if text:
                    return text
        
        # Also search in raw content for HTML-embedded XBRL (ix:nonFraction)
        if self.content:
            import re
            for variant in tag_variants:
                # Remove namespace prefixes for search
                search_variant = variant.replace('dei:', '').replace('{http://xbrl.sec.gov/dei/2014-01-31}', '')
                # Try multiple patterns
                patterns = [
                    rf'name=["\']dei:{search_variant}["\'][^>]*>([^<]+)</',
                    rf'name=["\']dei:{search_variant}["\'][^>]*>(.*?)</(?:ix:nonFraction|ix:nonNumeric|nonFraction|nonNumeric)',
                    rf'name=["\']{search_variant}["\'][^>]*>([^<]+)</',
                ]
                for pattern in patterns:
                    matches = re.findall(pattern, self.content, re.IGNORECASE | re.DOTALL)
                    if matches:
                        for match in matches:
                            # Remove HTML tags and entities if present
                            text = re.sub(r'<[^>]+>', '', match)
                            text = re.sub(r'&#\d+;', ' ', text)  # Remove HTML entities
                            text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
                            # For address fields, take first line only and clean up
                            if 'Address' in variant:
                                # Split by comma to get address components
                                parts = text.split(',')
                                if parts:
                                    # Take first part (street address)
                                    first_part = parts[0].strip()
                                    # Remove phone numbers, URLs, etc.
                                    first_part = re.sub(r'\(?\d{3}\)?\s*\d{3}-\d{4}', '', first_part)
                                    first_part = re.sub(r'www\.[^\s]+', '', first_part)
                                    first_part = re.sub(r'\s+', ' ', first_part).strip()
                                    # Stop at keywords that indicate end of address
                                    stop_words = ['Securities', 'Title', 'Trading', 'Common stock']
                                    for word in stop_words:
                                        if word in first_part:
                                            first_part = first_part.split(word)[0].strip()
                                    if first_part:
                                        text = first_part
                            if text and len(text) > 0 and len(text) < 200:  # Reasonable length
                                return text
        
        return None
    
    def _extract_all_us_gaap_tags(self) -> Dict[str, float]:
        """Extract all us-gaap tags from XBRL for comprehensive coverage."""
        all_tags: Dict[str, float] = {}
        best_meta: Dict[str, Tuple[int, float, str]] = {}  # key -> (priority, abs(value), period_type)

        if not self.xbrl_root:
            return all_tags

        # Find all elements with us-gaap namespace
        us_gaap_patterns = [
            './/{http://fasb.org/us-gaap/}*',
            './/us-gaap:*',
            './/*[starts-with(local-name(), "us-gaap:")]',
        ]

        # Also search in raw content for HTML-embedded XBRL
        if self.content:
            import re
            # Items where period type matters (income statement and cash flow)
            period_sensitive_keys = {
                'netcashprovidedbyusedinoperatingactivities',
                'cashflowfromoperatingactivities',
                'netcashprovidedbyusedininvestingactivities',
                'cashflowfrominvestingactivities',
                'netcashprovidedbyusedinfinancingactivities',
                'cashflowfromfinancingactivities',
                'revenues',
                'revenuefromcontractwithcustomerexcludingassessedtax',
                'netincomeloss',
                'operatingincomeloss',
                'costofgoodsandservicessold',
                'costofrevenue',
            }
            # Find all ix:nonFraction and ix:nonNumeric elements with us-gaap tags
            patterns = [
                r'<ix:nonFraction([^>]*)name=["\']([^"\']*us-gaap:([^"\']+))["\'][^>]*>([^<]+)</ix:nonFraction>',
                r'<ix:nonNumeric([^>]*)name=["\']([^"\']*us-gaap:([^"\']+))["\'][^>]*>([^<]+)</ix:nonNumeric>',
            ]

            for pattern in patterns:
                matches = re.findall(pattern, self.content, re.IGNORECASE)
                for match in matches:
                    if len(match) >= 4:
                        attrs = match[0]  # Attributes string
                        tag_name = match[2] if len(match) > 2 else match[1]
                        value_str = match[-1] if len(match) > 3 else match[2]

                        # Clean value
                        value_str = re.sub(r'<[^>]+>', '', value_str)
                        value_str = re.sub(r'&#\d+;', ' ', value_str)
                        value_str = re.sub(r'\s+', ' ', value_str).strip()
                        if value_str.startswith('(') and value_str.endswith(')'):
                            value_str = f"-{value_str[1:-1]}"
                        value_str = value_str.replace(',', '')

                        # Try to convert to float
                        try:
                            value = float(value_str)

                            # Handle scale attribute - convert to millions (Compustat standard)
                            scale_match = re.search(r'scale=["\']([-\d]+)["\']', attrs)
                            if scale_match:
                                try:
                                    scale_int = int(scale_match.group(1))
                                    # Convert to millions (scale=6 is baseline)
                                    if scale_int != 6:
                                        adjustment = scale_int - 6
                                        value *= (10 ** adjustment)
                                except ValueError:
                                    pass

                            # Extract context reference for period type detection
                            context_match = re.search(r'contextRef=["\']([^"\']+)["\']', attrs, re.IGNORECASE)
                            context_ref = context_match.group(1) if context_match else ''
                            period_type = self.get_period_type(context_ref)

                            # Use tag name as key (normalize)
                            key = tag_name.lower().replace('us-gaap:', '').replace(':', '_')

                            # For period-sensitive items, prefer QTD over YTD
                            if key in period_sensitive_keys:
                                # Priority: QTD > UNKNOWN > YTD
                                if period_type == 'QTD':
                                    priority = 5
                                elif period_type == 'INSTANT':
                                    priority = 4
                                elif period_type == 'UNKNOWN':
                                    priority = 2
                                else:  # YTD variants
                                    priority = 1

                                prev = best_meta.get(key)
                                if prev is None or priority > prev[0] or (priority == prev[0] and abs(value) > prev[1]):
                                    best_meta[key] = (priority, abs(value), period_type)
                                    all_tags[key] = value
                                    # Store period type for this tag
                                    period_key = f"_period_type_{key}"
                                    all_tags[period_key] = period_type  # type: ignore
                            else:
                                if key not in all_tags or abs(value) > abs(all_tags[key]):
                                    all_tags[key] = value
                                    # Store period type for non-sensitive items too
                                    period_key = f"_period_type_{key}"
                                    all_tags[period_key] = period_type  # type: ignore
                        except ValueError:
                            continue

        return all_tags
    
    def _extract_security_data(self, data: Dict[str, Any]):
        """Extract security identifiers from XBRL."""
        # Try various tag patterns including namespaced versions
        ticker = self._extract_text_value([
            'TradingSymbol', 
            'EntityTradingSymbol',
            'dei:TradingSymbol',
            '{http://xbrl.sec.gov/dei/2014-01-31}TradingSymbol'
        ])
        if ticker:
            ticker = ticker.upper().strip()
            if ticker and ticker not in {'FALSE', 'TRUE'}:
                data['security_data']['ticker'] = ticker
        
        # Also try searching in raw content for HTML-embedded XBRL
        if 'ticker' not in data['security_data'] and self.content:
            import re
            # Find dei:TradingSymbol tag and extract text content (handles nested HTML)
            ticker_match = re.search(r'name=["\']dei:TradingSymbol["\'][^>]*>(.*?)</', self.content, re.IGNORECASE | re.DOTALL)
            if ticker_match:
                inner_content = ticker_match.group(1)
                # Remove all HTML tags to get just text
                text_content = re.sub(r'<[^>]+>', '', inner_content).strip()
                # Extract ticker (1-5 uppercase letters)
                ticker_candidates = re.findall(r'\b([A-Z]{1,5})\b', text_content)
                invalid_words = {'FALSE', 'TRUE', 'NONE', 'SPAN', 'STYLE', 'TEXT'}
                for candidate in ticker_candidates:
                    if candidate.isalpha() and candidate not in invalid_words and 2 <= len(candidate) <= 5:
                        data['security_data']['ticker'] = candidate
                        break
    
    def _get_namespace(self) -> str:
        """Get XBRL namespace from root element."""
        if self.xbrl_root is None:
            return ""
        # Extract namespace from root tag
        tag = self.xbrl_root.tag
        if tag.startswith('{'):
            return tag[1:tag.index('}')]
        return ""
    
    def _extract_company_metadata(self, data: Dict[str, Any]):
        """Extract company metadata from dei: tags."""
        # Address fields
        address_line1 = self._extract_text_value([
            'EntityAddressAddressLine1',
            'dei:EntityAddressAddressLine1',
            '{http://xbrl.sec.gov/dei/2014-01-31}EntityAddressAddressLine1'
        ])
        if address_line1:
            data['company_metadata']['address_line1'] = address_line1
        
        address_line2 = self._extract_text_value([
            'EntityAddressAddressLine2',
            'dei:EntityAddressAddressLine2',
            '{http://xbrl.sec.gov/dei/2014-01-31}EntityAddressAddressLine2'
        ])
        if address_line2:
            data['company_metadata']['address_line2'] = address_line2
        
        # Parse city, state, zip from address_line1 if we got it
        if address_line1:
            # Clean address_line1 - remove extra content
            address_line1 = re.sub(r'\(?\d{3}\)?\s*\d{3}-\d{4}', '', address_line1)
            address_line1 = re.sub(r'www\.[^\s]+', '', address_line1)
            address_line1 = address_line1.strip()
            data['company_metadata']['address_line1'] = address_line1
            
            # Try to parse "CITY, STATE ZIP" format from full address
            # Look for pattern like "REDMOND, Washington 98052-6399" or "REDMOND, WA 98052-6399"
            city_state_match = re.search(r',\s*([^,]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', address_line1)
            if not city_state_match:
                # Try with full state name: ", CITY, STATE ZIP"
                city_state_match = re.search(r',\s*([^,]+),\s*([A-Z][a-z]+)\s+(\d{5}(?:-\d{4})?)', address_line1)
            if city_state_match:
                city = city_state_match.group(1).strip()
                state = city_state_match.group(2).strip()
                zip_code = city_state_match.group(3).strip()
                
                # Map full state names to abbreviations
                state_map = {
                    'Washington': 'WA', 'California': 'CA', 'New York': 'NY', 'Texas': 'TX',
                    'Florida': 'FL', 'Illinois': 'IL', 'Pennsylvania': 'PA', 'Ohio': 'OH',
                    'Georgia': 'GA', 'North Carolina': 'NC', 'Michigan': 'MI', 'New Jersey': 'NJ',
                    'Virginia': 'VA', 'Massachusetts': 'MA', 'Tennessee': 'TN', 'Indiana': 'IN',
                    'Arizona': 'AZ', 'Missouri': 'MO', 'Maryland': 'MD', 'Wisconsin': 'WI',
                    'Colorado': 'CO', 'Minnesota': 'MN', 'South Carolina': 'SC', 'Alabama': 'AL',
                    'Louisiana': 'LA', 'Kentucky': 'KY', 'Oregon': 'OR', 'Oklahoma': 'OK',
                    'Connecticut': 'CT', 'Utah': 'UT', 'Iowa': 'IA', 'Nevada': 'NV',
                    'Arkansas': 'AR', 'Mississippi': 'MS', 'Kansas': 'KS', 'New Mexico': 'NM',
                    'Nebraska': 'NE', 'West Virginia': 'WV', 'Idaho': 'ID', 'Hawaii': 'HI',
                    'New Hampshire': 'NH', 'Maine': 'ME', 'Montana': 'MT', 'Rhode Island': 'RI',
                    'Delaware': 'DE', 'South Dakota': 'SD', 'North Dakota': 'ND', 'Alaska': 'AK',
                    'Vermont': 'VT', 'Wyoming': 'WY', 'District of Columbia': 'DC'
                }
                if state in state_map:
                    state = state_map[state]
                
                if not data['company_metadata'].get('city'):
                    data['company_metadata']['city'] = city
                if not data['company_metadata'].get('state'):
                    data['company_metadata']['state'] = state
                if not data['company_metadata'].get('zip_code'):
                    data['company_metadata']['zip_code'] = zip_code
        
        # Try separate extraction if not found above
        if 'city' not in data['company_metadata'] or not data['company_metadata']['city']:
            city = self._extract_text_value([
                'EntityAddressCityOrTown',
                'dei:EntityAddressCityOrTown',
                '{http://xbrl.sec.gov/dei/2014-01-31}EntityAddressCityOrTown'
            ])
            if city:
                city = re.sub(r'<[^>]+>', '', city)
                city = re.sub(r'&#\d+;', ' ', city)
                city = re.sub(r'\s+', ' ', city).strip()
                city = city.split(',')[0].split('\n')[0].strip()
                if city and len(city) < 50:
                    data['company_metadata']['city'] = city
        
        if 'state' not in data['company_metadata'] or not data['company_metadata']['state']:
            # PRIORITY 1: Try XBRL tag for business address state (most reliable)
            state = self._extract_text_value([
                'EntityAddressStateOrProvince',
                'dei:EntityAddressStateOrProvince',
                '{http://xbrl.sec.gov/dei/2014-01-31}EntityAddressStateOrProvince'
            ])
            if state:
                state = re.sub(r'<[^>]+>', '', state)
                state = re.sub(r'&#\d+;', ' ', state)
                state = re.sub(r'\s+', ' ', state).strip()
                state = state.split('\n')[0].split(' ')[0].strip()
                state_upper = state.upper()
                if state_upper:
                    if len(state_upper) <= 2:
                        data['company_metadata']['state'] = state_upper
                    else:
                        abbr = STATE_ABBREVIATIONS.get(state_upper)
                        if abbr:
                            data['company_metadata']['state'] = abbr

            # PRIORITY 2: Try to extract from address_line1 (business address)
            if not data['company_metadata'].get('state') and data['company_metadata'].get('address_line1'):
                addr = data['company_metadata']['address_line1']
                # Look for state pattern in address (e.g., "Redmond, WA 98052")
                state_match = re.search(r',\s*([A-Z]{2})\s+\d{5}', addr, re.IGNORECASE)
                if state_match:
                    data['company_metadata']['state'] = state_match.group(1).upper()

            # PRIORITY 3: Fallback to state of incorporation (least preferred)
            if not data['company_metadata'].get('state') and self.content:
                state_incorp_match = re.search(r'STATE OF INCORPORATION[:\s]+([A-Z]{2})', self.content[:100000], re.IGNORECASE)
                if state_incorp_match:
                    data['company_metadata']['state'] = state_incorp_match.group(1).strip()
        if 'zip_code' not in data['company_metadata'] or not data['company_metadata']['zip_code']:
            zip_code = self._extract_text_value([
                'EntityAddressPostalZipCode',
                'dei:EntityAddressPostalZipCode',
                '{http://xbrl.sec.gov/dei/2014-01-31}EntityAddressPostalZipCode'
            ])
            if zip_code:
                zip_code = re.sub(r'<[^>]+>', '', zip_code)
                zip_code = re.sub(r'&#\d+;', ' ', zip_code)
                zip_code = re.sub(r'\s+', ' ', zip_code).strip()
                zip_code = zip_code.split('\n')[0].split(' ')[0].strip()
                # Extract ZIP pattern
                zip_match = re.search(r'(\d{5}(?:-\d{4})?)', zip_code)
                if zip_match:
                    data['company_metadata']['zip_code'] = zip_match.group(1)
        
        # Fiscal year end month - try header first (most reliable)
        fiscal_year_end_month = None
        if self.content:
            # Try format "FISCAL YEAR END: 0630" (MMDD format) in header
            fye_mmdd_match = re.search(r'FISCAL YEAR END[:\s]+(\d{4})', self.content[:100000], re.IGNORECASE)
            if fye_mmdd_match:
                mmdd = fye_mmdd_match.group(1)
                if len(mmdd) == 4 and mmdd.isdigit():
                    month = int(mmdd[:2])
                    if 1 <= month <= 12:
                        fiscal_year_end_month = month
                        data['company_metadata']['fiscal_year_end_month'] = month
        
        # Fiscal year end date
        period_end = self._extract_text_value([
            'DocumentPeriodEndDate',
            'dei:DocumentPeriodEndDate',
            '{http://xbrl.sec.gov/dei/2014-01-31}DocumentPeriodEndDate'
        ])
        if period_end:
            # Clean up the date string
            period_end = re.sub(r'<[^>]+>', '', period_end)
            period_end = re.sub(r'&#\d+;', ' ', period_end)
            period_end = re.sub(r'\s+', ' ', period_end).strip()
            # Take first line only
            period_end = period_end.split('\n')[0].split('OR')[0].strip()
            
            data['company_metadata']['document_period_end_date'] = period_end
            
            # Parse date and extract period end month (for calculating fiscal year end)
            try:
                from datetime import datetime
                # Try different date formats
                date_formats = [
                    '%Y-%m-%d',  # YYYY-MM-DD
                    '%B %d, %Y',  # June 30, 2024
                    '%b %d, %Y',  # Jun 30, 2024
                    '%m/%d/%Y',  # 06/30/2024
                ]
                parsed_date = None
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(period_end[:20], fmt)
                        break
                    except:
                        continue
                
                if parsed_date:
                    # Store period end month (will be used to calculate fiscal year end)
                    data['company_metadata']['period_end_month'] = parsed_date.month
                    # Also store parsed date for easier use
                    data['company_metadata']['document_period_end_date_parsed'] = parsed_date.date()
                    
                    # Only use period end month as fiscal year end if we don't have explicit fiscal year end
                    if not fiscal_year_end_month:
                        # For fiscal year end, we want the month of the period end date
                        # But we should prefer the most common value across filings
                        data['company_metadata']['fiscal_year_end_month'] = parsed_date.month
            except Exception as e:
                logger.debug(f"Could not parse date '{period_end}': {e}")
                pass
        
        # Legal name
        legal_name = self._extract_text_value([
            'EntityRegistrantName',
            'dei:EntityRegistrantName',
            '{http://xbrl.sec.gov/dei/2014-01-31}EntityRegistrantName'
        ])
        if legal_name:
            data['company_metadata']['legal_name'] = legal_name
        
        # CIK (if not already set)
        if not data.get('cik'):
            cik = self._extract_text_value([
                'EntityCentralIndexKey',
                'dei:EntityCentralIndexKey',
                '{http://xbrl.sec.gov/dei/2014-01-31}EntityCentralIndexKey'
            ])
            if cik:
                data['cik'] = cik.lstrip('0') or '0'
        
        # Extract currency from unitRef
        currency = self._extract_currency()
        if currency:
            data['company_metadata']['currency'] = currency
        
        # Extract SIC, phone, website, EIN from raw content (may be in HTML-embedded XBRL)
        self._extract_additional_company_metadata(data)
    
    def _extract_currency(self) -> Optional[str]:
        """Extract currency code from XBRL unitRef."""
        if not self.xbrl_root:
            return None
        
        # Look for unit elements
        unit_patterns = [
            './/{http://www.xbrl.org/2003/instance}unit',
            './/unit',
        ]
        
        for pattern in unit_patterns:
            try:
                units = self.xbrl_root.findall(pattern)
                for unit in units:
                    # Check for measure elements with currency
                    measures = unit.findall('.//{http://www.xbrl.org/2003/instance}measure')
                    for measure in measures:
                        text = (measure.text or '').strip()
                        if text.startswith('iso4217:'):
                            return text.split(':')[1]
                        elif len(text) == 3 and text.isupper():
                            return text
            except:
                continue
        
        # Also search in raw content
        if self.content:
            import re
            # Look for currency in unitRef
            currency_match = re.search(r'unitRef=["\']([^"\']*)["\']', self.content)
            if currency_match:
                unit_ref = currency_match.group(1)
                # Find unit definition
                unit_match = re.search(rf'<unit[^>]*id=["\']{re.escape(unit_ref)}["\'][^>]*>.*?<measure[^>]*>([^<]+)</measure>', self.content, re.DOTALL | re.IGNORECASE)
                if unit_match:
                    measure = unit_match.group(1).strip()
                    if measure.startswith('iso4217:'):
                        return measure.split(':')[1]
                    elif len(measure) == 3 and measure.isupper():
                        return measure
        
        return None
    
    def _extract_additional_company_metadata(self, data: Dict[str, Any]):
        """Extract SIC, phone, website, EIN from raw content."""
        import re
        
        if not self.content:
            return
        
        # Extract SIC code - try multiple patterns and locations
        if 'sic' not in data['company_metadata'] or not data['company_metadata'].get('sic'):
            # First try in cover page area (first 100KB - SIC is usually early in filing)
            cover_content = self.content[:100000] if len(self.content) > 100000 else self.content
            
            # Try patterns in order of specificity
            sic_patterns = [
                # Most specific patterns first
                r'Standard Industrial Classification[:\s]+Code[:\s]+(\d{4})',
                r'Standard Industrial Classification Code[:\s]+(\d{4})',
                r'Industry Classification Code[:\s]+(\d{4})',
                r'SIC Code[:\s]+(\d{4})',
                r'\(SIC\)[:\s]+(\d{4})',
                r'SIC[:\s]+(\d{4})',
                # Also try in table format (SIC might be in a table cell)
                r'(?:SIC|Standard Industrial Classification).{0,100}?(\d{4})',
            ]
            
            for pattern in sic_patterns:
                matches = list(re.finditer(pattern, cover_content, re.IGNORECASE | re.DOTALL))
                if matches:
                    # Take the first valid 4-digit code
                    for match in matches:
                        sic_code = match.group(1) if match.lastindex else match.group(0)
                        # Extract 4-digit code if pattern captured more
                        sic_match = re.search(r'(\d{4})', sic_code)
                        if sic_match:
                            sic_code = sic_match.group(1)
                            if len(sic_code) == 4 and sic_code.isdigit() and sic_code[0] != '0' or sic_code == '0000':
                                # Valid SIC code (usually starts with non-zero, but allow 0000)
                                data['company_metadata']['sic'] = sic_code
                                break
                    if 'sic' in data['company_metadata']:
                        break
            
            # If still not found, try full content with most common patterns
            if 'sic' not in data['company_metadata'] or not data['company_metadata'].get('sic'):
                for pattern in [r'SIC[:\s]+(\d{4})', r'Standard Industrial Classification[:\s]+(\d{4})']:
                    matches = list(re.finditer(pattern, self.content, re.IGNORECASE))
                    if matches:
                        for match in matches[:5]:  # Check first 5 matches
                            sic_code = match.group(1)
                            if len(sic_code) == 4 and sic_code.isdigit():
                                data['company_metadata']['sic'] = sic_code
                                break
                        if 'sic' in data['company_metadata']:
                            break
        
        # Extract phone number
        if 'phone' not in data['company_metadata'] or not data['company_metadata'].get('phone'):
            phone_patterns = [
                r'Phone[:\s]+([\d\s\-\(\)]+)',
                r'Telephone[:\s]+([\d\s\-\(\)]+)',
                r'\((\d{3})\)\s*(\d{3})-(\d{4})',
                r'(\d{3})[-\s](\d{3})[-\s](\d{4})',
            ]
            for pattern in phone_patterns:
                matches = re.findall(pattern, self.content, re.IGNORECASE)
                if matches:
                    if isinstance(matches[0], tuple):
                        phone = ''.join(matches[0])
                    else:
                        phone = re.sub(r'[^\d]', '', matches[0])
                    if len(phone) == 10:
                        data['company_metadata']['phone'] = phone
                        break
        
        # Extract website URL
        if 'website' not in data['company_metadata'] or not data['company_metadata'].get('website'):
            website_patterns = [
                r'www\.([a-zA-Z0-9\-\.]+)',
                r'http[s]?://([a-zA-Z0-9\-\.]+)',
            ]
            for pattern in website_patterns:
                matches = re.findall(pattern, self.content, re.IGNORECASE)
                if matches:
                    # Prefer company domain (exclude third-party sites)
                    for match in matches:
                        domain = match.lower()
                        # Filter out common non-company domains
                        if not any(x in domain for x in ['sec.gov', 'edgar', 'xbrl', 'dfinsolutions', 'donnelley']):
                            # Prefer microsoft.com, nvidia.com, etc.
                            if any(x in domain for x in ['microsoft', 'nvidia', 'apple', 'google', 'amazon']):
                                data['company_metadata']['website'] = f'www.{domain}'
                                break
                    if 'website' not in data['company_metadata'] and matches:
                        # Use first match if no company domain found
                        domain = matches[0].lower()
                        if not any(x in domain for x in ['sec.gov', 'edgar', 'xbrl', 'dfinsolutions', 'donnelley']):
                            data['company_metadata']['website'] = f'www.{domain}'
                    break
        
        # Extract EIN - try multiple patterns and locations
        if 'ein' not in data['company_metadata'] or not data['company_metadata'].get('ein'):
            # First try XBRL tag: dei:EntityTaxIdentificationNumber
            ein_xbrl = self._extract_text_value([
                'EntityTaxIdentificationNumber',
                'dei:EntityTaxIdentificationNumber',
                '{http://xbrl.sec.gov/dei/2014-01-31}EntityTaxIdentificationNumber'
            ])
            if ein_xbrl:
                # Clean and format EIN
                ein_xbrl = re.sub(r'<[^>]+>', '', ein_xbrl)
                ein_xbrl = re.sub(r'&#\d+;', ' ', ein_xbrl)
                ein_xbrl = re.sub(r'\s+', '', ein_xbrl).strip()
                # Format as XX-XXXXXXX if it's 9 digits
                if len(ein_xbrl) == 9 and ein_xbrl.isdigit():
                    data['company_metadata']['ein'] = f"{ein_xbrl[:2]}-{ein_xbrl[2:]}"
                elif re.match(r'\d{2}-\d{7}', ein_xbrl):
                    data['company_metadata']['ein'] = ein_xbrl
            
            # Also try in cover page area (first 100KB - EIN is usually early in filing)
            if not data['company_metadata'].get('ein'):
                cover_content = self.content[:100000] if len(self.content) > 100000 else self.content
                
                # Try patterns in order of specificity
                ein_patterns = [
                    # Header section: "IRS NUMBER: 911144442" (9 digits, no dash)
                    r'IRS NUMBER[:\s]+(\d{9})',
                    r'IRS[:\s]+NUMBER[:\s]+(\d{9})',
                    # Most specific patterns first
                    r'Employer Identification Number[:\s]+(\d{2}-\d{7})',
                    r'Federal Tax Identification Number[:\s]+(\d{2}-\d{7})',
                    r'Federal Tax ID[:\s]+(\d{2}-\d{7})',
                    r'E\.I\.N\.\s*[:\s]+(\d{2}-\d{7})',
                    r'EIN[:\s]+(\d{2}-\d{7})',
                    r'Tax ID[:\s]+(\d{2}-\d{7})',
                    # Also try finding XX-XXXXXXX pattern near EIN keywords
                    r'(?:EIN|Employer Identification Number|Federal Tax ID|Tax ID)[^\\d]{0,50}(\\d{2}-\\d{7})',
                ]
            
                for pattern in ein_patterns:
                    matches = list(re.finditer(pattern, cover_content, re.IGNORECASE | re.DOTALL))
                    if matches:
                        # Take the first valid EIN format
                        for match in matches:
                            ein = match.group(1) if match.lastindex else None
                            if not ein:
                                # Try to extract from full match
                                ein_match = re.search(r'(\d{2}-\d{7})', match.group(0))
                                if ein_match:
                                    ein = ein_match.group(1)
                            
                            # Format 9-digit EIN as XX-XXXXXXX
                            if ein and len(ein) == 9 and ein.isdigit():
                                ein = f"{ein[:2]}-{ein[2:]}"
                            
                            if ein and (re.match(r'\d{2}-\d{7}', ein) or (len(ein) == 9 and ein.isdigit())):
                                if len(ein) == 9:
                                    ein = f"{ein[:2]}-{ein[2:]}"
                                data['company_metadata']['ein'] = ein
                                break
                        if 'ein' in data['company_metadata']:
                            break
            
            # If still not found, try full content with most common patterns
            if 'ein' not in data['company_metadata'] or not data['company_metadata'].get('ein'):
                for pattern in [r'EIN[:\s]+(\d{2}-\d{7})', r'Employer Identification Number[:\s]+(\d{2}-\d{7})']:
                    matches = list(re.finditer(pattern, self.content, re.IGNORECASE))
                    if matches:
                        for match in matches[:5]:  # Check first 5 matches
                            ein = match.group(1)
                            if re.match(r'\d{2}-\d{7}', ein):
                                data['company_metadata']['ein'] = ein
                                break
                        if 'ein' in data['company_metadata']:
                            break
        
        # Extract business description from Item 1 (for 10-K filings)
        if self.filing_type and '10-K' in self.filing_type.upper():
            if 'business_description' not in data['company_metadata'] or not data['company_metadata'].get('business_description'):
                # Try multiple patterns for Item 1
                item1_patterns = [
                    r'Item\s+1[\.\s]+Business(.*?)(?=Item\s+2|$)',
                    r'ITEM\s+1[\.\s]+BUSINESS(.*?)(?=ITEM\s+2|$)',
                    r'Item\s+1\.\s*Business(.*?)(?=Item\s+2|$)',
                    r'PART\s+I[^\n]*Item\s+1[\.\s]+Business(.*?)(?=Item\s+2|$)',
                ]
                
                for pattern in item1_patterns:
                    item1_match = re.search(pattern, self.content, re.IGNORECASE | re.DOTALL)
                    if item1_match:
                        item1_content = item1_match.group(1)
                        # Remove HTML tags first
                        item1_content = re.sub(r'<[^>]+>', ' ', item1_content)
                        item1_content = re.sub(r'&#\d+;', ' ', item1_content)
                        
                        # Extract first few paragraphs
                        # Split by double newlines or HTML paragraph breaks
                        paragraphs = re.split(r'\n\s*\n|</p>|<p>', item1_content)
                        # Filter out very short paragraphs
                        paragraphs = [p.strip() for p in paragraphs if len(p.strip()) > 50]
                        
                        if paragraphs:
                            # Take first 2-4 paragraphs, up to 1000 chars
                            business_desc = ' '.join(paragraphs[:4])[:1000].strip()
                            # Clean up whitespace
                            business_desc = re.sub(r'\s+', ' ', business_desc).strip()
                            
                            if len(business_desc) > 200:
                                data['company_metadata']['business_description'] = business_desc
                                break


class HTMLParser(FilingParser):
    """Parser for HTML filings."""
    
    def __init__(self, filing_path: Path):
        super().__init__(filing_path)
        self.soup = None
    
    def load(self):
        """Load HTML content."""
        if not super().load():
            return False
        
        # Extract HTML section
        html_match = re.search(r'<HTML>(.*?)</HTML>', self.content, re.DOTALL | re.IGNORECASE)
        if html_match:
            html_content = html_match.group(1)
        else:
            # Try to find HTML document directly
            html_match = re.search(r'(<html.*?</html>)', self.content, re.DOTALL | re.IGNORECASE)
            if html_match:
                html_content = html_match.group(1)
            else:
                return False
        
        try:
            # Try lxml first (better for XML/namespaces), fallback to html.parser
            try:
                self.soup = BeautifulSoup(html_content, 'lxml')
            except:
                self.soup = BeautifulSoup(html_content, 'html.parser')
            return True
        except Exception as e:
            logger.warning(f"Error parsing HTML: {e}")
            return False
    
    def parse(self) -> Dict[str, Any]:
        """Parse HTML filing."""
        if not self.soup:
            return {}
        
        data = {
            'cik': self.cik,
            'company_name': self.company_name,
            'filing_date': self.filing_date,
            'filing_type': self.filing_type,
            'financial_data': {},
            'security_data': {},
            'company_metadata': {},
            'text_content': self._extract_text()
        }
        
        # Extract ticker symbol from various locations
        self._extract_ticker_from_html(data)
        # Extract company metadata (address, business description)
        self._extract_company_metadata_from_html(data)
        
        # Try to extract financial tables
        tables = self.soup.find_all('table')
        for table in tables:
            # Look for financial statement tables
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value_text = cells[1].get_text(strip=True)
                    # Try to extract numeric values (handle millions, thousands)
                    value_match = re.search(r'[\d,]+\.?\d*', value_text.replace(',', ''))
                    if value_match:
                        try:
                            value = float(value_match.group(0))
                            # Check for units (millions, thousands)
                            if 'million' in value_text.lower():
                                value *= 1_000_000
                            elif 'thousand' in value_text.lower():
                                value *= 1_000
                            
                            # Map common financial statement items
                            if any(term in label for term in ['revenue', 'sales', 'net sales']):
                                data['financial_data']['revenue'] = value
                            elif any(term in label for term in ['total assets', 'assets']):
                                data['financial_data']['assets'] = value
                            elif any(term in label for term in ['total liabilities', 'liabilities']):
                                data['financial_data']['liabilities'] = value
                            elif any(term in label for term in ['stockholders equity', 'shareholders equity', 'equity']):
                                data['financial_data']['equity'] = value
                            elif any(term in label for term in ['net income', 'net earnings', 'income']):
                                data['financial_data']['net_income'] = value
                        except ValueError:
                            pass
        
        return data
    
    def _extract_ticker_from_html(self, data: Dict[str, Any]):
        """Extract ticker symbol from HTML."""
        # Method 1: Search in raw HTML content for XBRL patterns (works better than BeautifulSoup for namespaced XML)
        if self.content:
            # Find all TradingSymbol tags and extract their text content
            all_matches = re.finditer(r'name=["\']dei:TradingSymbol["\'][^>]*>(.*?)</', self.content, re.IGNORECASE | re.DOTALL)
            invalid_words = {'FALSE', 'TRUE', 'NONE', 'COMMON', 'STOCK', 'TRADING', 'SYMBOL', 'TITLE', 'NAME', 'YES', 'NO'}
            
            for match in all_matches:
                inner_content = match.group(1)
                # Remove HTML tags to get just text
                text_content = re.sub(r'<[^>]+>', ' ', inner_content).strip()
                # Extract all sequences of 1-5 uppercase letters
                ticker_candidates = re.findall(r'\b([A-Z]{1,5})\b', text_content)
                # Filter out common non-ticker words - prefer shorter, valid tickers
                for candidate in ticker_candidates:
                    if (candidate.isalpha() and 
                        candidate not in invalid_words and 
                        len(candidate) >= 1 and 
                        len(candidate) <= 5):
                        # Prefer 2-5 letter tickers over single letters
                        if len(candidate) >= 2:
                            data['security_data']['ticker'] = candidate
                            return
                        # Store single letter as fallback
                        elif 'ticker' not in data['security_data']:
                            data['security_data']['ticker'] = candidate
            
            # If we found a single letter, use it
            if 'ticker' in data['security_data']:
                return
            
            # Also try without dei: prefix
            ticker_match = re.search(r'name=["\']TradingSymbol["\'][^>]*>.*?([A-Z]{1,5}).*?</', self.content, re.IGNORECASE | re.DOTALL)
            if ticker_match:
                ticker = ticker_match.group(1).upper().strip()
                if ticker and len(ticker) <= 5 and ticker.isalpha():
                    data['security_data']['ticker'] = ticker
                    return
        
        # Method 2: Find all tags with name attribute containing TradingSymbol
        all_tags = self.soup.find_all(True, {'name': True})
        for tag in all_tags:
            name_attr = tag.get('name', '').lower()
            if 'tradingsymbol' in name_attr or (name_attr.startswith('dei:') and 'trading' in name_attr):
                text = tag.get_text(strip=True)
                if text and len(text) <= 5 and text.isupper() and text.isalpha():
                    data['security_data']['ticker'] = text
                    return
        
        # Method 2: Look for "Trading Symbol" text and find ticker in nearby table cells
        trading_symbol_texts = self.soup.find_all(string=re.compile(r'Trading\s+Symbol', re.I))
        for text_node in trading_symbol_texts[:5]:
            # Navigate up to find table row
            parent = text_node.parent if hasattr(text_node, 'parent') else None
            while parent and parent.name != 'tr' and parent.name != 'table':
                parent = getattr(parent, 'parent', None)
            
            if parent and parent.name == 'tr':
                # Look in all cells of this row
                cells = parent.find_all(['td', 'th'])
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    # Check if it looks like a ticker (1-5 uppercase letters)
                    if cell_text and len(cell_text) <= 5 and cell_text.isupper() and cell_text.isalpha():
                        data['security_data']['ticker'] = cell_text
                        return
        
        # Fallback to regex patterns in text
        ticker_patterns = [
            r'Trading\s+Symbol[:\s]+([A-Z]{1,5})',
            r'Common\s+Stock[:\s]+([A-Z]{1,5})',
            r'Symbol[:\s]+([A-Z]{1,5})',
            r'Ticker[:\s]+([A-Z]{1,5})',
        ]
        
        text = self.soup.get_text()
        for pattern in ticker_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                ticker = match.group(1).upper()
                if len(ticker) <= 5 and ticker.isalpha():  # Valid ticker length and format
                    data['security_data']['ticker'] = ticker
                    break
    
    def _extract_text(self) -> str:
        """Extract text content from HTML."""
        if not self.soup:
            return ""
        return self.soup.get_text(separator=' ', strip=True)
    
    def _extract_company_metadata_from_html(self, data: Dict[str, Any]):
        """Extract company metadata from HTML filing."""
        import re
        
        if not self.content:
            return
        
        # Extract address from cover page
        # Look for "Principal Executive Offices" or similar patterns
        address_patterns = [
            r'Principal\s+Executive\s+Offices[:\s]+([^\n]{50,200})',
            r'Company\s+Address[:\s]+([^\n]{50,200})',
            r'Address[:\s]+([^\n]{50,200})',
        ]
        
        for pattern in address_patterns:
            match = re.search(pattern, self.content, re.IGNORECASE)
            if match:
                address_text = match.group(1).strip()
                # Parse address components
                lines = [line.strip() for line in address_text.split('\n') if line.strip()]
                if lines:
                    data['company_metadata']['address_line1'] = lines[0]
                    if len(lines) > 1:
                        data['company_metadata']['address_line2'] = lines[1]
                    # Try to extract city, state, zip from last line
                    if lines:
                        last_line = lines[-1]
                        # Pattern: City, State ZIP
                        city_state_match = re.search(r'^([^,]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', last_line)
                        if city_state_match:
                            data['company_metadata']['city'] = city_state_match.group(1).strip()
                            data['company_metadata']['state'] = city_state_match.group(2).strip()
                            data['company_metadata']['zip_code'] = city_state_match.group(3).strip()
                break
        
        # Extract SIC code
        sic_patterns = [
            r'SIC[:\s]+(\d{4})',
            r'Standard Industrial Classification[:\s]+(\d{4})',
            r'Industry Classification Code[:\s]+(\d{4})',
            r'Standard Industrial Classification Code[:\s]+(\d{4})',
        ]
        for pattern in sic_patterns:
            match = re.search(pattern, self.content, re.IGNORECASE)
            if match:
                data['company_metadata']['sic'] = match.group(1)
                break
        
        # Extract phone number
        phone_patterns = [
            r'Phone[:\s]+([\d\s\-\(\)]+)',
            r'Telephone[:\s]+([\d\s\-\(\)]+)',
            r'\((\d{3})\)\s*(\d{3})-(\d{4})',
            r'(\d{3})[-\s](\d{3})[-\s](\d{4})',
        ]
        for pattern in phone_patterns:
            matches = re.findall(pattern, self.content, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    phone = ''.join(matches[0])
                else:
                    phone = re.sub(r'[^\d]', '', matches[0])
                if len(phone) == 10:
                    data['company_metadata']['phone'] = phone
                    break
        
        # Extract website URL
        website_patterns = [
            r'www\.([a-zA-Z0-9\-\.]+)',
            r'http[s]?://([a-zA-Z0-9\-\.]+)',
        ]
        for pattern in website_patterns:
            matches = re.findall(pattern, self.content, re.IGNORECASE)
            if matches:
                # Prefer company domain (exclude third-party sites)
                for match in matches:
                    domain = match.lower()
                    # Filter out common non-company domains
                    if not any(x in domain for x in ['sec.gov', 'edgar', 'xbrl', 'dfinsolutions']):
                        data['company_metadata']['website'] = f'www.{domain}'
                        break
                if 'website' in data['company_metadata']:
                    break
        
        # Extract EIN
        ein_patterns = [
            r'EIN[:\s]+(\d{2}-\d{7})',
            r'Employer Identification Number[:\s]+(\d{2}-\d{7})',
            r'Federal Tax ID[:\s]+(\d{2}-\d{7})',
            r'Tax ID[:\s]+(\d{2}-\d{7})',
        ]
        for pattern in ein_patterns:
            match = re.search(pattern, self.content, re.IGNORECASE)
            if match:
                data['company_metadata']['ein'] = match.group(1)
                break
        
        # Extract business description from Item 1
        business_desc = self._extract_business_description()
        if business_desc:
            data['company_metadata']['business_description'] = business_desc
    
    def _extract_business_description(self) -> Optional[str]:
        """Extract business description from Item 1 section."""
        if not self.content:
            return None
        
        # Look for Item 1 Business section
        # Pattern: Item 1. Business or Item 1 Business
        item1_patterns = [
            r'Item\s+1[\.\s]+Business[^\n]*\n([^\n]{200,2000})',
            r'ITEM\s+1[\.\s]+BUSINESS[^\n]*\n([^\n]{200,2000})',
        ]
        
        for pattern in item1_patterns:
            match = re.search(pattern, self.content, re.IGNORECASE | re.DOTALL)
            if match:
                desc = match.group(1).strip()
                # Clean up HTML tags if present
                desc = re.sub(r'<[^>]+>', ' ', desc)
                # Get first 2-3 paragraphs (first 500-1000 chars)
                desc = desc[:1000].strip()
                if len(desc) > 200:
                    return desc
        
        return None


class TextParser(FilingParser):
    """Parser for plain text filings (fallback)."""
    
    def parse(self) -> Dict[str, Any]:
        """Parse text filing using regex patterns."""
        if not self.content:
            return {}
        
        data = {
            'cik': self.cik,
            'company_name': self.company_name,
            'filing_date': self.filing_date,
            'filing_type': self.filing_type,
            'financial_data': {},
            'security_data': {},
            'text_content': self.content[:10000]  # First 10k chars
        }
        
        # Extract ticker symbol
        ticker_patterns = [
            r'Trading\s+Symbol[:\s]+([A-Z]{1,5})',
            r'Common\s+Stock[:\s]+([A-Z]{1,5})',
            r'Symbol[:\s]+([A-Z]{1,5})',
        ]
        
        for pattern in ticker_patterns:
            match = re.search(pattern, self.content, re.IGNORECASE)
            if match:
                ticker = match.group(1).upper()
                if len(ticker) <= 5:
                    data['security_data']['ticker'] = ticker
                    break
        
        # Try to extract financial data using regex patterns
        financial_patterns = {
            'revenue': [
                r'total\s+revenue[s]?\s*[:\$]?\s*\$?\s*([\d,]+\.?\d*)\s*million',
                r'net\s+sales\s*[:\$]?\s*\$?\s*([\d,]+\.?\d*)\s*million',
            ],
            'assets': [
                r'total\s+assets\s*[:\$]?\s*\$?\s*([\d,]+\.?\d*)\s*million',
            ],
            'net_income': [
                r'net\s+income\s*[:\$]?\s*\$?\s*([\d,]+\.?\d*)\s*million',
            ],
        }
        
        for key, patterns in financial_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, self.content, re.IGNORECASE)
                if match:
                    try:
                        value = float(match.group(1).replace(',', '')) * 1_000_000  # Convert millions
                        data['financial_data'][key] = value
                        break
                    except ValueError:
                        pass
        
        return data


def get_parser(filing_path: Path) -> FilingParser:
    """
    Get appropriate parser for filing.
    
    Args:
        filing_path: Path to filing file
        
    Returns:
        Appropriate parser instance
    """
    parser = FilingParser(filing_path)
    if not parser.load():
        return None
    
    # Determine parser type
    if parser.is_xbrl():
        xbrl_parser = XBRLParser(filing_path)
        if xbrl_parser.load():
            return xbrl_parser
    
    # Try HTML parser
    html_parser = HTMLParser(filing_path)
    if html_parser.load():
        return html_parser
    
    # Fallback to text parser
    return TextParser(filing_path)
