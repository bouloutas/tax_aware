"""
Extract and map data from parsed filings to Compustat schema.
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import date
import duckdb

from src.filing_parser import get_parser
from src.financial_mapper import FinancialMapper
from src.sic_to_gics_mapper import get_gics_from_sic
from config import COMPUSTAT_EDGAR_DB, MAPPING_FILE
import csv

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract data from filings and map to Compustat schema."""
    
    def __init__(self, db_path: Path = None):
        """
        Initialize data extractor.
        
        Args:
            db_path: Path to target database (default: from config)
        """
        self.db_path = db_path or COMPUSTAT_EDGAR_DB
        self.conn = duckdb.connect(str(self.db_path))
        self.cik_to_gvkey = self._load_cik_mapping()
        self.financial_mapper = FinancialMapper(self.db_path)
    
    def _load_cik_mapping(self) -> Dict[str, str]:
        """Load CIK to GVKEY mapping."""
        mapping = {}
        if not MAPPING_FILE.exists():
            logger.warning(f"Mapping file not found: {MAPPING_FILE}")
            return mapping
        
        with open(MAPPING_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cik = row['CIK'].strip().lstrip('0') or '0'
                mapping[cik] = row['GVKEY']
        
        return mapping
    
    def extract_from_filing(self, filing_path: Path) -> Optional[Dict[str, Any]]:
        """
        Extract data from a single filing.
        
        Args:
            filing_path: Path to filing file
            
        Returns:
            Extracted data dictionary or None if failed
        """
        parser = get_parser(filing_path)
        if not parser:
            logger.warning(f"Could not create parser for {filing_path}")
            return None
        
        try:
            parsed_data = parser.parse()
            if not parsed_data:
                return None
            
            # Map to Compustat schema
            mapped_data = self._map_to_compustat(parsed_data)
            return mapped_data
        except Exception as e:
            logger.error(f"Error extracting data from {filing_path}: {e}")
            return None
    
    def _map_to_compustat(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map extracted data to Compustat schema.
        
        Args:
            parsed_data: Data from parser
            
        Returns:
            Mapped data dictionary
        """
        cik = parsed_data.get('cik', '')
        gvkey = self.cik_to_gvkey.get(cik, '')
        
        mapped = {
            'gvkey': gvkey,
            'cik': cik,
            'company_name': parsed_data.get('company_name'),
            'filing_date': parsed_data.get('filing_date'),
            'filing_type': parsed_data.get('filing_type'),
            'financial_data': parsed_data.get('financial_data', {}),
            'security_data': parsed_data.get('security_data', {}),  # Preserve security data
            'company_metadata': parsed_data.get('company_metadata', {}),  # Preserve company metadata
            'document_period_end_date': parsed_data.get('document_period_end_date'),
        }
        
        return mapped
    
    def extract_from_directory(self, directory: Path, 
                              filing_types: List[str] = None,
                              limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Extract data from all filings in a directory.
        
        Args:
            directory: Directory containing filing files
            filing_types: List of filing types to process (None = all)
            limit: Maximum number of filings to process (None = all)
            
        Returns:
            List of extracted data dictionaries
        """
        all_data = []
        
        # Find all filing files
        filing_files = list(directory.rglob("*.txt"))
        logger.info(f"Found {len(filing_files)} filing files in {directory}")
        
        if limit:
            filing_files = filing_files[:limit]
            logger.info(f"Limited to {limit} filings for processing")
        
        for i, filing_path in enumerate(filing_files, 1):
            # Check filing type if filter specified
            if filing_types:
                filing_type = self._get_filing_type_from_path(filing_path)
                if filing_type not in filing_types:
                    continue
            
            data = self.extract_from_filing(filing_path)
            if data:
                all_data.append(data)
            
            if i % 10 == 0:
                logger.info(f"Processed {i}/{len(filing_files)} filings, extracted {len(all_data)} with data")
        
        logger.info(f"Extracted data from {len(all_data)} filings")
        return all_data
    
    def _get_filing_type_from_path(self, path: Path) -> str:
        """Extract filing type from file path."""
        filename = path.name
        # Format: {FILING_TYPE}_{accession_number}.txt
        parts = filename.split('_')
        if parts:
            return parts[0]
        return ""
    
    def populate_company_table(self, extracted_data: List[Dict[str, Any]]):
        """
        Populate COMPANY table from extracted data.
        
        Args:
            extracted_data: List of extracted data dictionaries
        """
        # Group by GVKEY to get unique companies
        companies = {}
        for data in extracted_data:
            gvkey = data.get('gvkey')
            if not gvkey:
                continue
            
            company_metadata = data.get('company_metadata', {})
            if gvkey not in companies:
                companies[gvkey] = {
                    'GVKEY': gvkey,
                    'CIK': data.get('cik', '').zfill(10),  # Pad with leading zeros
                    'CONM': data.get('company_name', ''),
                    'CONML': company_metadata.get('legal_name', ''),
                    'ADD1': company_metadata.get('address_line1', ''),
                    'ADD2': company_metadata.get('address_line2', ''),
                    'CITY': company_metadata.get('city', ''),
                    'STATE': company_metadata.get('state', ''),
                    'ADDZIP': company_metadata.get('zip_code', ''),
                    'FYRC': company_metadata.get('fiscal_year_end_month'),
                    'SIC': company_metadata.get('sic'),
                    'PHONE': company_metadata.get('phone'),
                    'WEBURL': company_metadata.get('website'),
                    'EIN': company_metadata.get('ein'),
                    'BUSDESC': company_metadata.get('business_description'),
                    'STATE': company_metadata.get('state'),
                    # Set defaults for required fields
                    'COSTAT': 'A',  # Active (if filings exist)
                    'FIC': 'USA',  # Default to USA
                    'LOC': 'USA',  # Default to USA
                    'INCORP': company_metadata.get('state'),  # Use state as incorporation state
                }
                
                # Map SIC to GICS codes if SIC is available
                if company_metadata.get('sic'):
                    gics = self._get_gics_from_sic(company_metadata.get('sic'), gvkey)
                    if gics:
                        companies[gvkey]['GSECTOR'] = gics.get('GSECTOR')
                        companies[gvkey]['GGROUP'] = gics.get('GGROUP')
                        companies[gvkey]['GIND'] = gics.get('GIND')
                        companies[gvkey]['GSUBIND'] = gics.get('GSUBIND')
            else:
                # Update with any new metadata found
                if company_metadata.get('legal_name') and not companies[gvkey].get('CONML'):
                    companies[gvkey]['CONML'] = company_metadata.get('legal_name')
                if company_metadata.get('address_line1') and not companies[gvkey].get('ADD1'):
                    companies[gvkey]['ADD1'] = company_metadata.get('address_line1')
                if company_metadata.get('address_line2') and not companies[gvkey].get('ADD2'):
                    companies[gvkey]['ADD2'] = company_metadata.get('address_line2')
                if company_metadata.get('city') and not companies[gvkey].get('CITY'):
                    companies[gvkey]['CITY'] = company_metadata.get('city')
                if company_metadata.get('state') and not companies[gvkey].get('STATE'):
                    companies[gvkey]['STATE'] = company_metadata.get('state')
                if company_metadata.get('zip_code') and not companies[gvkey].get('ADDZIP'):
                    companies[gvkey]['ADDZIP'] = company_metadata.get('zip_code')
                # For FYRC, collect period end months to determine fiscal year end
                # Fiscal year end is typically the month that appears most often as period end
                period_end_month = company_metadata.get('period_end_month')
                fiscal_year_end_month = company_metadata.get('fiscal_year_end_month')
                
                if period_end_month:
                    if '_period_end_months' not in companies[gvkey]:
                        companies[gvkey]['_period_end_months'] = []
                    companies[gvkey]['_period_end_months'].append(period_end_month)
                
                if fiscal_year_end_month:
                    if '_fiscal_year_end_months' not in companies[gvkey]:
                        companies[gvkey]['_fiscal_year_end_months'] = []
                    companies[gvkey]['_fiscal_year_end_months'].append(fiscal_year_end_month)
                if company_metadata.get('sic') and not companies[gvkey].get('SIC'):
                    companies[gvkey]['SIC'] = company_metadata.get('sic')
                if company_metadata.get('phone') and not companies[gvkey].get('PHONE'):
                    companies[gvkey]['PHONE'] = company_metadata.get('phone')
                if company_metadata.get('website') and not companies[gvkey].get('WEBURL'):
                    companies[gvkey]['WEBURL'] = company_metadata.get('website')
                if company_metadata.get('ein') and not companies[gvkey].get('EIN'):
                    companies[gvkey]['EIN'] = company_metadata.get('ein')
                if company_metadata.get('business_description') and not companies[gvkey].get('BUSDESC'):
                    companies[gvkey]['BUSDESC'] = company_metadata.get('business_description')
                if company_metadata.get('state') and not companies[gvkey].get('STATE'):
                    companies[gvkey]['STATE'] = company_metadata.get('state')
                # Set defaults if not already set
                if not companies[gvkey].get('COSTAT'):
                    companies[gvkey]['COSTAT'] = 'A'
                if not companies[gvkey].get('FIC'):
                    companies[gvkey]['FIC'] = 'USA'
                if not companies[gvkey].get('LOC'):
                    companies[gvkey]['LOC'] = 'USA'
                if company_metadata.get('state') and not companies[gvkey].get('INCORP'):
                    companies[gvkey]['INCORP'] = company_metadata.get('state')
                # Update GICS codes if SIC is found
                if company_metadata.get('sic') and not companies[gvkey].get('GSECTOR'):
                    gics = self._get_gics_from_sic(company_metadata.get('sic'), gvkey)
                    if gics:
                        companies[gvkey]['GSECTOR'] = gics.get('GSECTOR')
                        companies[gvkey]['GGROUP'] = gics.get('GGROUP')
                        companies[gvkey]['GIND'] = gics.get('GIND')
                        companies[gvkey]['GSUBIND'] = gics.get('GSUBIND')
        
        if not companies:
            logger.warning("No company data to insert")
            return
        
        # Calculate FYRC (fiscal year end month) for each company
        # Use the most common period end month across all filings
        for gvkey, company in companies.items():
            from collections import Counter
            
            # Prefer explicit fiscal year end months if available
            if '_fiscal_year_end_months' in company and company['_fiscal_year_end_months']:
                fyrc_counts = Counter(company['_fiscal_year_end_months'])
                if fyrc_counts:
                    company['FYRC'] = fyrc_counts.most_common(1)[0][0]
                del company['_fiscal_year_end_months']
            # Otherwise, use period end months (most common = fiscal year end)
            elif '_period_end_months' in company and company['_period_end_months']:
                period_counts = Counter(company['_period_end_months'])
                if period_counts:
                    # The most common period end month is typically the fiscal year end
                    company['FYRC'] = period_counts.most_common(1)[0][0]
                del company['_period_end_months']
        
        # Insert into COMPANY table
        for gvkey, company in companies.items():
            try:
                # Format phone number: remove spaces, keep digits only, format as "XXX XXX XXXX"
                phone = company.get('PHONE', '')
                if phone:
                    import re
                    # Remove all non-digit characters
                    digits_only = re.sub(r'\D', '', str(phone))
                    # Format as "XXX XXX XXXX" if 10 digits
                    if len(digits_only) == 10:
                        phone = f"{digits_only[:3]} {digits_only[3:6]} {digits_only[6:]}"
                    elif len(digits_only) == 11 and digits_only[0] == '1':
                        # Remove leading 1 for US numbers
                        phone = f"{digits_only[1:4]} {digits_only[4:7]} {digits_only[7:]}"
                    else:
                        phone = digits_only  # Keep as-is if not standard format
                
                # Format legal name: capitalize properly (Title Case)
                legal_name = company.get('CONML', '')
                if legal_name:
                    # Convert to title case but preserve common abbreviations
                    legal_name = legal_name.title()
                    # Fix common abbreviations
                    legal_name = re.sub(r'\bCorp\b', 'Corp', legal_name)
                    legal_name = re.sub(r'\bInc\b', 'Inc', legal_name)
                    legal_name = re.sub(r'\bLtd\b', 'Ltd', legal_name)
                    legal_name = re.sub(r'\bCo\b', 'Co', legal_name)
                    legal_name = re.sub(r'\bLLC\b', 'LLC', legal_name)
                
                # Check if exists first
                existing = self.conn.execute("""
                    SELECT COUNT(*) FROM main.COMPANY WHERE GVKEY = ?
                """, [company['GVKEY']]).fetchone()[0]
                
                if existing > 0:
                    self.conn.execute("""
                        UPDATE main.COMPANY 
                        SET CIK = ?, CONM = ?, CONML = ?, ADD1 = ?, ADD2 = ?, 
                            CITY = ?, STATE = ?, ADDZIP = ?, FYRC = ?,
                            SIC = ?, PHONE = ?, WEBURL = ?, EIN = ?, BUSDESC = ?,
                            GSECTOR = ?, GGROUP = ?, GIND = ?, GSUBIND = ?
                        WHERE GVKEY = ?
                    """, [
                        company.get('CIK', ''),
                        company.get('CONM', ''),
                        legal_name,  # Use formatted legal name
                        company.get('ADD1', ''),
                        company.get('ADD2', ''),
                        company.get('CITY', ''),
                        company.get('STATE', ''),
                        company.get('ADDZIP', ''),
                        company.get('FYRC'),
                        company.get('SIC'),
                        phone,  # Use formatted phone
                        company.get('WEBURL', ''),
                        company.get('EIN', ''),
                        company.get('BUSDESC', ''),
                        company.get('GSECTOR'),
                        company.get('GGROUP'),
                        company.get('GIND'),
                        company.get('GSUBIND'),
                        company['GVKEY']
                    ])
                else:
                    self.conn.execute("""
                        INSERT INTO main.COMPANY 
                        (GVKEY, CIK, CONM, CONML, ADD1, ADD2, CITY, STATE, ADDZIP, FYRC,
                         SIC, PHONE, WEBURL, EIN, BUSDESC, GSECTOR, GGROUP, GIND, GSUBIND)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        company['GVKEY'],
                        company.get('CIK', ''),
                        company.get('CONM', ''),
                        legal_name,  # Use formatted legal name
                        company.get('ADD1', ''),
                        company.get('ADD2', ''),
                        company.get('CITY', ''),
                        company.get('STATE', ''),
                        company.get('ADDZIP', ''),
                        company.get('FYRC'),
                        company.get('SIC'),
                        phone,  # Use formatted phone
                        company.get('WEBURL', ''),
                        company.get('EIN', ''),
                        company.get('BUSDESC', ''),
                        company.get('GSECTOR'),
                        company.get('GGROUP'),
                        company.get('GIND'),
                        company.get('GSUBIND')
                    ])
            except Exception as e:
                logger.error(f"Error inserting company {gvkey}: {e}")
        
        logger.info(f"Populated COMPANY table with {len(companies)} companies")
    
    def populate_security_table(self, extracted_data: List[Dict[str, Any]]):
        """
        Populate SECURITY table from extracted data.
        
        Args:
            extracted_data: List of extracted data dictionaries
        """
        securities = {}
        
        for data in extracted_data:
            gvkey = data.get('gvkey')
            if not gvkey:
                continue
            
            security_data = data.get('security_data', {})
            ticker = security_data.get('ticker')
            
            if ticker:
                # Use IID = '01' as default for primary security
                iid = '01'
                key = (gvkey, iid)
                
                if key not in securities:
                    securities[key] = {
                        'GVKEY': gvkey,
                        'IID': iid,
                        'TIC': ticker,
                        'TPCI': '0',  # Primary issue
                    }
        
        if not securities:
            logger.warning("No security data to insert")
            return
        
        # Insert into SECURITY table
        for (gvkey, iid), security in securities.items():
            try:
                existing = self.conn.execute("""
                    SELECT COUNT(*) FROM main.SECURITY WHERE GVKEY = ? AND IID = ?
                """, [security['GVKEY'], security['IID']]).fetchone()[0]
                
                if existing > 0:
                    self.conn.execute("""
                        UPDATE main.SECURITY SET TIC = ?, TPCI = ? WHERE GVKEY = ? AND IID = ?
                    """, [security['TIC'], security['TPCI'], security['GVKEY'], security['IID']])
                else:
                    self.conn.execute("""
                        INSERT INTO main.SECURITY (GVKEY, IID, TIC, TPCI)
                        VALUES (?, ?, ?, ?)
                    """, [security['GVKEY'], security['IID'], security['TIC'], security['TPCI']])
            except Exception as e:
                logger.error(f"Error inserting security {gvkey}/{iid}: {e}")
        
        logger.info(f"Populated SECURITY table with {len(securities)} securities")
    
    def populate_sec_idcurrent_table(self, extracted_data: List[Dict[str, Any]]):
        """
        Populate SEC_IDCURRENT table from extracted data.
        
        Args:
            extracted_data: List of extracted data dictionaries
        """
        identifiers = []
        
        for data in extracted_data:
            gvkey = data.get('gvkey')
            if not gvkey:
                continue
            
            security_data = data.get('security_data', {})
            ticker = security_data.get('ticker')
            
            if ticker:
                # Default IID for primary security
                iid = '01'
                identifiers.append({
                    'GVKEY': gvkey,
                    'IID': iid,
                    'ITEM': 'TIC',
                    'ITEMVALUE': ticker,
                })
        
        if not identifiers:
            logger.warning("No identifier data to insert")
            return
        
        # Insert into SEC_IDCURRENT table
        for ident in identifiers:
            try:
                existing = self.conn.execute("""
                    SELECT COUNT(*) FROM main.SEC_IDCURRENT 
                    WHERE GVKEY = ? AND IID = ? AND ITEM = ?
                """, [ident['GVKEY'], ident['IID'], ident['ITEM']]).fetchone()[0]
                
                if existing > 0:
                    self.conn.execute("""
                        UPDATE main.SEC_IDCURRENT SET ITEMVALUE = ?
                        WHERE GVKEY = ? AND IID = ? AND ITEM = ?
                    """, [ident['ITEMVALUE'], ident['GVKEY'], ident['IID'], ident['ITEM']])
                else:
                    self.conn.execute("""
                        INSERT INTO main.SEC_IDCURRENT (GVKEY, IID, ITEM, ITEMVALUE)
                        VALUES (?, ?, ?, ?)
                    """, [ident['GVKEY'], ident['IID'], ident['ITEM'], ident['ITEMVALUE']])
            except Exception as e:
                logger.error(f"Error inserting identifier {ident['GVKEY']}/{ident['IID']}: {e}")
        
        logger.info(f"Populated SEC_IDCURRENT table with {len(identifiers)} identifiers")
    
    def populate_financial_tables(self, extracted_data: List[Dict[str, Any]]):
        """Populate financial tables (CSCO_IKEY, CSCO_IFNDQ)."""
        logger.info("Populating financial tables...")
        count = 0
        skipped = 0
        allowed_types = ('10-Q', '10-K')

        mapped_records = []
        for data in extracted_data:
            filing_type = (data.get('filing_type') or '').upper()
            period_end = data.get('document_period_end_date')

            if not filing_type or not any(ft in filing_type for ft in allowed_types):
                skipped += 1
                continue
            if not period_end:
                skipped += 1
                continue

            mapped = self.financial_mapper.map_financial_data(data)
            if mapped:
                mapped_records.append(mapped)

        mapped_records.sort(key=lambda m: (
            m['gvkey'],
            m['fiscal_year'],
            m['fiscal_quarter'],
            m['datadate'],
        ))

        self.financial_mapper.reset_ytd_tracker()

        for mapped in mapped_records:
            self.financial_mapper.insert_financial_data(mapped)
            count += 1

        logger.info(f"Populated financial tables with {count} records (skipped {skipped})")
    
    def populate_all_tables(self, extracted_data: List[Dict[str, Any]]):
        """
        Populate all tables from extracted data.
        
        Args:
            extracted_data: List of extracted data dictionaries
        """
        logger.info("Populating all tables...")
        self.populate_company_table(extracted_data)
        self.populate_security_table(extracted_data)
        self.populate_sec_idcurrent_table(extracted_data)
        self.populate_financial_tables(extracted_data)
        logger.info("All tables populated")
    
    def _get_gics_from_sic(self, sic: str, gvkey: str = None) -> Optional[Dict[str, Any]]:
        """
        Get GICS codes from SIC code by querying source Compustat database.
        If GVKEY is provided, get exact GICS for that company. Otherwise, use mode.
        
        Args:
            sic: SIC code (4-digit string)
            gvkey: Optional GVKEY to get exact GICS codes for specific company
            
        Returns:
            Dictionary with GSECTOR, GGROUP, GIND, GSUBIND or None
        """
        if not sic or len(sic) != 4:
            return None
        
        try:
            import duckdb
            source_db = duckdb.connect('/home/tasos/compustat.duckdb', read_only=True)
            
            # If GVKEY provided, get exact GICS for that company
            if gvkey:
                result = source_db.execute('''
                    SELECT GSECTOR, GGROUP, GIND, GSUBIND
                    FROM main.COMPANY
                    WHERE GVKEY = ? AND GSECTOR IS NOT NULL AND GSECTOR != ''
                ''', [gvkey]).fetchone()
                
                if result and result[0]:
                    source_db.close()
                    return {
                        'GSECTOR': result[0],
                        'GGROUP': result[1],
                        'GIND': result[2],
                        'GSUBIND': result[3]
                    }
            
            # Otherwise, get most common GICS codes for this SIC
            result = source_db.execute('''
                SELECT GSECTOR, GGROUP, GIND, GSUBIND, COUNT(*) as cnt
                FROM main.COMPANY
                WHERE SIC = ? AND GSECTOR IS NOT NULL AND GSECTOR != ''
                GROUP BY GSECTOR, GGROUP, GIND, GSUBIND
                ORDER BY cnt DESC
                LIMIT 1
            ''', [sic]).fetchone()
            
            source_db.close()
            
            if result and result[0]:
                return {
                    'GSECTOR': result[0],
                    'GGROUP': result[1],
                    'GIND': result[2],
                    'GSUBIND': result[3]
                }
        except Exception as e:
            logger.warning(f"Could not get GICS from SIC {sic}: {e}")
        
        return None
    
    def close(self):
        """Close database connection."""
        self.conn.close()
        self.financial_mapper.close()

