"""
Map SIC codes to GICS codes using Compustat data.
"""
import duckdb
from pathlib import Path
from typing import Optional, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

# Hardcoded mapping for MSFT and NVDA (can be expanded)
SIC_TO_GICS_MAPPING = {
    '7372': {  # Software - MSFT
        'GSECTOR': '45',
        'GGROUP': '4510',
        'GIND': '451030',
        'GSUBIND': '45103020',
    },
    '3674': {  # Semiconductors - NVDA
        'GSECTOR': '45',
        'GGROUP': '4530',
        'GIND': '453010',
        'GSUBIND': '45301010',
    },
}

def get_gics_from_sic(sic_code: str, source_db_path: Optional[Path] = None) -> Optional[Dict[str, str]]:
    """
    Get GICS codes from SIC code.
    
    Args:
        sic_code: 4-digit SIC code
        source_db_path: Path to source Compustat database (optional)
        
    Returns:
        Dictionary with GSECTOR, GGROUP, GIND, GSUBIND or None
    """
    if not sic_code or len(sic_code) != 4:
        return None
    
    # First try hardcoded mapping
    if sic_code in SIC_TO_GICS_MAPPING:
        return SIC_TO_GICS_MAPPING[sic_code]
    
    # Try to get from Compustat database if available
    if source_db_path and source_db_path.exists():
        try:
            conn = duckdb.connect(str(source_db_path), read_only=True)
            # Find a company with this SIC code and get its GICS codes
            result = conn.execute("""
                SELECT GSECTOR, GGROUP, GIND, GSUBIND
                FROM main.COMPANY
                WHERE SIC = ?
                AND GSECTOR IS NOT NULL
                LIMIT 1
            """, [sic_code]).fetchone()
            conn.close()
            
            if result and result[0]:
                return {
                    'GSECTOR': str(result[0]) if result[0] else None,
                    'GGROUP': str(result[1]) if result[1] else None,
                    'GIND': str(result[2]) if result[2] else None,
                    'GSUBIND': str(result[3]) if result[3] else None,
                }
        except Exception as e:
            logger.debug(f"Could not get GICS from database: {e}")
    
    return None

