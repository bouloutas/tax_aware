"""
GVKEY to Ticker mapping utility.

Maps Compustat GVKEY identifiers to ticker symbols for Barra data integration.
"""
from pathlib import Path
from typing import Optional

import pandas as pd


class GVKEYMapper:
    """Maps GVKEY to ticker symbols."""

    def __init__(self, mapping_file: Optional[Path] = None):
        """
        Initialize GVKEYMapper.

        Args:
            mapping_file: Path to GVKEY-ticker mapping CSV file
        """
        if mapping_file is None:
            project_root = Path(__file__).parent.parent.parent
            mapping_file = project_root / "data" / "raw" / "barra" / "gvkey_ticker_mapping.csv"

        self.mapping_file = Path(mapping_file)
        self._mapping_df: Optional[pd.DataFrame] = None

    def load_mapping(self) -> pd.DataFrame:
        """Load GVKEY to ticker mapping."""
        if self._mapping_df is not None:
            return self._mapping_df

        if not self.mapping_file.exists():
            raise FileNotFoundError(f"Mapping file not found: {self.mapping_file}")

        self._mapping_df = pd.read_csv(self.mapping_file)
        return self._mapping_df

    def gvkey_to_ticker(self, gvkey: str) -> Optional[str]:
        """
        Convert GVKEY to ticker.

        Args:
            gvkey: GVKEY identifier

        Returns:
            Ticker symbol or None if not found
        """
        mapping = self.load_mapping()
        
        # Handle different possible column names
        gvkey_col = None
        ticker_col = None
        
        for col in mapping.columns:
            if col.upper() in ['GVKEY', 'GKEY']:
                gvkey_col = col
            elif col.upper() in ['TICKER', 'SYMBOL']:
                ticker_col = col
        
        if gvkey_col is None or ticker_col is None:
            raise ValueError(f"Could not find GVKEY/ticker columns. Available: {list(mapping.columns)}")
        
        # Normalize GVKEY (handle leading zeros)
        gvkey_normalized = str(gvkey).lstrip('0') or '0'
        
        # Try exact match first
        result = mapping[mapping[gvkey_col].astype(str) == str(gvkey)]
        
        # If not found, try normalized match
        if result.empty:
            result = mapping[mapping[gvkey_col].astype(str).str.lstrip('0') == gvkey_normalized]

        if result.empty:
            return None

        return result[ticker_col].iloc[0]

    def ticker_to_gvkey(self, ticker: str) -> Optional[str]:
        """
        Convert ticker to GVKEY.

        Args:
            ticker: Ticker symbol

        Returns:
            GVKEY identifier or None if not found
        """
        mapping = self.load_mapping()
        
        # Handle different possible column names
        gvkey_col = None
        ticker_col = None
        
        for col in mapping.columns:
            if col.upper() in ['GVKEY', 'GKEY']:
                gvkey_col = col
            elif col.upper() in ['TICKER', 'SYMBOL']:
                ticker_col = col
        
        if gvkey_col is None or ticker_col is None:
            raise ValueError(f"Could not find GVKEY/ticker columns. Available: {list(mapping.columns)}")
        
        result = mapping[mapping[ticker_col] == ticker.upper()]

        if result.empty:
            return None

        return str(result[gvkey_col].iloc[0])

    def get_all_mappings(self) -> pd.DataFrame:
        """Get all GVKEY-ticker mappings."""
        return self.load_mapping().copy()

