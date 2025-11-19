"""
Build compustat_edgar.duckdb database from SEC filings.
"""
import sys
import duckdb
from pathlib import Path
from typing import List
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import COMPUSTAT_SOURCE_DB, COMPUSTAT_EDGAR_DB
from src.schema_mapper import SchemaMapper

logger = logging.getLogger(__name__)


class DatabaseBuilder:
    """Build the compustat_edgar database."""
    
    def __init__(self):
        """Initialize database builder."""
        self.target_db = COMPUSTAT_EDGAR_DB
        self.source_db = COMPUSTAT_SOURCE_DB
        self.conn = duckdb.connect(str(self.target_db))
        self.schema_mapper = SchemaMapper(self.source_db, self.target_db)
    
    def initialize_schema(self, table_names: List[str] = None):
        """
        Initialize target database schema from source.
        
        Args:
            table_names: List of table names to create. If None, creates key tables.
        """
        if table_names is None:
            # Key tables to replicate
            table_names = [
                'COMPANY',
                'SECURITY',
                'SEC_IDCURRENT',
                'SEC_DPRC',
                'FUNDA',  # Annual fundamentals
                'FUNDQ',  # Quarterly fundamentals
                'FUNDY',  # Year-to-date fundamentals
            ]
        
        logger.info(f"Initializing schema with {len(table_names)} tables")
        self.schema_mapper.initialize_target_schema(table_names)
    
    def get_key_tables(self) -> List[str]:
        """
        Get list of key tables from source database.
        
        Returns:
            List of table names
        """
        self.conn.execute(f"ATTACH '{self.source_db}' AS source (READ_ONLY)")
        
        tables = self.conn.execute("""
            SELECT table_name 
            FROM source.information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()
        
        self.conn.execute("DETACH source")
        return [t[0] for t in tables]
    
    def close(self):
        """Close database connections."""
        self.schema_mapper.close()
        self.conn.close()

