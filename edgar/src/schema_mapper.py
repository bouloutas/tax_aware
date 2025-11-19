"""
Schema mapping utilities to map EDGAR filing data to Compustat database schema.
"""
import duckdb
from pathlib import Path
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


class SchemaMapper:
    """Maps EDGAR filing data to Compustat database schema."""
    
    def __init__(self, source_db_path: Path, target_db_path: Path):
        """
        Initialize schema mapper.
        
        Args:
            source_db_path: Path to source Compustat database
            target_db_path: Path to target compustat_edgar database
        """
        self.source_db_path = source_db_path
        self.target_db_path = target_db_path
        self.conn = duckdb.connect(str(target_db_path))
        
    def get_table_schema(self, table_name: str) -> List[Tuple[str, str]]:
        """
        Get schema for a table from source database.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of (column_name, data_type) tuples
        """
        self.conn.execute(f"ATTACH '{self.source_db_path}' AS source (READ_ONLY)")
        
        # Use PRAGMA table_info for DuckDB
        schema = self.conn.execute(f"PRAGMA table_info(source.main.{table_name})").fetchall()
        # PRAGMA returns: (cid, name, type, notnull, dflt_value, pk)
        result = [(row[1], row[2]) for row in schema]
        
        self.conn.execute("DETACH source")
        return result
    
    def create_table_from_schema(self, table_name: str):
        """
        Create a table in target database matching source schema.
        
        Args:
            table_name: Name of the table to create
        """
        schema = self.get_table_schema(table_name)
        
        columns = ", ".join([f"{col} {dtype}" for col, dtype in schema])
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS main.{table_name} (
            {columns}
        )
        """
        
        self.conn.execute(create_sql)
        logger.info(f"Created table {table_name} with {len(schema)} columns")
    
    def initialize_target_schema(self, table_names: List[str]):
        """
        Initialize target database with schema from source.
        
        Args:
            table_names: List of table names to create
        """
        logger.info(f"Initializing target schema with {len(table_names)} tables")
        for table_name in table_names:
            try:
                self.create_table_from_schema(table_name)
            except Exception as e:
                logger.error(f"Error creating table {table_name}: {e}")
    
    def close(self):
        """Close database connection."""
        self.conn.close()

