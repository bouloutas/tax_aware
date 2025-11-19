#!/usr/bin/env python3
"""
Initialize the compustat_edgar.duckdb database with schema from compustat.duckdb.
"""
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.database_builder import DatabaseBuilder
from config import COMPUSTAT_EDGAR_DB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Initialize the target database schema."""
    logger.info("Initializing compustat_edgar.duckdb database...")
    
    # Remove existing database if it exists
    if COMPUSTAT_EDGAR_DB.exists():
        logger.warning(f"Existing database found: {COMPUSTAT_EDGAR_DB}")
        response = input("Delete and recreate? (y/n): ")
        if response.lower() == 'y':
            COMPUSTAT_EDGAR_DB.unlink()
            logger.info("Deleted existing database")
        else:
            logger.info("Keeping existing database")
    
    builder = DatabaseBuilder()
    
    try:
        # Initialize with key tables
        builder.initialize_schema()
        logger.info("Database schema initialized successfully")
        
        # Show what was created
        tables = builder.conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()
        
        logger.info(f"Created {len(tables)} tables:")
        for table_name, in tables:
            try:
                count = builder.conn.execute(f"SELECT COUNT(*) FROM main.{table_name}").fetchone()[0]
                logger.info(f"  - {table_name}: {count} rows")
            except Exception as e:
                logger.debug(f"  - {table_name}: (error getting count: {e})")
            
    finally:
        builder.close()


if __name__ == "__main__":
    main()

