#!/usr/bin/env python3
"""
duckdb_manager.py

Database connection manager for DuckDB operations.
Handles connections to both Compustat and Fama-French databases.
"""

import duckdb
import os
from contextlib import contextmanager
import pandas as pd
from typing import Optional, List, Any, Union

class DuckDBManager:
    """
    Manages DuckDB connections for the Fama-French project.
    
    Database separation:
    - Compustat/SPGlobal data: /home/tasos/T9_APFS/compustat.duckdb
    - All other data: /home/tasos/T9_APFS/fama_french.duckdb
    """
    
    def __init__(self):
        self.compustat_path = '/home/tasos/T9_APFS/compustat.duckdb'
        self.ff_path = '/home/tasos/T9_APFS/fama_french.duckdb'
        
        # Ensure directories exist
        os.makedirs(os.path.dirname(self.compustat_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.ff_path), exist_ok=True)
    
    @contextmanager
    def get_compustat_connection(self):
        """Get connection to Compustat database"""
        conn = duckdb.connect(self.compustat_path)
        try:
            yield conn
        finally:
            conn.close()
    
    @contextmanager
    def get_ff_connection(self):
        """Get connection to Fama-French database"""
        conn = duckdb.connect(self.ff_path)
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, database: str = 'ff', params: Optional[List[Any]] = None) -> List[Any]:
        """
        Execute query on specified database
        
        Args:
            query: SQL query string
            database: 'compustat' or 'ff' (default: 'ff')
            params: Optional parameters for prepared statements
            
        Returns:
            Query results as list of tuples
        """
        if database == 'compustat':
            with self.get_compustat_connection() as conn:
                if params:
                    return conn.execute(query, params).fetchall()
                else:
                    return conn.execute(query).fetchall()
        else:
            with self.get_ff_connection() as conn:
                if params:
                    return conn.execute(query, params).fetchall()
                else:
                    return conn.execute(query).fetchall()
    
    def read_sql(self, query: str, database: str = 'ff') -> pd.DataFrame:
        """
        Read SQL query into pandas DataFrame
        
        Args:
            query: SQL query string
            database: 'compustat' or 'ff' (default: 'ff')
            
        Returns:
            pandas DataFrame with query results
        """
        if database == 'compustat':
            with self.get_compustat_connection() as conn:
                return conn.execute(query).df()
        else:
            with self.get_ff_connection() as conn:
                return conn.execute(query).df()
    
    def write_dataframe(self, df: pd.DataFrame, table_name: str, database: str = 'ff', 
                       if_exists: str = 'replace') -> None:
        """
        Write pandas DataFrame to DuckDB table
        
        Args:
            df: pandas DataFrame to write
            table_name: Target table name
            database: 'compustat' or 'ff' (default: 'ff')
            if_exists: 'replace', 'append', or 'fail' (default: 'replace')
        """
        if database == 'compustat':
            with self.get_compustat_connection() as conn:
                if if_exists == 'replace':
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        else:
            with self.get_ff_connection() as conn:
                if if_exists == 'replace':
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    
    def table_exists(self, table_name: str, database: str = 'ff') -> bool:
        """
        Check if table exists in database
        
        Args:
            table_name: Table name to check
            database: 'compustat' or 'ff' (default: 'ff')
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            if database == 'compustat':
                with self.get_compustat_connection() as conn:
                    result = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
                    return result[0] > 0
            else:
                with self.get_ff_connection() as conn:
                    result = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
                    return result[0] > 0
        except:
            return False
    
    def get_table_info(self, table_name: str, database: str = 'ff') -> pd.DataFrame:
        """
        Get table schema information
        
        Args:
            table_name: Table name
            database: 'compustat' or 'ff' (default: 'ff')
            
        Returns:
            DataFrame with table schema information
        """
        query = f"DESCRIBE {table_name}"
        return self.read_sql(query, database)
    
    def get_table_count(self, table_name: str, database: str = 'ff') -> int:
        """
        Get row count for table
        
        Args:
            table_name: Table name
            database: 'compustat' or 'ff' (default: 'ff')
            
        Returns:
            Number of rows in table
        """
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute_query(query, database)
        return result[0][0] if result else 0
    
    def create_schema(self, database: str = 'ff') -> None:
        """
        Create database schema with all required tables
        
        Args:
            database: 'compustat' or 'ff' (default: 'ff')
        """
        if database == 'compustat':
            schema_sql = """
            -- Compustat database schema
            CREATE TABLE IF NOT EXISTS data_for_factor_construction (
                GVKEY VARCHAR,
                IID VARCHAR,
                FORMATION_YEAR_T INTEGER,
                RETURN_MONTH_END_DATE DATE,
                MONTHLY_RETURN DECIMAL(10,6),
                EXCHG VARCHAR,
                ME_JUNE DECIMAL(15,2),
                BE_FY_T_MINUS_1 DECIMAL(15,2),
                OP_FY_T_MINUS_1 DECIMAL(15,2),
                INV_FY_T_MINUS_1 DECIMAL(15,2),
                BM_T_MINUS_1 DECIMAL(10,6)
            );
            """
            with self.get_compustat_connection() as conn:
                conn.execute(schema_sql)
        else:
            schema_sql = """
            -- Fama-French database schema
            CREATE TABLE IF NOT EXISTS final_combined_factors (
                Date DATE PRIMARY KEY,
                Mkt_RF DECIMAL(10,6),
                RF DECIMAL(10,6),
                SMB DECIMAL(10,6),
                HML DECIMAL(10,6),
                RMW DECIMAL(10,6),
                CMA DECIMAL(10,6)
            );
            
            CREATE TABLE IF NOT EXISTS optimization_portfolio_monthly_returns (
                TICKER VARCHAR,
                MONTH_END_DATE DATE,
                GVKEY VARCHAR,
                MONTHLY_RETURN DECIMAL(10,6)
            );
            
            CREATE TABLE IF NOT EXISTS ken_french_factors (
                Date DATE PRIMARY KEY,
                Mkt_RF DECIMAL(10,6),
                SMB DECIMAL(10,6),
                HML DECIMAL(10,6),
                RMW DECIMAL(10,6),
                CMA DECIMAL(10,6),
                RF DECIMAL(10,6)
            );
            
            CREATE TABLE IF NOT EXISTS my_constructed_factors (
                Date DATE,
                SMB DECIMAL(10,6),
                HML DECIMAL(10,6),
                RMW DECIMAL(10,6),
                CMA DECIMAL(10,6)
            );
            
            CREATE TABLE IF NOT EXISTS universe_factor_scores (
                ticker VARCHAR,
                datadate DATE,
                factor_score DECIMAL(10,6),
                decile INTEGER,
                company_name VARCHAR,
                _5d_forward_return DECIMAL(10,6)
            );
            """
            with self.get_ff_connection() as conn:
                conn.execute(schema_sql)
    
    def backup_database(self, database: str = 'ff', backup_path: Optional[str] = None) -> str:
        """
        Create backup of database
        
        Args:
            database: 'compustat' or 'ff' (default: 'ff')
            backup_path: Optional custom backup path
            
        Returns:
            Path to backup file
        """
        import shutil
        from datetime import datetime
        
        if database == 'compustat':
            source_path = self.compustat_path
        else:
            source_path = self.ff_path
        
        if not backup_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"{source_path}.backup_{timestamp}"
        
        shutil.copy2(source_path, backup_path)
        return backup_path
    
    def optimize_database(self, database: str = 'ff') -> None:
        """
        Optimize database for better performance
        
        Args:
            database: 'compustat' or 'ff' (default: 'ff')
        """
        if database == 'compustat':
            with self.get_compustat_connection() as conn:
                conn.execute("PRAGMA optimize")
        else:
            with self.get_ff_connection() as conn:
                conn.execute("PRAGMA optimize")


# Convenience functions for backward compatibility
def get_duckdb_manager() -> DuckDBManager:
    """Get DuckDB manager instance"""
    return DuckDBManager()

def get_compustat_connection():
    """Get Compustat connection (for backward compatibility)"""
    manager = DuckDBManager()
    return manager.get_compustat_connection()

def get_ff_connection():
    """Get Fama-French connection (for backward compatibility)"""
    manager = DuckDBManager()
    return manager.get_ff_connection()


if __name__ == "__main__":
    # Test the DuckDB manager
    manager = DuckDBManager()
    
    print("Testing DuckDB Manager...")
    
    # Create schemas
    print("Creating schemas...")
    manager.create_schema('compustat')
    manager.create_schema('ff')
    
    # Test connections
    print("Testing connections...")
    with manager.get_compustat_connection() as conn:
        print("Compustat connection: OK")
    
    with manager.get_ff_connection() as conn:
        print("Fama-French connection: OK")
    
    print("DuckDB Manager test completed successfully!")
