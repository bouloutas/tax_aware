#!/usr/bin/env python3
"""
test_duckdb_migration.py

Comprehensive test script to validate the MySQL to DuckDB migration.
Tests data integrity, functionality, and performance.
"""

import duckdb
import pandas as pd
import numpy as np
import time
import sys
import os
from datetime import datetime
from duckdb_manager import DuckDBManager
from sqlalchemy import create_engine

class MigrationTester:
    """
    Comprehensive tester for DuckDB migration validation.
    """
    
    def __init__(self):
        self.duckdb_manager = DuckDBManager()
        
        # MySQL connection for comparison
        self.mysql_ff_url = "mysql+pymysql://root:@localhost/fama_french_local?unix_socket=/Users/tasosbouloutas/mysql_data/mysql.sock"
        self.mysql_manual_url = "mysql+pymysql://root:@localhost/manual_weights?unix_socket=/Users/tasosbouloutas/mysql_data/mysql.sock"
        
        self.mysql_ff_engine = None
        self.mysql_manual_engine = None
        
        self.test_results = []
    
    def connect_mysql(self):
        """Connect to MySQL for comparison testing"""
        try:
            self.mysql_ff_engine = create_engine(self.mysql_ff_url)
            self.mysql_manual_engine = create_engine(self.mysql_manual_url)
            
            # Test connections
            with self.mysql_ff_engine.connect() as conn:
                conn.execute("SELECT 1")
            with self.mysql_manual_engine.connect() as conn:
                conn.execute("SELECT 1")
            
            print("✓ MySQL connections established for comparison testing")
            return True
        except Exception as e:
            print(f"⚠ Could not connect to MySQL for comparison: {e}")
            return False
    
    def disconnect_mysql(self):
        """Disconnect from MySQL"""
        if self.mysql_ff_engine:
            self.mysql_ff_engine.dispose()
        if self.mysql_manual_engine:
            self.mysql_manual_engine.dispose()
        print("✓ MySQL connections closed")
    
    def log_test_result(self, test_name: str, passed: bool, details: str = ""):
        """Log test result"""
        result = {
            'test_name': test_name,
            'passed': passed,
            'details': details,
            'timestamp': datetime.now()
        }
        self.test_results.append(result)
        
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")
        if details:
            print(f"    {details}")
    
    def test_database_connections(self):
        """Test DuckDB database connections"""
        print("\n--- Testing Database Connections ---")
        
        try:
            # Test Compustat connection
            with self.duckdb_manager.get_compustat_connection() as conn:
                result = conn.execute("SELECT 1").fetchone()
                if result[0] == 1:
                    self.log_test_result("Compustat DB Connection", True)
                else:
                    self.log_test_result("Compustat DB Connection", False, "Unexpected result")
        except Exception as e:
            self.log_test_result("Compustat DB Connection", False, str(e))
        
        try:
            # Test Fama-French connection
            with self.duckdb_manager.get_ff_connection() as conn:
                result = conn.execute("SELECT 1").fetchone()
                if result[0] == 1:
                    self.log_test_result("Fama-French DB Connection", True)
                else:
                    self.log_test_result("Fama-French DB Connection", False, "Unexpected result")
        except Exception as e:
            self.log_test_result("Fama-French DB Connection", False, str(e))
    
    def test_table_existence(self):
        """Test that all required tables exist"""
        print("\n--- Testing Table Existence ---")
        
        # Compustat tables
        compustat_tables = ['data_for_factor_construction']
        for table in compustat_tables:
            exists = self.duckdb_manager.table_exists(table, 'compustat')
            self.log_test_result(f"Table {table} (Compustat)", exists, 
                               f"Table {'exists' if exists else 'missing'}")
        
        # Fama-French tables
        ff_tables = [
            'final_combined_factors',
            'optimization_portfolio_monthly_returns',
            'ken_french_factors',
            'my_constructed_factors',
            'universe_factor_scores'
        ]
        for table in ff_tables:
            exists = self.duckdb_manager.table_exists(table, 'ff')
            self.log_test_result(f"Table {table} (FF)", exists,
                               f"Table {'exists' if exists else 'missing'}")
    
    def test_data_integrity(self):
        """Test data integrity by comparing row counts"""
        print("\n--- Testing Data Integrity ---")
        
        if not self.connect_mysql():
            print("⚠ Skipping data integrity tests - MySQL not available")
            return
        
        try:
            # Test Compustat data
            duckdb_count = self.duckdb_manager.get_table_count('data_for_factor_construction', 'compustat')
            mysql_count = pd.read_sql("SELECT COUNT(*) as cnt FROM data_for_factor_construction", 
                                    self.mysql_ff_engine).iloc[0]['cnt']
            
            if duckdb_count == mysql_count:
                self.log_test_result("Compustat Data Integrity", True, 
                                   f"Both databases have {duckdb_count} rows")
            else:
                self.log_test_result("Compustat Data Integrity", False,
                                   f"DuckDB: {duckdb_count}, MySQL: {mysql_count}")
            
            # Test FF data
            ff_tables = [
                'final_combined_factors',
                'optimization_portfolio_monthly_returns',
                'ken_french_factors',
                'my_constructed_factors'
            ]
            
            for table in ff_tables:
                try:
                    duckdb_count = self.duckdb_manager.get_table_count(table, 'ff')
                    mysql_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", 
                                            self.mysql_ff_engine).iloc[0]['cnt']
                    
                    if duckdb_count == mysql_count:
                        self.log_test_result(f"{table} Data Integrity", True,
                                           f"Both databases have {duckdb_count} rows")
                    else:
                        self.log_test_result(f"{table} Data Integrity", False,
                                           f"DuckDB: {duckdb_count}, MySQL: {mysql_count}")
                except Exception as e:
                    self.log_test_result(f"{table} Data Integrity", False, str(e))
            
            # Test manual weights data
            try:
                duckdb_count = self.duckdb_manager.get_table_count('universe_factor_scores', 'ff')
                mysql_count = pd.read_sql("SELECT COUNT(*) as cnt FROM universe_factor_scores", 
                                        self.mysql_manual_engine).iloc[0]['cnt']
                
                if duckdb_count == mysql_count:
                    self.log_test_result("Manual Weights Data Integrity", True,
                                       f"Both databases have {duckdb_count} rows")
                else:
                    self.log_test_result("Manual Weights Data Integrity", False,
                                       f"DuckDB: {duckdb_count}, MySQL: {mysql_count}")
            except Exception as e:
                self.log_test_result("Manual Weights Data Integrity", False, str(e))
                
        finally:
            self.disconnect_mysql()
    
    def test_data_quality(self):
        """Test data quality and schema consistency"""
        print("\n--- Testing Data Quality ---")
        
        # Test Compustat data quality
        try:
            df = self.duckdb_manager.read_sql("SELECT * FROM data_for_factor_construction LIMIT 5", 'compustat')
            required_cols = ['GVKEY', 'IID', 'FORMATION_YEAR_T', 'RETURN_MONTH_END_DATE', 'MONTHLY_RETURN']
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if not missing_cols:
                self.log_test_result("Compustat Schema", True, "All required columns present")
            else:
                self.log_test_result("Compustat Schema", False, f"Missing columns: {missing_cols}")
            
            # Check for null values in critical columns
            null_check = df[required_cols].isnull().sum()
            if null_check.sum() == 0:
                self.log_test_result("Compustat Data Quality", True, "No nulls in sample")
            else:
                self.log_test_result("Compustat Data Quality", False, f"Nulls found: {null_check.to_dict()}")
                
        except Exception as e:
            self.log_test_result("Compustat Data Quality", False, str(e))
        
        # Test FF data quality
        try:
            df = self.duckdb_manager.read_sql("SELECT * FROM final_combined_factors LIMIT 5", 'ff')
            required_cols = ['Date', 'Mkt_RF', 'RF', 'SMB', 'HML', 'RMW', 'CMA']
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if not missing_cols:
                self.log_test_result("FF Schema", True, "All required columns present")
            else:
                self.log_test_result("FF Schema", False, f"Missing columns: {missing_cols}")
                
        except Exception as e:
            self.log_test_result("FF Data Quality", False, str(e))
        
        # Test universe scores data quality
        try:
            df = self.duckdb_manager.read_sql("SELECT * FROM universe_factor_scores LIMIT 5", 'ff')
            required_cols = ['ticker', 'datadate', 'factor_score', 'decile']
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if not missing_cols:
                self.log_test_result("Universe Scores Schema", True, "All required columns present")
            else:
                self.log_test_result("Universe Scores Schema", False, f"Missing columns: {missing_cols}")
                
        except Exception as e:
            self.log_test_result("Universe Scores Data Quality", False, str(e))
    
    def test_query_performance(self):
        """Test query performance comparison"""
        print("\n--- Testing Query Performance ---")
        
        if not self.connect_mysql():
            print("⚠ Skipping performance tests - MySQL not available")
            return
        
        try:
            # Test complex query performance
            query = """
            SELECT GVKEY, COUNT(*) as cnt, AVG(MONTHLY_RETURN) as avg_return
            FROM data_for_factor_construction 
            WHERE FORMATION_YEAR_T >= 2020
            GROUP BY GVKEY
            ORDER BY cnt DESC
            LIMIT 100
            """
            
            # DuckDB performance
            start_time = time.time()
            duckdb_result = self.duckdb_manager.read_sql(query, 'compustat')
            duckdb_time = time.time() - start_time
            
            # MySQL performance
            start_time = time.time()
            mysql_result = pd.read_sql(query, self.mysql_ff_engine)
            mysql_time = time.time() - start_time
            
            # Compare results
            if len(duckdb_result) == len(mysql_result):
                self.log_test_result("Query Performance", True, 
                                   f"DuckDB: {duckdb_time:.3f}s, MySQL: {mysql_time:.3f}s")
            else:
                self.log_test_result("Query Performance", False,
                                   f"Result count mismatch: DuckDB {len(duckdb_result)}, MySQL {len(mysql_result)}")
                
        except Exception as e:
            self.log_test_result("Query Performance", False, str(e))
        finally:
            self.disconnect_mysql()
    
    def test_functionality(self):
        """Test core functionality with sample operations"""
        print("\n--- Testing Core Functionality ---")
        
        try:
            # Test factor construction data access
            df = self.duckdb_manager.read_sql("""
                SELECT FORMATION_YEAR_T, COUNT(*) as stock_count
                FROM data_for_factor_construction 
                GROUP BY FORMATION_YEAR_T 
                ORDER BY FORMATION_YEAR_T DESC 
                LIMIT 5
            """, 'compustat')
            
            if not df.empty and 'FORMATION_YEAR_T' in df.columns:
                self.log_test_result("Factor Construction Query", True, 
                                   f"Retrieved {len(df)} formation years")
            else:
                self.log_test_result("Factor Construction Query", False, "Empty or malformed result")
                
        except Exception as e:
            self.log_test_result("Factor Construction Query", False, str(e))
        
        try:
            # Test portfolio optimization data access
            df = self.duckdb_manager.read_sql("""
                SELECT TICKER, COUNT(*) as return_count, AVG(MONTHLY_RETURN) as avg_return
                FROM optimization_portfolio_monthly_returns 
                GROUP BY TICKER 
                ORDER BY return_count DESC 
                LIMIT 10
            """, 'ff')
            
            if not df.empty and 'TICKER' in df.columns:
                self.log_test_result("Portfolio Optimization Query", True,
                                   f"Retrieved {len(df)} tickers")
            else:
                self.log_test_result("Portfolio Optimization Query", False, "Empty or malformed result")
                
        except Exception as e:
            self.log_test_result("Portfolio Optimization Query", False, str(e))
        
        try:
            # Test universe scores data access
            df = self.duckdb_manager.read_sql("""
                SELECT datadate, COUNT(*) as score_count, AVG(factor_score) as avg_score
                FROM universe_factor_scores 
                GROUP BY datadate 
                ORDER BY datadate DESC 
                LIMIT 5
            """, 'ff')
            
            if not df.empty and 'datadate' in df.columns:
                self.log_test_result("Universe Scores Query", True,
                                   f"Retrieved {len(df)} dates")
            else:
                self.log_test_result("Universe Scores Query", False, "Empty or malformed result")
                
        except Exception as e:
            self.log_test_result("Universe Scores Query", False, str(e))
    
    def test_edge_cases(self):
        """Test edge cases and error handling"""
        print("\n--- Testing Edge Cases ---")
        
        try:
            # Test non-existent table
            result = self.duckdb_manager.read_sql("SELECT * FROM non_existent_table", 'ff')
            self.log_test_result("Non-existent Table Handling", False, "Should have raised exception")
        except Exception as e:
            self.log_test_result("Non-existent Table Handling", True, "Properly raised exception")
        
        try:
            # Test invalid SQL
            result = self.duckdb_manager.read_sql("INVALID SQL SYNTAX", 'ff')
            self.log_test_result("Invalid SQL Handling", False, "Should have raised exception")
        except Exception as e:
            self.log_test_result("Invalid SQL Handling", True, "Properly raised exception")
        
        try:
            # Test empty result set
            result = self.duckdb_manager.read_sql("""
                SELECT * FROM final_combined_factors 
                WHERE Date > '2099-12-31'
            """, 'ff')
            
            if result.empty:
                self.log_test_result("Empty Result Handling", True, "Properly handled empty result")
            else:
                self.log_test_result("Empty Result Handling", False, "Unexpected non-empty result")
                
        except Exception as e:
            self.log_test_result("Empty Result Handling", False, str(e))
    
    def print_test_summary(self):
        """Print comprehensive test summary"""
        print("\n" + "="*60)
        print("MIGRATION TEST SUMMARY")
        print("="*60)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['passed'])
        failed_tests = total_tests - passed_tests
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print("\nFAILED TESTS:")
            for result in self.test_results:
                if not result['passed']:
                    print(f"  ✗ {result['test_name']}: {result['details']}")
        
        print("\n" + "="*60)
        
        if failed_tests == 0:
            print("🎉 ALL TESTS PASSED! Migration is successful.")
            return True
        else:
            print("❌ Some tests failed. Please review the issues above.")
            return False
    
    def run_all_tests(self):
        """Run all migration tests"""
        print("Starting DuckDB Migration Validation Tests")
        print(f"Test started at: {datetime.now()}")
        
        self.test_database_connections()
        self.test_table_existence()
        self.test_data_integrity()
        self.test_data_quality()
        self.test_query_performance()
        self.test_functionality()
        self.test_edge_cases()
        
        return self.print_test_summary()


def main():
    """Main test function"""
    tester = MigrationTester()
    
    print("DuckDB Migration Validation Test Suite")
    print("This will test the migration from MySQL to DuckDB")
    print()
    
    success = tester.run_all_tests()
    
    if success:
        print("\n✅ Migration validation completed successfully!")
        print("Your DuckDB migration is ready for production use.")
    else:
        print("\n❌ Migration validation failed!")
        print("Please address the issues above before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main()
