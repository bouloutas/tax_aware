#!/usr/bin/env python3
"""
migrate_to_duckdb.py

Migration script to transfer data from MySQL to DuckDB.
Handles both Compustat and Fama-French data migration.
"""

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from datetime import datetime
from duckdb_manager import DuckDBManager

class DatabaseMigrator:
    """
    Migrates data from MySQL to DuckDB databases.
    
    Database separation:
    - Compustat/SPGlobal data: /home/tasos/T9_APFS/compustat.duckdb
    - All other data: /home/tasos/T9_APFS/fama_french.duckdb
    """
    
    def __init__(self):
        self.duckdb_manager = DuckDBManager()
        
        # MySQL connection URLs
        self.mysql_ff_url = "mysql+pymysql://root:@localhost/fama_french_local?unix_socket=/Users/tasosbouloutas/mysql_data/mysql.sock"
        self.mysql_manual_url = "mysql+pymysql://root:@localhost/manual_weights?unix_socket=/Users/tasosbouloutas/mysql_data/mysql.sock"
        
        # MySQL engines
        self.mysql_ff_engine = None
        self.mysql_manual_engine = None
        
        # Migration log
        self.migration_log = []
    
    def connect_mysql(self):
        """Connect to MySQL databases"""
        try:
            self.mysql_ff_engine = create_engine(self.mysql_ff_url)
            self.mysql_manual_engine = create_engine(self.mysql_manual_url)
            
            # Test connections
            with self.mysql_ff_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            with self.mysql_manual_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            print("✓ MySQL connections established")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to MySQL: {e}")
            return False
    
    def disconnect_mysql(self):
        """Disconnect from MySQL databases"""
        if self.mysql_ff_engine:
            self.mysql_ff_engine.dispose()
        if self.mysql_manual_engine:
            self.mysql_manual_engine.dispose()
        print("✓ MySQL connections closed")
    
    def log_migration(self, operation: str, table: str, rows: int, database: str):
        """Log migration operation"""
        log_entry = {
            'timestamp': datetime.now(),
            'operation': operation,
            'table': table,
            'rows': rows,
            'database': database
        }
        self.migration_log.append(log_entry)
        print(f"✓ {operation}: {table} ({rows} rows) → {database}")
    
    def migrate_compustat_data(self):
        """Migrate Compustat/SPGlobal data to compustat.duckdb"""
        print("\n--- Migrating Compustat Data ---")
        
        try:
            # Read from MySQL
            df = pd.read_sql("SELECT * FROM data_for_factor_construction", self.mysql_ff_engine)
            
            if df.empty:
                print("⚠ No Compustat data found in MySQL")
                return False
            
            # Create schema if needed
            self.duckdb_manager.create_schema('compustat')
            
            # Write to DuckDB
            self.duckdb_manager.write_dataframe(df, 'data_for_factor_construction', 'compustat', 'replace')
            
            self.log_migration('Migrate', 'data_for_factor_construction', len(df), 'compustat')
            return True
            
        except Exception as e:
            print(f"✗ Error migrating Compustat data: {e}")
            return False
    
    def migrate_ff_data(self):
        """Migrate Fama-French data to fama_french.duckdb"""
        print("\n--- Migrating Fama-French Data ---")
        
        tables_to_migrate = [
            'final_combined_factors',
            'optimization_portfolio_monthly_returns', 
            'ken_french_factors',
            'my_constructed_factors'
        ]
        
        success_count = 0
        
        # Create schema if needed
        self.duckdb_manager.create_schema('ff')
        
        for table in tables_to_migrate:
            try:
                # Check if table exists in MySQL
                df = pd.read_sql(f"SELECT * FROM {table}", self.mysql_ff_engine)
                
                if df.empty:
                    print(f"⚠ Table {table} is empty, skipping")
                    continue
                
                # Write to DuckDB
                self.duckdb_manager.write_dataframe(df, table, 'ff', 'replace')
                
                self.log_migration('Migrate', table, len(df), 'ff')
                success_count += 1
                
            except Exception as e:
                print(f"✗ Error migrating {table}: {e}")
        
        print(f"✓ Successfully migrated {success_count}/{len(tables_to_migrate)} tables")
        return success_count > 0
    
    def migrate_manual_weights(self):
        """Migrate manual weights data"""
        print("\n--- Migrating Manual Weights Data ---")
        
        try:
            # Read from MySQL manual_weights database
            df = pd.read_sql("SELECT * FROM universe_factor_scores", self.mysql_manual_engine)
            
            if df.empty:
                print("⚠ No manual weights data found in MySQL")
                return False
            
            # Write to DuckDB
            self.duckdb_manager.write_dataframe(df, 'universe_factor_scores', 'ff', 'replace')
            
            self.log_migration('Migrate', 'universe_factor_scores', len(df), 'ff')
            return True
            
        except Exception as e:
            print(f"✗ Error migrating manual weights data: {e}")
            return False
    
    def validate_migration(self):
        """Validate that migration was successful"""
        print("\n--- Validating Migration ---")
        
        validation_results = {}
        
        # Check Compustat data
        try:
            compustat_count = self.duckdb_manager.get_table_count('data_for_factor_construction', 'compustat')
            validation_results['compustat'] = compustat_count
            print(f"✓ Compustat data: {compustat_count} rows")
        except Exception as e:
            print(f"✗ Compustat validation failed: {e}")
            validation_results['compustat'] = 0
        
        # Check FF data
        ff_tables = ['final_combined_factors', 'optimization_portfolio_monthly_returns', 
                    'ken_french_factors', 'my_constructed_factors']
        
        for table in ff_tables:
            try:
                count = self.duckdb_manager.get_table_count(table, 'ff')
                validation_results[table] = count
                print(f"✓ {table}: {count} rows")
            except Exception as e:
                print(f"✗ {table} validation failed: {e}")
                validation_results[table] = 0
        
        # Check manual weights
        try:
            scores_count = self.duckdb_manager.get_table_count('universe_factor_scores', 'ff')
            validation_results['universe_factor_scores'] = scores_count
            print(f"✓ universe_factor_scores: {scores_count} rows")
        except Exception as e:
            print(f"✗ universe_factor_scores validation failed: {e}")
            validation_results['universe_factor_scores'] = 0
        
        return validation_results
    
    def create_backup(self):
        """Create backup of existing DuckDB files"""
        print("\n--- Creating Backup ---")
        
        try:
            compustat_backup = self.duckdb_manager.backup_database('compustat')
            ff_backup = self.duckdb_manager.backup_database('ff')
            
            print(f"✓ Compustat backup: {compustat_backup}")
            print(f"✓ Fama-French backup: {ff_backup}")
            
            return compustat_backup, ff_backup
        except Exception as e:
            print(f"✗ Backup failed: {e}")
            return None, None
    
    def print_migration_summary(self):
        """Print migration summary"""
        print("\n--- Migration Summary ---")
        print(f"Total operations: {len(self.migration_log)}")
        
        total_rows = sum(log['rows'] for log in self.migration_log)
        print(f"Total rows migrated: {total_rows:,}")
        
        print("\nOperations by database:")
        compustat_ops = [log for log in self.migration_log if log['database'] == 'compustat']
        ff_ops = [log for log in self.migration_log if log['database'] == 'ff']
        
        print(f"Compustat: {len(compustat_ops)} operations")
        print(f"Fama-French: {len(ff_ops)} operations")
    
    def run_migration(self, create_backup: bool = True):
        """Run complete migration process"""
        print("=== MySQL to DuckDB Migration ===")
        print(f"Started at: {datetime.now()}")
        
        # Connect to MySQL
        if not self.connect_mysql():
            print("✗ Cannot proceed without MySQL connection")
            return False
        
        try:
            # Create backup if requested
            if create_backup:
                self.create_backup()
            
            # Run migrations
            success = True
            
            # Migrate Compustat data
            if not self.migrate_compustat_data():
                success = False
            
            # Migrate FF data
            if not self.migrate_ff_data():
                success = False
            
            # Migrate manual weights
            if not self.migrate_manual_weights():
                success = False
            
            # Validate migration
            validation_results = self.validate_migration()
            
            # Print summary
            self.print_migration_summary()
            
            if success:
                print("\n✓ Migration completed successfully!")
                return True
            else:
                print("\n⚠ Migration completed with errors")
                return False
                
        finally:
            # Always disconnect from MySQL
            self.disconnect_mysql()


def main():
    """Main migration function"""
    migrator = DatabaseMigrator()
    
    # Auto-confirm migration for automated execution
    print("This will migrate all data from MySQL to DuckDB. Proceeding automatically...")
    
    # Run migration
    success = migrator.run_migration(create_backup=True)
    
    if success:
        print("\n🎉 Migration completed successfully!")
        print("You can now use the updated modules with DuckDB.")
    else:
        print("\n❌ Migration failed. Check the logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
