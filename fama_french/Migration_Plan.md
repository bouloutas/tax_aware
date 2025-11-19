# Database Migration Plan: MySQL → DuckDB

## Overview
This plan outlines the migration of all database interactions from MySQL to DuckDB, with specific paths for different data types:
- **Compustat/SPGlobal data**: `/home/tasos/T9_APFS/compustat.duckdb`
- **All other data**: `/home/tasos/T9_APFS/fama_french.duckdb`

## Migration Strategy

### Phase 1: Database Setup and Schema Migration

#### 1.1 Create DuckDB Databases
```python
# Create compustat.duckdb for SPGlobal/Compustat data
import duckdb
compustat_conn = duckdb.connect('/home/tasos/T9_APFS/compustat.duckdb')

# Create fama_french.duckdb for all other data
ff_conn = duckdb.connect('/home/tasos/T9_APFS/fama_french.duckdb')
```

#### 1.2 Schema Migration
**Compustat Database Schema:**
```sql
-- Tables to migrate to compustat.duckdb
CREATE TABLE data_for_factor_construction (
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

-- Any other Compustat-specific tables
```

**Fama-French Database Schema:**
```sql
-- Tables to migrate to fama_french.duckdb
CREATE TABLE final_combined_factors (
    Date DATE PRIMARY KEY,
    Mkt_RF DECIMAL(10,6),
    RF DECIMAL(10,6),
    SMB DECIMAL(10,6),
    HML DECIMAL(10,6),
    RMW DECIMAL(10,6),
    CMA DECIMAL(10,6)
);

CREATE TABLE optimization_portfolio_monthly_returns (
    TICKER VARCHAR,
    MONTH_END_DATE DATE,
    GVKEY VARCHAR,
    MONTHLY_RETURN DECIMAL(10,6)
);

CREATE TABLE ken_french_factors (
    Date DATE PRIMARY KEY,
    Mkt_RF DECIMAL(10,6),
    SMB DECIMAL(10,6),
    HML DECIMAL(10,6),
    RMW DECIMAL(10,6),
    CMA DECIMAL(10,6),
    RF DECIMAL(10,6)
);

CREATE TABLE my_constructed_factors (
    Date DATE,
    SMB DECIMAL(10,6),
    HML DECIMAL(10,6),
    RMW DECIMAL(10,6),
    CMA DECIMAL(10,6)
);

-- Manual weights database tables
CREATE TABLE universe_factor_scores (
    ticker VARCHAR,
    datadate DATE,
    factor_score DECIMAL(10,6),
    decile INTEGER,
    company_name VARCHAR,
    _5d_forward_return DECIMAL(10,6)
);
```

### Phase 2: Data Migration Scripts

#### 2.1 Create Migration Utilities
```python
# migrate_to_duckdb.py
import duckdb
import pandas as pd
from sqlalchemy import create_engine
import os

class DatabaseMigrator:
    def __init__(self):
        self.compustat_path = '/home/tasos/T9_APFS/compustat.duckdb'
        self.ff_path = '/home/tasos/T9_APFS/fama_french.duckdb'
        
        # MySQL connection (existing)
        self.mysql_url = "mysql+pymysql://root:@localhost/fama_french_local?unix_socket=/Users/tasosbouloutas/mysql_data/mysql.sock"
        self.mysql_engine = create_engine(self.mysql_url)
        
        # DuckDB connections
        self.compustat_conn = duckdb.connect(self.compustat_path)
        self.ff_conn = duckdb.connect(self.ff_path)
    
    def migrate_compustat_data(self):
        """Migrate Compustat/SPGlobal data to compustat.duckdb"""
        print("Migrating Compustat data...")
        
        # Read from MySQL
        df = pd.read_sql("SELECT * FROM data_for_factor_construction", self.mysql_engine)
        
        # Write to DuckDB
        self.compustat_conn.execute("CREATE TABLE IF NOT EXISTS data_for_factor_construction AS SELECT * FROM df")
        
        print(f"Migrated {len(df)} rows to compustat.duckdb")
    
    def migrate_ff_data(self):
        """Migrate Fama-French data to fama_french.duckdb"""
        print("Migrating Fama-French data...")
        
        tables_to_migrate = [
            'final_combined_factors',
            'optimization_portfolio_monthly_returns', 
            'ken_french_factors',
            'my_constructed_factors'
        ]
        
        for table in tables_to_migrate:
            try:
                df = pd.read_sql(f"SELECT * FROM {table}", self.mysql_engine)
                self.ff_conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df")
                print(f"Migrated {len(df)} rows from {table}")
            except Exception as e:
                print(f"Error migrating {table}: {e}")
    
    def migrate_manual_weights(self):
        """Migrate manual weights data"""
        print("Migrating manual weights data...")
        
        # Connect to manual_weights database
        manual_url = "mysql+pymysql://root:@localhost/manual_weights?unix_socket=/Users/tasosbouloutas/mysql_data/mysql.sock"
        manual_engine = create_engine(manual_url)
        
        df = pd.read_sql("SELECT * FROM universe_factor_scores", manual_engine)
        self.ff_conn.execute("CREATE TABLE IF NOT EXISTS universe_factor_scores AS SELECT * FROM df")
        
        print(f"Migrated {len(df)} rows from universe_factor_scores")
    
    def close_connections(self):
        self.compustat_conn.close()
        self.ff_conn.close()
        self.mysql_engine.dispose()

if __name__ == "__main__":
    migrator = DatabaseMigrator()
    migrator.migrate_compustat_data()
    migrator.migrate_ff_data()
    migrator.migrate_manual_weights()
    migrator.close_connections()
```

### Phase 3: Update Core Modules

#### 3.1 Create DuckDB Connection Manager
```python
# duckdb_manager.py
import duckdb
import os
from contextlib import contextmanager

class DuckDBManager:
    def __init__(self):
        self.compustat_path = '/home/tasos/T9_APFS/compustat.duckdb'
        self.ff_path = '/home/tasos/T9_APFS/fama_french.duckdb'
    
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
    
    def execute_query(self, query, database='ff', params=None):
        """Execute query on specified database"""
        if database == 'compustat':
            with self.get_compustat_connection() as conn:
                return conn.execute(query, params).fetchall() if params else conn.execute(query).fetchall()
        else:
            with self.get_ff_connection() as conn:
                return conn.execute(query, params).fetchall() if params else conn.execute(query).fetchall()
    
    def read_sql(self, query, database='ff'):
        """Read SQL query into pandas DataFrame"""
        if database == 'compustat':
            with self.get_compustat_connection() as conn:
                return conn.execute(query).df()
        else:
            with self.get_ff_connection() as conn:
                return conn.execute(query).df()
```

#### 3.2 Update factor_construction.py
```python
# Updated imports
import duckdb
from duckdb_manager import DuckDBManager

# Replace MySQL functions
def get_mysql_connection(db_name=MYSQL_DATABASE):
    # OLD: MySQL connection
    # NEW: DuckDB connection
    return DuckDBManager()

def get_data_for_formation_year(duckdb_manager, formation_year):
    query = f"""
    SELECT
        GVKEY, IID, RETURN_MONTH_END_DATE, MONTHLY_RETURN, EXCHG,
        ME_JUNE, BE_FY_T_MINUS_1, OP_FY_T_MINUS_1, INV_FY_T_MINUS_1, BM_T_MINUS_1
    FROM data_for_factor_construction
    WHERE FORMATION_YEAR_T = {formation_year};
    """
    print(f"Fetching data for formation year: {formation_year} from DuckDB...")
    try:
        df = duckdb_manager.read_sql(query, database='compustat')
        df['RETURN_MONTH_END_DATE'] = pd.to_datetime(df['RETURN_MONTH_END_DATE'])
        print(f"Fetched {len(df)} rows for formation year {formation_year}.")
        return df
    except Exception as e:
        print(f"Error fetching data for formation year {formation_year} from DuckDB: {e}")
        return pd.DataFrame()

# Update main function
def main_factor_construction_and_analysis():
    print("--- Starting Factor Construction and Analysis ---")
    duckdb_manager = DuckDBManager()
    
    # Rest of the function updated to use DuckDB instead of MySQL
    # ... (similar pattern for all database operations)
```

#### 3.3 Update portfolio_optimization.py
```python
# Updated imports
from duckdb_manager import DuckDBManager

# Replace get_mysql_engine function
def get_duckdb_manager():
    return DuckDBManager()

# Update main function
def main():
    print("--- Starting Portfolio Optimization Script ---")
    duckdb_manager = get_duckdb_manager()
    
    # Load Fama-French Factors
    try:
        print("Loading Fama-French factors from DuckDB...")
        factors_df = duckdb_manager.read_sql("SELECT * FROM final_combined_factors", database='ff')
        factors_df['Date'] = pd.to_datetime(factors_df['Date'])
        factors_df.set_index('Date', inplace=True)
        print(f"Loaded {len(factors_df)} rows of factor data from {factors_df.index.min()} to {factors_df.index.max()}.")
        if factors_df.empty: raise ValueError("Factor data is empty.")
    except Exception as e:
        print(f"CRITICAL: Error loading Fama-French factors: {e}")
        sys.exit(1)
    
    # Load Monthly Stock and SPY Returns
    try:
        print("Loading stock and SPY monthly returns from DuckDB...")
        all_monthly_returns_df = duckdb_manager.read_sql(
            "SELECT TICKER, MONTH_END_DATE, GVKEY, MONTHLY_RETURN FROM optimization_portfolio_monthly_returns", 
            database='ff'
        )
        all_monthly_returns_df['MONTH_END_DATE'] = pd.to_datetime(all_monthly_returns_df['MONTH_END_DATE'])
        print(f"Loaded {len(all_monthly_returns_df)} rows of monthly stock/SPY returns.")
        if all_monthly_returns_df.empty: raise ValueError("Stock/SPY returns data is empty.")
    except Exception as e:
        print(f"CRITICAL: Error loading monthly stock/SPY returns: {e}")
        sys.exit(1)
    
    # Rest of function continues with DuckDB...
```

#### 3.4 Update run_optimized_backtest.py
```python
# Updated imports
from duckdb_manager import DuckDBManager

# Replace get_db_engine function
def get_duckdb_manager():
    return DuckDBManager()

# Update load_data function
def load_data():
    print("\n--- Loading Scored Universe and Historical Returns ---")
    duckdb_manager = get_duckdb_manager()
    
    # Load scores from DuckDB
    scores_df = duckdb_manager.read_sql(f"SELECT * FROM {Config.UNIVERSE_SCORES_TABLE}", database='ff')
    scores_df['datadate'] = pd.to_datetime(scores_df['datadate'])
    print(f"Loaded {len(scores_df)} rows from '{Config.UNIVERSE_SCORES_TABLE}'")
    
    # Load returns from DuckDB
    returns_df = duckdb_manager.read_sql(
        f"SELECT TICKER, MONTH_END_DATE, MONTHLY_RETURN FROM {Config.RETURNS_TABLE}", 
        database='ff'
    )
    returns_df['MONTH_END_DATE'] = pd.to_datetime(returns_df['MONTH_END_DATE'])
    returns_pivot = returns_df.pivot(index='MONTH_END_DATE', columns='TICKER', values='MONTHLY_RETURN')
    print(f"Loaded {len(returns_df)} historical monthly return rows for {returns_df['TICKER'].nunique()} tickers.")
    
    return scores_df, returns_pivot
```

#### 3.5 Update advanced_optimizer.py
```python
# Updated imports
from duckdb_manager import DuckDBManager

# Replace get_db_engine function
def get_duckdb_manager():
    return DuckDBManager()

# Update load_data function
def load_data():
    print("\n--- Loading Scored Universe and Historical Returns ---")
    duckdb_manager = get_duckdb_manager()
    
    scores_df = duckdb_manager.read_sql(f"SELECT * FROM {Config.UNIVERSE_SCORES_TABLE}", database='ff')
    scores_df['datadate'] = pd.to_datetime(scores_df['datadate'])
    if 'decile' not in scores_df.columns:
        print("CRITICAL: 'decile' column not found. Rerun manual_factors_v3.py.")
        sys.exit(1)
    print(f"Loaded {len(scores_df)} rows from '{Config.UNIVERSE_SCORES_TABLE}'")
    
    returns_df = duckdb_manager.read_sql(
        f"SELECT TICKER, MONTH_END_DATE, MONTHLY_RETURN FROM {Config.RETURNS_TABLE}", 
        database='ff'
    )
    returns_df['MONTH_END_DATE'] = pd.to_datetime(returns_df['MONTH_END_DATE'])
    returns_pivot = returns_df.pivot(index='MONTH_END_DATE', columns='TICKER', values='MONTHLY_RETURN')
    print(f"Loaded {len(returns_df)} historical monthly return rows for {returns_df['TICKER'].nunique()} tickers.")
    return scores_df, returns_pivot
```

### Phase 4: Update Configuration Files

#### 4.1 Update Config Classes
```python
# In all modules, update Config classes
class Config:
    # Database Settings - Updated for DuckDB
    COMPUSTAT_DB_PATH = "/home/tasos/T9_APFS/compustat.duckdb"
    FF_DB_PATH = "/home/tasos/T9_APFS/fama_french.duckdb"
    
    # Table names remain the same
    UNIVERSE_SCORES_TABLE = "universe_factor_scores"
    RETURNS_TABLE = "optimization_portfolio_monthly_returns"
    FACTORS_TABLE = "final_combined_factors"
    
    # Rest of configuration remains the same...
```

### Phase 5: Update SQL Files

#### 5.1 Create DuckDB SQL Files
```sql
-- sql/build_fama_french_tables_duckdb.sql
-- Create tables in DuckDB format

-- Compustat database tables
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

-- Fama-French database tables
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

CREATE TABLE IF NOT EXISTS universe_factor_scores (
    ticker VARCHAR,
    datadate DATE,
    factor_score DECIMAL(10,6),
    decile INTEGER,
    company_name VARCHAR,
    _5d_forward_return DECIMAL(10,6)
);
```

### Phase 6: Update Shell Scripts

#### 6.1 Update run_complete_pipeline.sh
```bash
#!/bin/zsh
# Updated pipeline script for DuckDB

echo "Starting complete pipeline with DuckDB..."

# Step 1: Run data migration (if needed)
echo "Checking data migration status..."
python migrate_to_duckdb.py

# Step 2: Run factor construction
echo "Running factor construction..."
python factor_construction.py

# Step 3: Run portfolio optimization
echo "Running portfolio optimization..."
python portfolio_optimization.py

# Step 4: Run advanced optimizer
echo "Running advanced optimizer..."
python advanced_optimizer.py

echo "Pipeline complete!"
```

### Phase 7: Testing and Validation

#### 7.1 Create Test Script
```python
# test_duckdb_migration.py
import duckdb
import pandas as pd
from duckdb_manager import DuckDBManager

def test_migration():
    """Test that all data migrated correctly"""
    duckdb_manager = DuckDBManager()
    
    # Test Compustat data
    compustat_count = duckdb_manager.execute_query(
        "SELECT COUNT(*) FROM data_for_factor_construction", 
        database='compustat'
    )[0][0]
    print(f"Compustat data: {compustat_count} rows")
    
    # Test FF data
    ff_count = duckdb_manager.execute_query(
        "SELECT COUNT(*) FROM final_combined_factors", 
        database='ff'
    )[0][0]
    print(f"FF factors: {ff_count} rows")
    
    returns_count = duckdb_manager.execute_query(
        "SELECT COUNT(*) FROM optimization_portfolio_monthly_returns", 
        database='ff'
    )[0][0]
    print(f"Returns data: {returns_count} rows")
    
    scores_count = duckdb_manager.execute_query(
        "SELECT COUNT(*) FROM universe_factor_scores", 
        database='ff'
    )[0][0]
    print(f"Scores data: {scores_count} rows")

if __name__ == "__main__":
    test_migration()
```

### Phase 8: Rollback Plan

#### 8.1 Rollback Script
```python
# rollback_to_mysql.py
import mysql.connector
import duckdb
import pandas as pd

def rollback_to_mysql():
    """Rollback to MySQL if needed"""
    print("Rolling back to MySQL...")
    
    # Reconnect to MySQL
    mysql_conn = mysql.connector.connect(
        user='root',
        password='',
        host='localhost',
        unix_socket='/Users/tasosbouloutas/mysql_data/mysql.sock',
        database='fama_french_local'
    )
    
    # Reconnect to DuckDB
    duckdb_conn = duckdb.connect('/home/tasos/T9_APFS/fama_french.duckdb')
    
    # Copy data back to MySQL
    tables = ['final_combined_factors', 'optimization_portfolio_monthly_returns']
    for table in tables:
        df = duckdb_conn.execute(f"SELECT * FROM {table}").df()
        df.to_sql(table, mysql_conn, if_exists='replace', index=False)
        print(f"Rolled back {table}")
    
    mysql_conn.close()
    duckdb_conn.close()
    print("Rollback complete!")
```

## Implementation Timeline

### Week 1: Setup and Migration
- [ ] Create DuckDB databases
- [ ] Run data migration scripts
- [ ] Validate data integrity

### Week 2: Code Updates
- [ ] Update core modules
- [ ] Create DuckDB manager
- [ ] Update configuration files

### Week 3: Testing
- [ ] Run test scripts
- [ ] Validate all functionality
- [ ] Performance testing

### Week 4: Deployment
- [ ] Update shell scripts
- [ ] Deploy to production
- [ ] Monitor performance

## Benefits of Migration

### Performance
- **Faster queries**: DuckDB is optimized for analytical workloads
- **Better compression**: More efficient storage
- **Parallel processing**: Better utilization of multi-core systems

### Simplicity
- **No server management**: Embedded database
- **Single file**: Easy backup and transfer
- **Cross-platform**: Works on any system with DuckDB

### Cost
- **No licensing fees**: Open source
- **No server costs**: Embedded solution
- **Lower maintenance**: Fewer moving parts

## Risk Mitigation

### Data Safety
- **Backup before migration**: Full MySQL backup
- **Incremental migration**: Test with small datasets first
- **Rollback plan**: Ability to revert to MySQL

### Performance Monitoring
- **Query performance**: Monitor query times
- **Memory usage**: Track DuckDB memory consumption
- **Disk space**: Monitor database file sizes

### Testing Strategy
- **Unit tests**: Test individual functions
- **Integration tests**: Test full pipeline
- **Performance tests**: Compare with MySQL performance

This migration plan provides a comprehensive approach to moving from MySQL to DuckDB while maintaining all existing functionality and improving performance.
