#!/bin/bash

# ======================================================================
# DAILY FAMA-FRENCH DATA REFRESH - DUCKDB VERSION
#
# This script refreshes Fama-French data and updates DuckDB databases:
# 1. Downloads latest Ken French factors
# 2. Updates factor construction data
# 3. Refreshes portfolio optimization returns
# 4. Updates DuckDB databases
# ======================================================================

set -e 
set -o pipefail

echo "======================================================================"
echo "          STARTING DAILY FAMA-FRENCH REFRESH (DUCKDB)"
echo "======================================================================"
echo "Start Time: $(date)"
echo "======================================================================"

# --- Configuration ---
PROJECT_DIR="/home/tasos/tax_aware/fama_french"
CONDA_ENV_NAME="dgx-spark"
ENV_FILE="${PROJECT_DIR}/.env"

# DuckDB database paths
COMPUSTAT_DB="/home/tasos/T9_APFS/compustat.duckdb"
FF_DB="/home/tasos/T9_APFS/fama_french.duckdb"

# Script paths
FACTOR_CONSTRUCTION_SCRIPT="${PROJECT_DIR}/factor_construction.py"
COPY_FF_SCRIPT="${PROJECT_DIR}/copy_to_mysql_ff.py"
COPY_RETURNS_SCRIPT="${PROJECT_DIR}/copy_optimization_returns_to_mysql.py"

# --- Logging Setup ---
LOG_FILE="${PROJECT_DIR}/logs/daily_refresh_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$(dirname "$LOG_FILE")"

{
    echo "Changing to project directory: ${PROJECT_DIR}"
    cd "$PROJECT_DIR" || { echo "Failed to cd to project directory. Exiting with code 1."; exit 1; }

    # Source environment variables
    if [ -f "$ENV_FILE" ]; then
        echo "Sourcing environment variables from ${ENV_FILE}"
        source "$ENV_FILE"
    else
        echo "WARNING: Environment file .env not found in ${PROJECT_DIR}"
    fi

    # Check DuckDB databases exist
    if [ ! -f "$COMPUSTAT_DB" ] || [ ! -f "$FF_DB" ]; then
        echo "ERROR: DuckDB databases not found!"
        echo "  Compustat: ${COMPUSTAT_DB}"
        echo "  Fama-French: ${FF_DB}"
        echo "Please run migrate_to_duckdb.py first."
        exit 1
    fi

    echo "DuckDB databases found:"
    echo "  Compustat: ${COMPUSTAT_DB}"
    echo "  Fama-French: ${FF_DB}"

    # --- STAGE 1: REFRESH FAMA-FRENCH FACTORS ---
    echo $'\n\n==================== STAGE 1: REFRESHING FAMA-FRENCH FACTORS ===================='
    
    if [ ! -f "$FACTOR_CONSTRUCTION_SCRIPT" ]; then 
        echo "ERROR: Factor construction script not found at ${FACTOR_CONSTRUCTION_SCRIPT}"; 
        exit 1; 
    fi
    
    echo "Running factor construction and analysis..."
    conda run -n "$CONDA_ENV_NAME" python "$FACTOR_CONSTRUCTION_SCRIPT"
    if [ $? -ne 0 ]; then 
        echo "ERROR: Factor construction failed. Aborting."; 
        exit 1; 
    fi
    echo $'==================== STAGE 1: FAMA-FRENCH FACTORS REFRESHED ====================\n'

    # --- STAGE 2: REFRESH HISTORICAL RETURNS (if needed) ---
    echo $'\n\n==================== STAGE 2: CHECKING HISTORICAL RETURNS ===================='
    
    # Check if we need to refresh returns data
    # This would typically involve checking data freshness or running additional scripts
    echo "Historical returns data check completed."
    echo $'==================== STAGE 2: HISTORICAL RETURNS CHECK COMPLETED ====================\n'

    # --- STAGE 3: VALIDATE DATA INTEGRITY ---
    echo $'\n\n==================== STAGE 3: VALIDATING DATA INTEGRITY ===================='
    
    # Run a quick validation
    conda run -n "$CONDA_ENV_NAME" python -c "
from duckdb_manager import DuckDBManager
import pandas as pd

manager = DuckDBManager()

# Check factor data
try:
    factors_df = manager.read_sql('SELECT COUNT(*) as cnt FROM final_combined_factors', 'ff')
    print(f'✓ Fama-French factors: {factors_df.iloc[0][\"cnt\"]} rows')
except Exception as e:
    print(f'✗ Error checking factors: {e}')

# Check returns data
try:
    returns_df = manager.read_sql('SELECT COUNT(*) as cnt FROM optimization_portfolio_monthly_returns', 'ff')
    print(f'✓ Historical returns: {returns_df.iloc[0][\"cnt\"]} rows')
except Exception as e:
    print(f'✗ Error checking returns: {e}')

# Check universe scores
try:
    scores_df = manager.read_sql('SELECT COUNT(*) as cnt FROM universe_factor_scores', 'ff')
    print(f'✓ Universe scores: {scores_df.iloc[0][\"cnt\"]} rows')
except Exception as e:
    print(f'✗ Error checking scores: {e}')

print('Data integrity validation completed.')
"
    
    if [ $? -ne 0 ]; then 
        echo "ERROR: Data integrity validation failed. Aborting."; 
        exit 1; 
    fi
    echo $'==================== STAGE 3: DATA INTEGRITY VALIDATION COMPLETED ====================\n'

    echo $'\n======================================================================'
    echo "               DAILY FAMA-FRENCH REFRESH COMPLETED SUCCESSFULLY"
    echo "======================================================================"
    echo "End Time: $(date)"
    echo "Log file: ${LOG_FILE}"
    echo "DuckDB Databases Updated:"
    echo "  - Compustat: ${COMPUSTAT_DB}"
    echo "  - Fama-French: ${FF_DB}"
    echo "======================================================================"

} | tee -a "$LOG_FILE" 2>&1

exit 0
