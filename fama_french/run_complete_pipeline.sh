#!/bin/bash

# ======================================================================
# MASTER PIPELINE ORCHESTRATOR - DUCKDB VERSION
#
# This is the single script to run the entire end-to-end process:
# 1. Runs data migration from MySQL to DuckDB (if needed)
# 2. Runs the Fama-French data pipeline to refresh historical returns.
# 3. Runs the custom factor scoring pipeline to generate alpha scores.
# 4. Runs the advanced optimizer to backtest the long-short strategy.
# ======================================================================

set -e 
set -o pipefail
# --- Script Start ---
echo "======================================================================"
echo "          STARTING COMPLETE QUANTITATIVE PIPELINE (DUCKDB)"
echo "======================================================================"
echo "Start Time: $(date)"
echo "======================================================================"

# --- Configuration ---
# Main project directory where this script lives
PROJECT_DIR="/home/tasos/tax_aware/fama_french"
# Directory for the custom factor script
PRODUCTION_DIR="/home/tasos/snowflake/spglobal/backtest/production"

CONDA_ENV_NAME="dgx-spark"
ENV_FILE="${PROJECT_DIR}/.env"

# Define the paths to the main components
MIGRATION_SCRIPT="${PROJECT_DIR}/migrate_to_duckdb.py"
FF_PIPELINE_SCRIPT="${PROJECT_DIR}/run_FF_daily.sh"
FACTOR_SCORING_SCRIPT="${PRODUCTION_DIR}/manual_factors_v3.py"
OPTIMIZER_SCRIPT="${PROJECT_DIR}/advanced_optimizer.py"
TEST_SCRIPT="${PROJECT_DIR}/test_duckdb_migration.py"

# --- Logging Setup ---
LOG_FILE="${PROJECT_DIR}/logs/complete_pipeline_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$(dirname "$LOG_FILE")"

# The entire script's logic is wrapped in braces and output is piped to tee
{
    echo "Changing to main project directory: ${PROJECT_DIR}"
    cd "$PROJECT_DIR" || { echo "Failed to cd to project directory. Exiting with code 1."; exit 1; }

    # Source the .env file to load variables into the current shell session.
    if [ -f "$ENV_FILE" ]; then
        echo "Sourcing environment variables from ${ENV_FILE}"
        source "$ENV_FILE"
    else
        echo "CRITICAL: Environment file .env not found in ${PROJECT_DIR}. Please create it."
        exit 1
    fi

    # --- STAGE 0: CHECK AND RUN DATA MIGRATION (if needed) ---
    echo $'\n\n==================== STAGE 0: CHECKING DATA MIGRATION STATUS ===================='
    
    # Check if DuckDB files exist
    COMPUSTAT_DB="/home/tasos/T9_APFS/compustat.duckdb"
    FF_DB="/home/tasos/T9_APFS/fama_french.duckdb"
    
    if [ ! -f "$COMPUSTAT_DB" ] || [ ! -f "$FF_DB" ]; then
        echo "DuckDB files not found. Running migration..."
        if [ ! -f "$MIGRATION_SCRIPT" ]; then 
            echo "Error: Migration script not found at ${MIGRATION_SCRIPT}"; 
            exit 1; 
        fi
        
        conda run -n "$CONDA_ENV_NAME" python "$MIGRATION_SCRIPT"
        if [ $? -ne 0 ]; then 
            echo "Error: Data migration failed. Aborting."; 
            exit 1; 
        fi
        echo "Data migration completed successfully."
    else
        echo "DuckDB files found. Skipping migration."
    fi
    
    # Run migration validation tests
    echo "Running migration validation tests..."
    if [ ! -f "$TEST_SCRIPT" ]; then 
        echo "Error: Test script not found at ${TEST_SCRIPT}"; 
        exit 1; 
    fi
    
    conda run -n "$CONDA_ENV_NAME" python "$TEST_SCRIPT"
    if [ $? -ne 0 ]; then 
        echo "Error: Migration validation tests failed. Aborting."; 
        exit 1; 
    fi
    echo $'==================== STAGE 0: DATA MIGRATION VALIDATION COMPLETED ====================\n'

    # --- STAGE 1: RUN FAMA-FRENCH DATA PIPELINE ---
    echo $'\n\n==================== STAGE 1: EXECUTING FAMA-FRENCH PIPELINE ===================='
    if [ ! -f "$FF_PIPELINE_SCRIPT" ]; then echo "Error: F-F pipeline script not found at ${FF_PIPELINE_SCRIPT}"; exit 1; fi
    
    # Make sure it's executable
    chmod +x "$FF_PIPELINE_SCRIPT"
    
    "$FF_PIPELINE_SCRIPT"
    if [ $? -ne 0 ]; then echo "Error: Fama-French pipeline (run_FF_daily.sh) failed. Aborting."; exit 1; fi
    echo $'==================== STAGE 1: FAMA-FRENCH PIPELINE COMPLETED SUCCESSFULLY ====================\n'

    # --- STAGE 2: RUN CUSTOM FACTOR SCORING PIPELINE ---
    echo $'\n\n==================== STAGE 2: EXECUTING FACTOR SCORING PIPELINE ===================='
    if [ ! -f "$FACTOR_SCORING_SCRIPT" ]; then echo "Error: Factor scoring script not found at ${FACTOR_SCORING_SCRIPT}"; exit 1; fi

    # Run the script in 'production' mode to get the portfolio for the latest possible date.
    conda run -n "$CONDA_ENV_NAME" python "$FACTOR_SCORING_SCRIPT" --mode production
    if [ $? -ne 0 ]; then echo "Error: Factor scoring pipeline (manual_factors_v3.py) failed. Aborting."; exit 1; fi
    echo $'==================== STAGE 2: FACTOR SCORING COMPLETED SUCCESSFULLY ====================\n'

    # --- STAGE 3: RUN THE ADVANCED OPTIMIZER AND BACKTEST ---
    echo $'\n\n==================== STAGE 3: EXECUTING OPTIMIZATION & BACKTEST ===================='
    if [ ! -f "$OPTIMIZER_SCRIPT" ]; then echo "Error: Optimizer script not found at ${OPTIMIZER_SCRIPT}"; exit 1; fi
    
    conda run -n "$CONDA_ENV_NAME" python "$OPTIMIZER_SCRIPT"
    if [ $? -ne 0 ]; then echo "Error: Optimization backtest (advanced_optimizer.py) failed. Aborting."; exit 1; fi
    echo $'==================== STAGE 3: OPTIMIZATION & BACKTEST COMPLETED SUCCESSFULLY ====================\n'

    echo $'\n======================================================================'
    echo "               ALL PIPELINE STAGES COMPLETED SUCCESSFULLY"
    echo "======================================================================"
    echo "End Time: $(date)"
    echo "Master log file: ${LOG_FILE}"
    echo "DuckDB Databases:"
    echo "  - Compustat: ${COMPUSTAT_DB}"
    echo "  - Fama-French: ${FF_DB}"
    echo "======================================================================"

} | tee -a "$LOG_FILE" 2>&1

exit 0
