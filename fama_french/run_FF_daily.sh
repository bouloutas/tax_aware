#!/bin/bash

# ======================================================================
# Daily Fama-French Data Processing, Factor Construction, and Portfolio Optimization Automator
# ======================================================================

# --- Script Start ---
echo "======================================================================"
echo "Starting Daily Fama-French Full Process: $(date)"
echo "======================================================================"

# --- Configuration ---
PROJECT_DIR="/home/tasos/tax_aware/fama_french"
CONDA_ENV_NAME="dgx-spark"
ENV_FILE="${PROJECT_DIR}/.env"

SQL_DIR="${PROJECT_DIR}/sql"
SNOWFLAKE_SQL_BUILD_FF_FILE="${SQL_DIR}/build_fama_french_tables.sql"
SNOWFLAKE_SQL_OPTIMIZATION_RETURNS_FILE="${SQL_DIR}/get_optimization_returns.sql"

PYTHON_SCRIPTS_DIR="${PROJECT_DIR}"
PYTHON_EXECUTE_SQL_SCRIPT="${PYTHON_SCRIPTS_DIR}/execute_sql_file.py"
PYTHON_GET_CONFIG_TICKERS_SCRIPT="${PYTHON_SCRIPTS_DIR}/get_config_tickers.py"
PYTHON_COPY_FF_BASE_TO_MYSQL_SCRIPT="${PYTHON_SCRIPTS_DIR}/copy_to_mysql_ff.py"
PYTHON_FACTOR_CONSTRUCTION_SCRIPT="${PYTHON_SCRIPTS_DIR}/factor_construction.py"
PYTHON_COPY_OPT_RETURNS_TO_MYSQL_SCRIPT="${PYTHON_SCRIPTS_DIR}/copy_optimization_returns_to_mysql.py"
PYTHON_PORTFOLIO_OPTIMIZATION_SCRIPT="${PYTHON_SCRIPTS_DIR}/portfolio_optimization.py"

LOG_FILE="${PROJECT_DIR}/logs/fama_french_daily_$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

# The entire script's logic is wrapped in braces and output is piped to tee
{
    echo "Changing to project directory: ${PROJECT_DIR}"
    cd "$PROJECT_DIR" || { echo "Failed to cd to project directory. Exiting with code 1."; exit 1; }

    # Source the .env file to load variables into the current shell session.
    if [ -f "$ENV_FILE" ]; then
        echo "Sourcing environment variables from ${ENV_FILE}"
        source "$ENV_FILE"
    else
        echo "CRITICAL: Environment file .env not found in ${PROJECT_DIR}. Please create it."
        exit 1
    fi

    # --- Helper Function for File Checks ---
    check_file() {
        if [ ! -f "$1" ]; then
            echo "Error: File not found at $1. Exiting with code $2."
            exit "$2"
        fi
    }
    check_file "$PYTHON_EXECUTE_SQL_SCRIPT" 19

    # --- Step 1: Run SQL to build Fama-French base data tables VIA PYTHON ---
    echo $'\n--- Step 1: Running Snowflake SQL for FF Base Data (via Python) ---'
    check_file "$SNOWFLAKE_SQL_BUILD_FF_FILE" 3
    conda run -n ${CONDA_ENV_NAME} python "${PYTHON_EXECUTE_SQL_SCRIPT}" "${SNOWFLAKE_SQL_BUILD_FF_FILE}"
    if [ $? -ne 0 ]; then echo "Error: Python SQL executor failed for FF base data. Exiting."; exit 4; fi
    echo "FF base data SQL script completed successfully."


    # --- Step 2: Copy FF Base Data from Snowflake to MySQL ---
    echo $'\n--- Step 2: Copying FF Base Data to MySQL ---'
    check_file "$PYTHON_COPY_FF_BASE_TO_MYSQL_SCRIPT" 5
    conda run -n ${CONDA_ENV_NAME} python "${PYTHON_COPY_FF_BASE_TO_MYSQL_SCRIPT}"
    if [ $? -ne 0 ]; then echo "Error: Copying FF base data to MySQL failed. Exiting."; exit 6; fi
    echo "Copying FF base data to MySQL completed successfully."


    # --- Step 3: Construct Fama-French Factors ---
    echo $'\n--- Step 3: Constructing Fama-French Factors ---'
    check_file "$PYTHON_FACTOR_CONSTRUCTION_SCRIPT" 7
    conda run -n ${CONDA_ENV_NAME} python "${PYTHON_FACTOR_CONSTRUCTION_SCRIPT}"
    if [ $? -ne 0 ]; then echo "Error: Fama-French factor construction failed. Exiting."; exit 8; fi
    echo "Fama-French factor construction completed successfully."

    # --- Step 4a: Get Dynamic Ticker List ---
    echo $'\n--- Step 4a: Generating ticker list file ---'
    check_file "$PYTHON_GET_CONFIG_TICKERS_SCRIPT" 81
    
    conda run -n ${CONDA_ENV_NAME} python "${PYTHON_GET_CONFIG_TICKERS_SCRIPT}"
    if [ $? -ne 0 ]; then echo "Error: Ticker list generation failed. Exiting."; exit 82; fi
    
    TICKER_LIST_FILE="${PROJECT_DIR}/ticker_list.txt"
    check_file "$TICKER_LIST_FILE" 83
    echo "Successfully created ticker list file at ${TICKER_LIST_FILE}"
    
    # --- Step 4b: Run Snowflake SQL to get returns ---
    echo $'\n--- Step 4b: Fetching Optimization Universe Returns (via Python) ---'
    check_file "$SNOWFLAKE_SQL_OPTIMIZATION_RETURNS_FILE" 9
    
    conda run -n ${CONDA_ENV_NAME} python "${PYTHON_EXECUTE_SQL_SCRIPT}" "${SNOWFLAKE_SQL_OPTIMIZATION_RETURNS_FILE}" "${TICKER_LIST_FILE}"
    
    if [ $? -ne 0 ]; then echo "Error: Python SQL executor failed for optimization returns. Exiting."; exit 10; fi
    echo "Optimization returns SQL script completed successfully."
    
    rm "$TICKER_LIST_FILE"
    
    # --- Step 5: Copy Optimization Returns Data to MySQL ---
    echo $'\n--- Step 5: Copying Optimization Returns to MySQL ---'
    check_file "$PYTHON_COPY_OPT_RETURNS_TO_MYSQL_SCRIPT" 11
    conda run -n ${CONDA_ENV_NAME} python "${PYTHON_COPY_OPT_RETURNS_TO_MYSQL_SCRIPT}"
    if [ $? -ne 0 ]; then echo "Error: Copying optimization returns to MySQL failed. Exiting."; exit 12; fi
    echo "Copying optimization returns to MySQL completed successfully."



    # --- DELETE THIS ENTIRE BLOCK ---
    # --- Step 6: Running Portfolio Optimization ---
    echo $'\n--- Step 6: Running Portfolio Optimization ---'
    check_file "$PYTHON_PORTFOLIO_OPTIMIZATION_SCRIPT" 13
    conda run -n ${CONDA_ENV_NAME} python "${PYTHON_PORTFOLIO_OPTIMIZATION_SCRIPT}"
    if [ $? -ne 0 ]; then echo "Error: Portfolio optimization script failed. Exiting."; exit 14; fi
    echo "Portfolio optimization script completed successfully."
    # --- END DELETE ---



    echo "======================================================================"
    echo "Daily Fama-French Process and Portfolio Optimization Completed Successfully: $(date)"
    echo "Log file: ${LOG_FILE}"
    echo "======================================================================"

} | tee -a "$LOG_FILE" 2>&1

exit 0
