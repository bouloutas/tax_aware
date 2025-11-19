
# get_config_tickers.py (FINAL, ROBUST FILE-BASED VERSION)

import pandas as pd
from sqlalchemy import create_engine
import sys
import os

# --- Configuration ---
DB_USER = "root"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_SOCKET = "/Users/tasosbouloutas/mysql_data/mysql.sock"
SCORES_DB = "manual_weights"
UNIVERSE_SCORES_TABLE = "universe_factor_scores"

# Define a standard output file path
# This will be created in the same directory as the script
OUTPUT_FILE_PATH = os.path.join(os.path.dirname(__file__), "ticker_list.txt")

def get_universe_tickers():
    # ... (this function is unchanged) ...
    print("--- Getting unique tickers from factor score universe ---", file=sys.stderr)
    try:
        engine_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{SCORES_DB}?unix_socket={DB_SOCKET}"
        engine = create_engine(engine_url)
        query = f"SELECT DISTINCT ticker FROM `{UNIVERSE_SCORES_TABLE}`"
        df = pd.read_sql(query, engine)
        engine.dispose()
        tickers = df['ticker'].dropna().unique().tolist()
        print(f"Found {len(tickers)} unique tickers in the universe.", file=sys.stderr)
        return tickers
    except Exception as e:
        print(f"CRITICAL: Could not fetch tickers from MySQL. Error: {e}", file=sys.stderr)
        return []

if __name__ == "__main__":
    universe_tickers = get_universe_tickers()
    
    benchmark_ticker = 'SPY'
    if benchmark_ticker not in universe_tickers:
        universe_tickers.append(benchmark_ticker)
    
    if not universe_tickers:
        formatted_tickers = "''" # A valid SQL empty string list
    else:
        formatted_tickers = ",".join([f"'{t}'" for t in universe_tickers])

    # Write the result to the predefined file instead of printing it
    try:
        with open(OUTPUT_FILE_PATH, 'w') as f:
            f.write(formatted_tickers)
        print(f"Successfully wrote {len(universe_tickers)} tickers to {OUTPUT_FILE_PATH}", file=sys.stderr)
    except Exception as e:
        print(f"CRITICAL: Failed to write ticker list to file. Error: {e}", file=sys.stderr)
        sys.exit(1)