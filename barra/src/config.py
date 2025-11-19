"""
Configuration constants for the Barra implementation.
All file paths remain absolute so downstream scripts can attach DuckDB databases
without relying on relative cwd state.
"""
from pathlib import Path

BASE_PATH = Path("/home/tasos")
COMPUSTAT_DB = BASE_PATH / "T9_APFS" / "compustat.duckdb"
PRICE_DB = BASE_PATH / "T9_APFS" / "backtest" / "stock_analysis_backtest.duckdb"
# Analytics DB will store curated tables/views produced by Phase 1 SQL
ANALYTICS_DB = BASE_PATH / "tax_aware" / "barra" / "barra_analytics.duckdb"
MULTIASSET_DB = BASE_PATH / "T9_APFS" / "backtest" / "multiasset_new.duckdb"

CURRENCY_BETA_LOOKBACK_MONTHS = 60
CURRENCY_BETA_MIN_OBS = 24
IMPUTATION_WARNING_THRESHOLD = 0.3
FACTOR_COV_WINDOW_MONTHS = 60

DEFAULT_ANALYTICS_SCHEMA = "analytics"
MONTHLY_RETURN_LOOKBACK = 60  # months required for beta calculations
MARKET_INDEX_TICKER = "SPY"  # used for proxying US market factor
