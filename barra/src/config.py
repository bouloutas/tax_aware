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

# ============================================================================
# Feature Flags for Methodology Upgrades (Phase 0)
# ============================================================================
# These flags control which methodology enhancements are active.
# All default to False for backward compatibility.
# Enable incrementally as each phase is validated.

# Phase 1: Data Foundations
USE_EXTERNAL_MARKET_PROXY = False  # Use SPY from multiasset_new vs. internal cap-weighted
ENFORCE_PIT = False                 # Enforce point-in-time data availability
PIT_LAG_DAYS = 90                   # Days to add to fundamentals_datadate for effective_date

# Phase 2: Factor Normalization & Regression
ORTHOGONALIZE_FACTORS = False       # Orthogonalize style factors to size/country
USE_RIDGE_REGRESSION = False        # Use Ridge regression fallback for ill-conditioned matrices
RIDGE_ALPHA = 1e-4                  # Ridge regularization parameter (auto-tuned if needed)
SMOOTH_SPECIFIC_RISK = False        # Apply EWMA smoothing + shrinkage to specific risk
SPECIFIC_RISK_LAMBDA = 0.94         # EWMA decay parameter for specific risk
SPECIFIC_RISK_SHRINKAGE = 0.1       # Shrinkage weight toward industry median

# Phase 3: Covariance & Factor Definitions
SHRINK_COVARIANCE = False           # Apply Ledoit-Wolf shrinkage to factor covariance
COV_SHRINKAGE_INTENSITY = 0.0       # Manual shrinkage intensity (0=auto Ledoit-Wolf, >0=fixed)
PSD_ENFORCEMENT = True              # Enforce PSD via eigenvalue clipping
EIGENVALUE_FLOOR = 1e-8             # Minimum eigenvalue for PSD enforcement
BLEND_SHORT_LONG_COV = False        # Blend 12-month and 60-month covariance estimates
COV_SHORT_HORIZON_MONTHS = 12       # Short horizon for covariance (recent volatility)
COV_LONG_HORIZON_MONTHS = 60        # Long horizon for covariance (stable correlations)
COV_SHORT_WEIGHT = 0.5              # Weight for short horizon in blend
MULTI_HORIZON_GROWTH = False        # Use 1/3/5-year weighted growth vs. 1-year only
INCLUDE_MOMENTUM_REVERSAL = False   # Include reversal component in momentum

# Phase 4: Enhanced Factor Definitions
MULTI_HORIZON_MOMENTUM = False      # Use multi-horizon (1/3/6/12mo) momentum vs. 12-2 only
MOMENTUM_1MO_WEIGHT = 0.05          # Weight for 1-month return in momentum
MOMENTUM_3MO_WEIGHT = 0.15          # Weight for 3-month return in momentum
MOMENTUM_6MO_WEIGHT = 0.30          # Weight for 6-month return in momentum
MOMENTUM_12MO_WEIGHT = 0.50         # Weight for 12-month return in momentum
MOMENTUM_REVERSAL_WEIGHT = 0.05     # Weight to subtract for 1-month reversal
GROWTH_1YR_WEIGHT = 0.50            # Weight for 1-year earnings growth
GROWTH_3YR_WEIGHT = 0.30            # Weight for 3-year CAGR
GROWTH_5YR_WEIGHT = 0.20            # Weight for 5-year CAGR

# Current model version
SCHEMA_VERSION = "v1.0"             # Incremented with each methodology change
