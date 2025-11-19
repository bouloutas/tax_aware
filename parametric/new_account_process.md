## New Account Onboarding and Daily Process (Parametric-Style, Tax-Aware)

Last updated: 2025-11-16

This document describes how to onboard a new taxable account and the daily operational process, including ingesting Barra inputs, market data, portfolio formats, optimization, and rebalancing.

---

## 1) Environment and Prerequisites

- Conda env: dgx-spark
- Database (PostgreSQL preferred):
  - DATABASE_URL=postgresql://USER:PASS@HOST:5432/tax_aware_portfolio
  - USE_SQLITE=false
- Verify packages:
  - python scripts/verify_environment.py

---

## 2) Data Inputs Overview

### 2.1 Barra Inputs (monthly, as-of date)
- Location: ~/tax_aware/barra/exports/releases/YYYY-MM-DD/
- Files (CSV preferred; Parquet equivalents also present):
  - factor_covariance_YYYY-MM-DD.csv
    - Columns: month_end_date, factor_i, factor_j, covariance
  - factor_returns_YYYY-MM-DD.csv
    - Columns: month_end_date, factor, factor_return
  - style_exposures_YYYY-MM-DD.csv
    - Columns: month_end_date, gvkey, factor, exposure, flags
  - specific_risk_YYYY-MM-DD.csv
    - Columns: month_end_date, gvkey, specific_var
  - manifest.json (metadata)
- Imported location (project): data/raw/barra/
- Import script:
  - python scripts/import_barra_data.py
  - Copies latest release, writes data_summary_*.json, README.md

### 2.2 Market Data (daily)
- Source: yfinance (default), or existing DBs if configured
- Storage: table market_data (Security-level, daily close)
- Functions:
  - MarketDataManager.download_and_store_price_data(ticker, start_date, end_date)
  - MarketDataManager.get_price_on_date(security_id, date)
  - MarketDataManager.get_latest_price(security_id)

### 2.3 Benchmarks
- Stored in tables benchmarks, benchmark_constituents
- Daily-return benchmark (e.g., SPY proxy) or equal-weight list
- Management via BenchmarkManager

---

## 3) Portfolio Formats (Accepted)

### 3.1 Target Portfolio (CSV)
- Columns: ticker, target_weight
- Weights sum to 1.0 (or <=1.0 if cash allowed)
- Example:
  ticker,target_weight
  AAPL,0.20
  MSFT,0.20
  META,0.20
  XOM,0.20
  PFE,0.20

### 3.2 Holdings / Positions (CSV)
- Columns: ticker, quantity, cost_basis, purchase_date
- If tax-lots are provided, one row per lot (preferred)
- Example:
  ticker,quantity,cost_basis,purchase_date
  AAPL,123.45,154.32,2023-05-10

### 3.3 Tax Lots (CSV)
- Columns: ticker, quantity, purchase_price, purchase_date
- Example:
  ticker,quantity,purchase_price,purchase_date
  MSFT,468.2815,103.1627,2023-08-26

### 3.4 JSON (Alternative)
- target_portfolio.json:
  { "AAPL": 0.20, "MSFT": 0.20, "META": 0.20, "XOM": 0.20, "PFE": 0.20 }
- tax_lots.json:
  [
    {"ticker": "MSFT", "quantity": 468.2815, "purchase_price": 103.1627, "purchase_date": "2023-08-26"}
  ]

---

## 4) New Account Onboarding (One-Time)

1. Create Account
   - src/core/account_manager.py
   - Fields: client_name, account_type (taxable), tax rates
2. Create/Assign Benchmark
   - BenchmarkManager.create_benchmark("SP500_SPY") or custom
   - Add constituents:
     - SPY 100% (proxy), or full constituent set with weights
3. Load/Map Securities
   - Insert new tickers into table securities (ticker, company_name, sector, etc.)
4. Load Market Data
   - For all tickers in portfolio/benchmark, fetch recent daily prices
5. Load Tax Lots
   - Create tax lots per ticker (long-term preferred for this system unless specified)
   - PositionManager.create_tax_lot(...)
6. Verify
   - Test script: scripts/test_setup.py
   - Optional: run a dry TE calc

---

## 5) Daily Process (Operations)

1. Acquire Barra Release (monthly; validate daily process uses latest)
   - Ensure ~/tax_aware/barra/exports/releases/YYYY-MM-DD exists
   - Run import:
     - python scripts/import_barra_data.py
   - Confirms files in data/raw/barra/
2. Market Data Refresh (daily)
   - For all tracked tickers: MarketDataManager.download_and_store_price_data(ticker, start, end)
   - Verify 1-day close available for each security
3. Risk Prep
   - BarraDataLoader loads style_exposures, factor_covariance, specific_risk
   - GVKEYMapper maps GVKEY↔ticker
4. Tracking Error & Risk
   - TrackingErrorCalculator.calculate_tracking_error(account_id, lookback_days=60/252)
   - Barra-based factor TE:
     - Build portfolio and benchmark exposures
     - Compute active exposures
     - TE = sqrt(x' Σ x) using factor_covariance
5. Optimization
   - PortfolioOptimizer.optimize_portfolio(account_id, max_tracking_error=…)
   - Incorporate constraints (turnover, sectors, etc.)
   - Optionally include tax-loss opportunities
6. Rebalancing
   - Rebalancer.check_rebalancing_needed(account_id, type='threshold'|'scheduled'|'tax_loss')
   - Rebalancer.rebalance_account(..., auto_execute=False for approval flow)
   - Output trades (buy/sell per ticker and quantities/lots)
7. Approvals & Execution
   - Run compliance checks
   - If approved, execute trades (integration-point ready)
8. Reporting
   - Store rebalancing events, TE, exposures, and performance snapshots

---

## 6) How To Ingest Barra Inputs (Step-by-Step)

1. Ensure the latest month-end folder exists:
   - ~/tax_aware/barra/exports/releases/YYYY-MM-DD/
   - Must contain CSVs listed in 2.1
2. Import to project:
   - python scripts/import_barra_data.py
   - Copies to data/raw/barra/
   - Generates data_summary_YYYY-MM-DD.json and README.md
3. Verify:
   - style_exposures_YYYY-MM-DD.csv present with ~10 style factors per security
   - factor_covariance_YYYY-MM-DD.csv present with full factor set (style+industry+country)
4. Use in code:
   - from src.data.barra_loader import BarraDataLoader
   - from src.data.gvkey_mapper import GVKEYMapper
   - loader.load_style_exposures(...), loader.load_factor_covariance(...), etc.

---

## 7) Example Daily Command Sequence

```bash
# 0) Environment
conda activate dgx-spark
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/tax_aware_portfolio

# 1) Import latest Barra release
python scripts/import_barra_data.py

# 2) Refresh market data (example subset)
python - <<'PY'
from datetime import date, timedelta
from src.core.database import create_database_engine, get_session_factory
from src.core.config import Config
from src.data.market_data import MarketDataManager
engine = create_database_engine(Config.get_database_url(), echo=False)
Session = get_session_factory(engine)
session = Session()
mkt = MarketDataManager(session)
for t in ["MSFT","DHR","META","XOM","PFE","SPY"]:
    mkt.download_and_store_price_data(t, start_date=date.today()-timedelta(days=10), end_date=date.today())
print("Market data refreshed")
session.close()
PY

# 3) (Optional) Compute TE vs SPY
python scripts/set_sp500_benchmark_and_te.py
```

---

## 8) Notes and Conventions

- Units/Scales:
  - Factor returns and covariance are unit-consistent with monthly Barra files; TE calculation uses sqrt(x' Σ x).
  - Tracking error in TrackingErrorCalculator is annualized by default (√252) for daily series.
- Identifiers:
  - Internally use tickers (parametric system); Barra files use GVKEY; GVKEYMapper bridges.
- Stability Guards:
  - If no benchmark is assigned, code falls back to equal-weight current holdings for optimization in tests.
- Persistence:
  - PostgreSQL preferred; SQLite supported for quick tests.

---

## 9) Appendices

### 9.1 Minimal API/Script Entrypoints
- Initialize DB: scripts/init_database.py
- Scenario example: scripts/run_scenario_20240930.py
- Barra analysis: scripts/run_barra_analysis_20240930.py
- Set SP500 benchmark + TE: scripts/set_sp500_benchmark_and_te.py
- Import Barra data: scripts/import_barra_data.py

### 9.2 Troubleshooting
- Missing Barra files:
  - Re-run python scripts/import_barra_data.py and confirm source path
- No market data on date:
  - Re-run download_and_store_price_data with a wider window
- Import errors:
  - Ensure PYTHONPATH includes project root
  - Verify requirements via scripts/verify_environment.py


