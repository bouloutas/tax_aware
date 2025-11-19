# Fama-French Factor Construction & Portfolio Optimization Project

## Overview
This project implements a comprehensive quantitative finance pipeline for constructing Fama-French factors, scoring stock universes, and optimizing long/short portfolios. The system combines academic factor construction with practical portfolio optimization techniques.

## Project Structure

### Core Modules

#### 1. Factor Construction (`factor_construction.py`)
- **Purpose**: Downloads and constructs Fama-French 5-factor model components
- **Key Functions**:
  - Downloads official Ken French 5-factor data from Dartmouth
  - Constructs custom SMB, HML, RMW, CMA factors using size/characteristic sorts
  - Combines official market factors (Mkt-RF, RF) with constructed factors
  - Calculates factor exposures (betas) for individual stocks
- **Data Sources**: Ken French website, Compustat data via MySQL
- **Output**: Combined factors table with correlations vs. official series

#### 2. Portfolio Optimization (`portfolio_optimization.py`)
- **Purpose**: Optimizes portfolios to track benchmark (SPY) with factor constraints
- **Key Functions**:
  - Minimizes tracking error to benchmark using SLSQP optimization
  - Supports predefined ticker lists or dynamic universe selection
  - Calculates factor exposures for optimized portfolios
  - Implements min/max weight constraints per stock
- **Optimization**: SLSQP with equality constraints and bounds
- **Analysis**: In-sample tracking error, factor beta analysis

#### 3. Advanced Optimizer (`advanced_optimizer.py`)
- **Purpose**: Minimum variance long/short portfolio construction
- **Key Functions**:
  - Uses decile-based long/short selection (deciles 1-2 long, 9-10 short)
  - Minimizes portfolio variance using 36-month covariance matrix
  - Saves latest optimized portfolio and candidates to `results/` directory
  - Implements separate long/short weight constraints
- **Output**: CSV files with optimized weights and candidate lists

#### 4. Optimized Backtest (`run_optimized_backtest.py`)
- **Purpose**: Master orchestration script for factor-based backtesting
- **Key Functions**:
  - Runs prerequisite pipelines (Fama-French refresh, factor scoring)
  - Loads scored universe and historical returns from MySQL
  - Optimizes weights to maximize factor score minus risk penalty
  - Backtests through time with weekly rebalancing
  - Generates performance analysis and plots
- **Strategy**: Long/short with factor score maximization and risk aversion

#### 5. Tax Lots Management (`tax_lots.py`)
- **Purpose**: Interactive Brokers tax lot tracking and wash sale detection
- **Key Functions**:
  - Fetches executions from IBKR TWS
  - Builds tax lots using FIFO methodology
  - Detects wash sale replacements (±30 day window)
  - Classifies lots as short-term (<365 days) or long-term
- **Output**: CSV and JSON files with tax lot details

### Supporting Files

#### Database Scripts
- `copy_to_mysql_ff.py`: Copies Fama-French data to MySQL
- `copy_optimization_returns_to_mysql.py`: Copies optimization returns to MySQL
- `execute_sql_file.py`: Executes SQL files against MySQL

#### SQL Definitions
- `sql/build_fama_french_tables.sql`: Creates required MySQL tables
- `sql/get_optimization_returns.sql`: Queries for optimization returns
- `sql/test_permissions.sql`: Tests database permissions

#### Shell Scripts
- `run_complete_pipeline.sh`: Runs the complete pipeline
- `run_FF_daily.sh`: Daily Fama-French data refresh

#### Configuration
- `get_config_tickers.py`: Retrieves configuration tickers

### Data Flow

1. **Data Ingestion**: Compustat data → MySQL (`fama_french_local`)
2. **Factor Construction**: Raw data → Ken French factors + custom factors → Combined factors table
3. **Universe Scoring**: Custom factor scoring pipeline → `manual_weights.universe_factor_scores`
4. **Portfolio Optimization**: Scored universe + historical returns → Optimized weights
5. **Backtesting**: Historical optimization → Performance analysis → Plots and reports

### Key Dependencies

#### Databases
- **MySQL**: Primary data storage (`fama_french_local`, `manual_weights`)
- **Tables**: `final_combined_factors`, `optimization_portfolio_monthly_returns`, `universe_factor_scores`

#### External APIs
- **Ken French Data**: Dartmouth MBA website
- **Interactive Brokers**: TWS API for tax lots
- **Yahoo Finance**: Fallback for SPY data

#### Python Libraries
- **Optimization**: `scipy.optimize.minimize` (SLSQP)
- **Database**: `sqlalchemy`, `mysql.connector`, `pymysql`
- **Analysis**: `pandas`, `numpy`, `statsmodels`
- **Visualization**: `matplotlib`
- **Trading**: `ib_insync`

### Configuration Parameters

#### Portfolio Constraints
- Long positions: Max 2% per stock
- Short positions: Max -2% per stock
- Lookback period: 36 months for covariance
- Risk aversion: λ = 0.5 (tunable)

#### Universe Selection
- Long candidates: Top 75 stocks by factor score
- Short candidates: Bottom 75 stocks by factor score
- Alternative: Decile-based selection (1-2 long, 9-10 short)

#### Optimization Methods
- **Score-Risk**: Maximize factor score minus risk penalty
- **Min Variance**: Minimize portfolio variance
- **Tracking Error**: Minimize deviation from benchmark

### Output Files

#### Results Directory (`results/`)
- `optimized_portfolio_YYYYMMDD.csv`: Latest optimized weights
- `portfolio_candidates_YYYYMMDD.csv`: Full candidate universe

#### Tax Management
- `tax_lots.csv`: Tax lot details
- `tax_lots.json`: JSON format tax lots

### Performance Metrics

#### Strategy Performance
- Annualized return and volatility
- Sharpe ratio
- Maximum drawdown
- Factor exposures (betas)

#### Benchmark Comparisons
- Equal-weighted long/short
- Individual long/short legs
- Market benchmark (SPY)

### Usage Examples

#### Run Complete Pipeline
```bash
./run_complete_pipeline.sh
```

#### Daily Factor Refresh
```bash
./run_FF_daily.sh
```

#### Individual Components
```python
# Factor construction
python factor_construction.py

# Portfolio optimization
python portfolio_optimization.py

# Advanced optimization
python advanced_optimizer.py

# Tax lot management
python tax_lots.py
```

### Future Enhancements

#### Planned Improvements
- Real-time factor updates
- Additional factor models (momentum, quality)
- Transaction cost modeling
- Risk management overlays
- Multi-asset class support

#### Scalability Considerations
- Database migration to DuckDB
- Parallel processing for factor construction
- Cloud deployment options
- API integration for real-time data

## Technical Notes

### Database Schema
The project uses MySQL with the following key tables:
- `final_combined_factors`: Date, Mkt_RF, RF, SMB, HML, RMW, CMA
- `optimization_portfolio_monthly_returns`: TICKER, MONTH_END_DATE, MONTHLY_RETURN
- `universe_factor_scores`: ticker, datadate, factor_score, decile, 5d_forward_return

### Optimization Constraints
- Long/short neutrality: Σw_long = 1, Σw_short = -1
- Per-stock bounds: [0, 0.02] for longs, [-0.02, 0] for shorts
- Risk aversion parameter controls return vs. risk trade-off

### Factor Construction Methodology
- Size breakpoints: NYSE median market cap
- Characteristic breakpoints: NYSE 30th/70th percentiles
- Portfolio construction: 2x3 sorts for each factor
- Factor calculation: Long-short portfolio returns

This project represents a complete quantitative finance pipeline from data ingestion through portfolio optimization and tax management, suitable for both research and practical implementation.
