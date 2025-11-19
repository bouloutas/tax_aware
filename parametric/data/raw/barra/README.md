# Barra Risk Model Data

**Release Date**: 2025-09-30
**Source**: ~/tax_aware/barra/exports/releases/2025-09-30/
**Imported**: 2025-11-16

## Files

### Factor Covariance
- **File**: `factor_covariance_2025-09-30.csv` / `.parquet`
- **Rows**: 1000
- **Columns**: month_end_date, factor_i, factor_j, covariance...
- **Size**: 4.32 MB

### Factor Returns
- **File**: `factor_returns_2025-09-30.csv` / `.parquet`
- **Rows**: 227
- **Columns**: month_end_date, factor, factor_return...
- **Size**: 0.01 MB

### Style Exposures
- **File**: `style_exposures_2025-09-30.csv` / `.parquet`
- **Rows**: 1000
- **Columns**: month_end_date, gvkey, factor, exposure, flags...
- **Size**: 0.73 MB

### Specific Risk
- **File**: `specific_risk_2025-09-30.csv` / `.parquet`
- **Rows**: 1000
- **Columns**: month_end_date, gvkey, specific_var...
- **Size**: 0.05 MB

### Portfolio Summary
- **File**: `portfolio_summary_2025-09-30.csv` / `.parquet`
- **Rows**: 12
- **Columns**: factor, portfolio_exposure, variance_contribution, type, top_n...
- **Size**: 0.0 MB

## Data Description

### Factor Covariance
Covariance matrix of factor returns. Used for portfolio risk calculation.

### Factor Returns
Historical factor returns (style factors, industry factors, country factor).

### Style Exposures
Factor exposures for each security:
- Style factors: Beta, Momentum, Size, Earnings Yield, Book-to-Price, Growth, Earnings Variability, Leverage, Currency Sensitivity, Dividend Yield
- Industry factors: GICS sector/industry exposures
- Country factor: US market exposure

### Specific Risk
Idiosyncratic (stock-specific) risk for each security.

### Portfolio Summary
Summary statistics for a sample portfolio (if available).

## Usage

These files can be used by the portfolio optimization engine to:
1. Calculate portfolio risk using factor exposures
2. Estimate tracking error
3. Optimize portfolios with risk constraints

See `src/optimization/risk_model.py` for integration.
