# Barra Risk Model Integration

**Date**: November 16, 2024  
**Barra Release**: 2025-09-30

## Overview

Barra risk model data has been imported from `~/tax_aware/barra/exports/` into the parametric system for use in portfolio optimization and risk calculations.

## Data Files

### Imported Files

1. **factor_covariance_2025-09-30.csv** (4.4 MB)
   - Factor covariance matrix
   - Format: month_end_date, factor_i, factor_j, covariance
   - 235 factors (style + industry + country)

2. **factor_returns_2025-09-30.csv** (13 KB)
   - Historical factor returns
   - Format: month_end_date, factor, factor_return

3. **style_exposures_2025-09-30.csv** (749 KB)
   - Factor exposures for 1,444 securities
   - Format: month_end_date, gvkey, factor, exposure, flags
   - 10 style factors per security

4. **specific_risk_2025-09-30.csv** (56 KB)
   - Idiosyncratic risk for each security
   - Format: month_end_date, gvkey, specific_var
   - 1,442 securities

5. **gvkey_ticker_mapping.csv** (28 KB)
   - GVKEY to ticker symbol mapping
   - 2,388 mappings
   - Format: GVKEY, ticker

6. **portfolio_summary_2025-09-30.csv** (806 bytes)
   - Sample portfolio summary statistics

## Data Structure

### Style Factors (10 factors)
- beta
- book_to_price
- currency_sensitivity
- dividend_yield
- earnings_variability
- earnings_yield
- growth
- leverage
- momentum
- size

### Industry Factors
- GICS sector/industry classifications
- One-hot encoded exposures

### Country Factor
- country:US (US market exposure)

## Usage

### Loading Barra Data

```python
from src.data.barra_loader import BarraDataLoader
from src.data.gvkey_mapper import GVKEYMapper

# Load Barra data
loader = BarraDataLoader()
release_date = loader.find_latest_release()  # "2025-09-30"

# Load style exposures
exposures = loader.load_style_exposures(release_date)

# Load factor covariance matrix
cov_matrix = loader.get_factor_covariance_matrix(release_date)

# Load specific risk
specific_risk = loader.load_specific_risk(release_date)

# Map GVKEY to ticker
mapper = GVKEYMapper()
ticker = mapper.gvkey_to_ticker("001045")  # Returns "AAL"
gvkey = mapper.ticker_to_gvkey("AAPL")  # Returns GVKEY
```

### Using in Risk Model

The `RiskModel` class automatically uses Barra data if available:

```python
from src.optimization import RiskModel

# Risk model will automatically use Barra data if available
risk_model = RiskModel(session, use_barra=True)

# Calculate portfolio risk
risk_metrics = risk_model.calculate_portfolio_risk(weights)
```

## Integration Status

- ✅ Barra data imported
- ✅ Data loader implemented (`BarraDataLoader`)
- ✅ GVKEY mapper implemented (`GVKEYMapper`)
- ✅ Risk model updated to use Barra data
- ⚠️  Note: Barra uses GVKEY identifiers, parametric system uses tickers
- ⚠️  Mapping required for integration (handled by `GVKEYMapper`)

## Next Steps

1. Update optimization engine to use Barra factor exposures
2. Integrate Barra covariance matrix into portfolio risk calculations
3. Use Barra specific risk for more accurate risk estimates
4. Test with real portfolios

## Data Refresh

To refresh Barra data:

```bash
python scripts/import_barra_data.py
```

This will:
1. Find latest Barra release
2. Copy files to `data/raw/barra/`
3. Create data summary
4. Update README

