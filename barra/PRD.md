# Barra Risk Model Implementation PRD

## Executive Summary

This document specifies the requirements for implementing a Barra-style risk model for US equities using Compustat fundamental data. The model decomposes stock returns into style factors, industry factors, and a country factor, enabling risk attribution, portfolio risk analysis, and factor-based portfolio construction.

## Model Overview

The Barra risk model follows a multi-factor framework where stock returns are decomposed as:

```
r_i = Σ(β_ik * f_k) + ε_i
```

Where:
- `r_i` = return of stock i
- `β_ik` = exposure of stock i to factor k
- `f_k` = return of factor k
- `ε_i` = stock-specific (idiosyncratic) return

The model includes:
- **10 Style Factors**: Beta, Momentum, Size, Earnings Yield, Book-to-Price, Growth, Earnings Variability, Leverage, Currency Sensitivity, Dividend Yield
- **Industry Factors**: GICS sector/industry classifications (11 sectors, ~70 industries)
- **Country Factor**: US market factor (common to all stocks)

## Style Factors - Detailed Specifications

### 1. Beta (Market Sensitivity)

**Definition**: Measures sensitivity to market movements, calculated as the regression coefficient of stock returns on market returns.

**Compustat Data Requirements**:
- **Price Data**: Monthly stock prices (PRCCM from CRSP/Compustat merged, or external price data)
- **Market Index**: S&P 500 or Russell 3000 returns
- **Lookback Period**: 60 months of monthly returns

**Calculation**:
```
Beta = Cov(r_stock, r_market) / Var(r_market)
```
Where returns are calculated over rolling 60-month windows.

**Data Frequency**: Monthly (requires price data, not available in Compustat alone)

**Normalization**: 
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: Use industry median beta if stock beta unavailable

---

### 2. Momentum

**Definition**: Price momentum factor measuring recent stock performance relative to market.

**Compustat Data Requirements**:
- **Price Data**: Monthly stock prices (requires external price data or CRSP merge)
- **Lookback Periods**: 
  - 1-month momentum (skip most recent month)
  - 12-month momentum (months 2-12)
  - 12-month momentum (months 13-24)

**Calculation**:
```
Momentum = (1 + r_1m) * (1 + r_12m) - 1
```
Where:
- `r_1m` = return from month t-2 to t-1 (skip most recent month)
- `r_12m` = return from month t-13 to t-2

**Data Frequency**: Monthly (requires price data)

**Normalization**: 
- Log transform: `log(1 + Momentum)`
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: Set to zero if insufficient price history

---

### 3. Size (Market Capitalization)

**Definition**: Natural logarithm of market capitalization, measuring company size effect.

**Compustat Data Requirements**:
- **Annual Data**:
  - `PRCC_F` (Price - Fiscal Year Close) - Annual data item
  - `CSHO` (Common Shares Outstanding) - Annual data item
- **Quarterly Data** (for more frequent updates):
  - `PRCCQ` (Price - Quarter Close) - Quarterly data item
  - `CSHOCQ` (Common Shares Outstanding - Current Quarter) - Quarterly data item

**Calculation**:
```
Market Cap = PRCC_F * CSHO  (annual)
Market Cap = PRCCQ * CSHOCQ  (quarterly)
Size = log(Market Cap)
```

**Data Frequency**: Quarterly preferred (more timely), Annual acceptable

**Normalization**: 
- Already log-transformed
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: 
- Use most recent available quarter/annual data
- If completely missing, use industry median

---

### 4. Earnings Yield

**Definition**: Earnings-to-price ratio, measuring value characteristics.

**Compustat Data Requirements**:
- **Annual Data**:
  - `IB` (Income Before Extraordinary Items) - Annual data item
  - `PRCC_F` (Price - Fiscal Year Close) - Annual data item
  - `CSHO` (Common Shares Outstanding) - Annual data item
- **Quarterly Data** (for trailing 12-month):
  - `IBQ` (Income Before Extraordinary Items - Quarterly)
  - `PRCCQ` (Price - Quarter Close)
  - `CSHOCQ` (Common Shares Outstanding - Current Quarter)

**Calculation**:
```
Earnings Per Share = IB / CSHO  (annual)
Earnings Yield = IB / (PRCC_F * CSHO) = EPS / Price
```

For quarterly: Use trailing 12-month earnings (sum of last 4 quarters)

**Data Frequency**: Quarterly preferred (trailing 12-month), Annual acceptable

**Normalization**: 
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: 
- If earnings negative or zero, set to missing
- Use industry median if missing

---

### 5. Book-to-Price (Value)

**Definition**: Book value of equity divided by market capitalization.

**Compustat Data Requirements**:
- **Annual Data**:
  - `CEQ` (Common/Ordinary Equity - Total) - Annual data item
  - `PRCC_F` (Price - Fiscal Year Close) - Annual data item
  - `CSHO` (Common Shares Outstanding) - Annual data item
- **Alternative**: `SEQ` (Stockholders' Equity - Total) if CEQ unavailable

**Calculation**:
```
Book Value Per Share = CEQ / CSHO
Market Price = PRCC_F
Book-to-Price = CEQ / (PRCC_F * CSHO) = Book Value / Market Cap
```

**Data Frequency**: Annual (book value typically reported annually)

**Normalization**: 
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: 
- If book value negative, set to missing
- Use industry median if missing

---

### 6. Growth

**Definition**: Earnings growth rate over multiple periods, measuring growth characteristics.

**Compustat Data Requirements**:
- **Annual Data**:
  - `IB` (Income Before Extraordinary Items) - Annual data item
  - Historical data for 1-year, 3-year, and 5-year growth calculations

**Calculation**:
```
EPS_t = IB_t / CSHO_t
EPS_t-1 = IB_t-1 / CSHO_t-1

1-Year Growth = (EPS_t - EPS_t-1) / |EPS_t-1|
3-Year Growth = ((EPS_t / EPS_t-3)^(1/3) - 1)
5-Year Growth = ((EPS_t / EPS_t-5)^(1/5) - 1)

Growth Factor = Weighted average of 1-year (50%), 3-year (30%), 5-year (20%)
```

**Data Frequency**: Annual

**Normalization**: 
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: 
- If insufficient history, use available periods only
- If all missing, use industry median

---

### 7. Earnings Variability

**Definition**: Volatility of earnings, measured as coefficient of variation of earnings over time.

**Compustat Data Requirements**:
- **Annual Data**:
  - `IB` (Income Before Extraordinary Items) - Annual data item
  - Historical data for 5-year window minimum

**Calculation**:
```
EPS_t = IB_t / CSHO_t  (for each year t in 5-year window)

Mean EPS = mean(EPS_t over 5 years)
Std Dev EPS = std(EPS_t over 5 years)

Earnings Variability = Std Dev EPS / |Mean EPS|
```

**Data Frequency**: Annual (requires 5-year history)

**Normalization**: 
- Log transform: `log(1 + Earnings Variability)`
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: 
- Require minimum 3 years of data
- If insufficient, use industry median

---

### 8. Leverage

**Definition**: Financial leverage, measured as total debt to equity ratio.

**Compustat Data Requirements**:
- **Annual Data**:
  - `DLTT` (Long-Term Debt - Total) - Annual data item
  - `DLC` (Debt in Current Liabilities - Total) - Annual data item
  - `CEQ` (Common/Ordinary Equity - Total) - Annual data item
- **Alternative**: `LT` (Liabilities - Total) and `AT` (Assets - Total) for total leverage

**Calculation**:
```
Total Debt = DLTT + DLC
Leverage = Total Debt / CEQ

Alternative (if CEQ missing or negative):
Total Leverage = LT / AT
```

**Data Frequency**: Annual

**Normalization**: 
- Log transform: `log(1 + Leverage)`
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: 
- If CEQ negative or zero, use alternative calculation
- If all missing, use industry median

---

### 9. Currency Sensitivity

**Definition**: Exposure to foreign currency movements, proxied by foreign sales or operations.

**Compustat Data Requirements**:
- **Annual Data**:
  - `PIFO` (Pretax Income - Foreign Operations) - Annual data item (if available)
  - `SALE` (Sales/Turnover - Net) - Annual data item
  - `SALEI` (Sales - International) - Annual data item (if available)
- **Segment Data** (if available):
  - Geographic segment sales breakdown

**Calculation**:
```
If SALEI available:
  Currency Sensitivity = SALEI / SALE  (Foreign Sales Ratio)

If PIFO available:
  Currency Sensitivity = PIFO / (PIFO + PIDOM)  (Foreign Income Ratio)

If neither available:
  Currency Sensitivity = 0  (assume domestic only)
```

**Data Frequency**: Annual

**Normalization**: 
- Already a ratio (0 to 1)
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: 
- Default to 0 if foreign data unavailable
- Many US companies will have 0 exposure

---

### 10. Dividend Yield

**Definition**: Annual dividend payments divided by market capitalization.

**Compustat Data Requirements**:
- **Annual Data**:
  - `DVC` (Dividends - Common/Ordinary) - Annual data item
  - `DVPSP_F` (Dividends Per Share - Pay Date - Fiscal) - Annual data item
  - `PRCC_F` (Price - Fiscal Year Close) - Annual data item
  - `CSHO` (Common Shares Outstanding) - Annual data item

**Calculation**:
```
Dividend Yield = DVC / (PRCC_F * CSHO)

Alternative (using per-share data):
Dividend Yield = DVPSP_F / PRCC_F
```

**Data Frequency**: Annual

**Normalization**: 
- Winsorize at 1st and 99th percentiles
- Standardize to z-scores

**Missing Data Handling**: 
- If no dividends paid, set to 0
- If DVC missing but company pays dividends, use DVPSP_F * CSHO

---

## Industry Factors

### GICS Classification

**Definition**: Industry factors represent sector and industry-specific risk exposures.

**Data Requirements**:
- **GICS Codes**: 
  - Sector Code (2-digit): 10-11 sectors
  - Industry Group Code (4-digit): ~25 industry groups
  - Industry Code (6-digit): ~70 industries
  - Sub-Industry Code (8-digit): ~150+ sub-industries

**Compustat Mapping**:
- Compustat provides GICS codes in:
  - `GSECTOR` (GICS Sector Code)
  - `GIND` (GICS Industry Code)
  - `GSUBIND` (GICS Sub-Industry Code)

**Factor Construction**:
1. Assign each stock to its GICS industry
2. Create industry dummy variables (1 if stock belongs to industry, 0 otherwise)
3. Industry factors are constructed to be market-capitalization weighted and industry-neutral
4. Factor returns calculated via cross-sectional regression

**Normalization**:
- Industry exposures are binary (0 or 1)
- Industry factors are orthogonalized to market factor
- Industry factor returns are market-cap weighted within each industry

**Missing Data Handling**:
- If GICS code missing, assign to "Unclassified" industry
- Use most recent available GICS code

---

## Country Factor

### US Market Factor

**Definition**: Common factor affecting all US equities, representing overall market movement.

**Construction**:
- Market-capitalization weighted portfolio of all US equities
- Typically represented by S&P 500 or Russell 3000 index returns
- All stocks have exposure of 1.0 to country factor

**Data Requirements**:
- Market index returns (S&P 500, Russell 3000, or custom market-cap weighted index)
- Can be calculated from individual stock returns and market caps

**Calculation**:
```
Country Factor Return = Σ(w_i * r_i)
Where w_i = Market Cap_i / Σ(Market Cap_all)
```

---

## Factor Construction Methodology

### Step 1: Data Extraction and Preparation

1. **Extract Compustat Data**:
   - Annual fundamental data (fiscal year end)
   - Quarterly fundamental data (for more timely updates)
   - GICS industry classifications
   - Market capitalization data

2. **Merge with Price Data**:
   - Compustat alone does not provide sufficient price data
   - Requires CRSP merge or external price data source
   - Monthly prices needed for Beta and Momentum factors

3. **Data Alignment**:
   - Align fiscal year data to calendar dates
   - Handle different fiscal year ends
   - Create point-in-time datasets (avoid look-ahead bias)

### Step 2: Factor Exposure Calculation

For each stock and each rebalance date:

1. **Calculate Raw Factor Exposures**:
   - Compute each style factor using formulas above
   - Assign industry exposures (binary)
   - Set country exposure = 1.0 for all stocks

2. **Apply Transformations**:
   - Log transforms (where specified)
   - Winsorization at 1st and 99th percentiles
   - Standardization to z-scores (mean=0, std=1)

3. **Handle Missing Data**:
   - Impute missing values using industry medians
   - Flag stocks with excessive missing data

### Step 3: Factor Return Calculation

**Cross-Sectional Regression**:

For each period t:
```
r_it = α_t + Σ(β_ik * f_kt) + ε_it
```

Where:
- `r_it` = stock i return in period t
- `β_ik` = exposure of stock i to factor k (known)
- `f_kt` = factor return for factor k in period t (estimated)
- `ε_it` = stock-specific return

**Estimation Method**:
- Weighted least squares (WLS) with market cap weights
- Or robust regression to handle outliers
- Industry factors orthogonalized to market factor

### Step 4: Factor Risk Model

**Covariance Matrix Construction**:

1. **Factor Covariance Matrix** (`Σ_F`):
   - Calculate factor return time series
   - Estimate covariance matrix using exponential weighting or rolling window
   - Apply shrinkage methods (Bayesian shrinkage, Ledoit-Wolf)

2. **Specific Risk** (`Σ_S`):
   - Diagonal matrix of stock-specific variances
   - Estimated from regression residuals
   - Apply shrinkage to specific risk estimates

3. **Total Risk**:
   ```
   Σ_Total = B * Σ_F * B' + Σ_S
   ```
   Where B is the exposure matrix

---

## Data Requirements Summary

### Compustat Annual Data Items

| Factor | Required Fields | Frequency |
|--------|----------------|-----------|
| Size | PRCC_F, CSHO | Annual |
| Earnings Yield | IB, PRCC_F, CSHO | Annual |
| Book-to-Price | CEQ, PRCC_F, CSHO | Annual |
| Growth | IB, CSHO (historical) | Annual |
| Earnings Variability | IB, CSHO (5-year history) | Annual |
| Leverage | DLTT, DLC, CEQ | Annual |
| Currency Sensitivity | SALEI, SALE (or PIFO) | Annual |
| Dividend Yield | DVC, PRCC_F, CSHO | Annual |
| Industry | GSECTOR, GIND, GSUBIND | Annual |
| Beta | Requires price data (external) | Monthly |
| Momentum | Requires price data (external) | Monthly |

### Compustat Quarterly Data Items (Optional, for timeliness)

| Factor | Required Fields | Frequency |
|--------|----------------|-----------|
| Size | PRCCQ, CSHOCQ | Quarterly |
| Earnings Yield | IBQ (trailing 12-month), PRCCQ, CSHOCQ | Quarterly |

### External Data Requirements

- **Price Data**: Monthly stock prices (CRSP or equivalent)
- **Market Index**: S&P 500 or Russell 3000 returns
- **Market Capitalization**: For market-cap weighting and size factor

---

## Implementation Phases

### Phase 1: Data Infrastructure
- Set up Compustat data extraction pipeline
- Integrate price data source (CRSP or equivalent)
- Create point-in-time data structure
- Implement data quality checks

### Phase 2: Factor Calculation Engine
- Implement each style factor calculation
- Create industry factor assignment logic
- Build factor exposure calculation pipeline
- Apply transformations and normalization

### Phase 3: Factor Return Estimation
- Implement cross-sectional regression
- Calculate factor returns
- Build factor covariance matrix
- Estimate specific risk

### Phase 4: Risk Model Application
- Portfolio risk calculation
- Risk attribution analysis
- Factor exposure reporting
- Performance attribution

### Phase 5: Validation and Testing
- Compare factor returns to published Barra factors
- Validate risk forecasts
- Backtest model performance
- Document deviations from standard Barra model

---

## Key Design Decisions

### 1. Data Frequency
- **Annual factors**: Use annual Compustat data (more reliable, less frequent)
- **Quarterly factors**: Use quarterly data where available (more timely, may be noisier)
- **Hybrid approach**: Use quarterly for Size and Earnings Yield, annual for others

### 2. Factor Orthogonalization
- Industry factors orthogonalized to market factor
- Style factors may be orthogonalized to reduce multicollinearity
- Consider factor rotation techniques

### 3. Missing Data Strategy
- Industry median imputation for missing style factors
- Flag stocks with >50% missing factors
- Consider excluding stocks with insufficient data history

### 4. Rebalancing Frequency
- Monthly rebalancing (standard for Barra models)
- Factor exposures updated monthly
- Factor returns calculated monthly

### 5. Universe Definition
- US equities only
- Minimum market cap threshold (e.g., $100M)
- Exclude REITs, financials, or include with special handling
- Minimum price threshold (e.g., $1)

---

## Validation Criteria

### Factor Return Validation
- Compare factor returns to published Barra USE3/USE4 factors
- Check factor return correlations (should be high but not perfect)
- Validate factor return volatility and distribution

### Risk Forecast Validation
- Compare predicted vs. realized volatility
- Check risk forecast accuracy (hit rates, calibration)
- Validate portfolio risk decomposition

### Exposure Validation
- Verify factor exposure distributions
- Check for outliers and data errors
- Validate industry assignments

---

## Deliverables

1. **PRD.md** (this document) - Complete factor specifications
2. **Factor Calculation Code** - Python/R implementation
3. **Data Pipeline** - Compustat extraction and processing
4. **Factor Return Estimation** - Regression implementation
5. **Risk Model** - Covariance matrix and risk calculation
6. **Documentation** - User guide and API documentation
7. **Validation Report** - Comparison to Barra benchmarks

---

## References

- MSCI Barra USE3/USE4 Model Documentation
- Compustat Data Guide
- Academic papers on multi-factor models
- Barra risk model methodology papers

---

## Appendix: Compustat Field Reference

### Key Annual Data Items

| Field Name | Mnemonic | Description |
|------------|----------|-------------|
| Assets - Total | AT | Total assets |
| Common/Ordinary Equity - Total | CEQ | Book value of equity |
| Common Shares Outstanding | CSHO | Shares outstanding |
| Debt in Current Liabilities - Total | DLC | Short-term debt |
| Long-Term Debt - Total | DLTT | Long-term debt |
| Dividends - Common/Ordinary | DVC | Total dividends paid |
| Dividends Per Share - Pay Date - Fiscal | DVPSP_F | Dividends per share |
| Income Before Extraordinary Items | IB | Net income |
| Liabilities - Total | LT | Total liabilities |
| Price - Fiscal Year Close | PRCC_F | Year-end stock price |
| Sales/Turnover - Net | SALE | Total sales |
| Sales - International | SALEI | Foreign sales |
| GICS Sector Code | GSECTOR | GICS sector |
| GICS Industry Code | GIND | GICS industry |
| GICS Sub-Industry Code | GSUBIND | GICS sub-industry |

### Key Quarterly Data Items

| Field Name | Mnemonic | Description |
|------------|----------|-------------|
| Common Shares Outstanding - Current Quarter | CSHOCQ | Quarterly shares outstanding |
| Income Before Extraordinary Items - Quarterly | IBQ | Quarterly net income |
| Price - Quarter Close | PRCCQ | Quarter-end stock price |

---

**Document Version**: 1.0  
**Last Updated**: 2025-11-14  
**Author**: Risk Model Development Team

