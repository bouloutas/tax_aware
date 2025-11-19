# Product Requirements Document (PRD)
## Parametric-Style Tax-Aware Portfolio Management System

**Version:** 1.0  
**Date:** November 16, 2024  
**Author:** Tax-Aware Portfolio Management Team  
**Status:** Draft

---

## Executive Summary

This document outlines the requirements for building a tax-aware portfolio management system inspired by Parametric Portfolio Associates' Direct Indexing and tax-loss harvesting platform. The system will enable automated tax-loss harvesting, portfolio optimization with tracking error constraints, and active rebalancing to minimize tax liabilities while maintaining benchmark alignment.

**Target:** Replicate Parametric's core functionality for tax-aware portfolio management on a local machine, with eventual scalability to support multiple client portfolios.

---

## 1. Company Overview: Parametric Portfolio Associates

### 1.1 Business Model
- **Company**: Parametric Portfolio Associates LLC (subsidiary of Morgan Stanley Investment Management)
- **Founded**: 1987
- **Assets Under Management**: $566+ billion (as of March 2025)
- **Core Service**: Tax-aware portfolio management through Direct Indexing and Separately Managed Accounts (SMAs)
- **Client Base**: Wealth managers, institutional investors, individual investors (minimum $100K+ portfolios)

### 1.2 Key Offerings
1. **Direct Indexing**: Individual stock portfolios that replicate indices (e.g., S&P 500, Russell 3000) with tax-loss harvesting
2. **Tax Harvest Core**: Systematic tax-loss harvesting using sector-based ETFs
3. **Custom Active**: Tax-optimized overlay for actively managed equity strategies
4. **Custom SMAs**: Separately managed accounts with tax management and customization

### 1.3 Value Proposition
- **Primary Goal**: Maximize after-tax returns while maintaining pre-tax benchmark tracking
- **Key Technique**: Active tax-loss harvesting throughout the year (not just year-end)
- **Differentiator**: Continuous monitoring and opportunistic loss realization
- **Tax Benefits**: 
  - Offset capital gains with harvested losses
  - Defer tax payments
  - Optimize long-term vs. short-term capital gains
  - Avoid wash sale violations

---

## 2. System Architecture Overview

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Interface Layer                    │
│  (Web Portal / API / Integration with Wealth Managers)     │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                 Portfolio Management Layer                    │
│  • Account Management  • Position Tracking  • Tax Lot Mgmt   │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│              Tax-Loss Harvesting Engine                       │
│  • Loss Identification  • Wash Sale Detection  • Optimization│
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│            Portfolio Optimization Engine                      │
│  • Risk Model  • Tracking Error Constraints  • Rebalancing   │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                  Data & Infrastructure Layer                  │
│  • Market Data  • Tax Data  • Risk Models  • Database        │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Core Components

1. **Portfolio Management System**
2. **Tax-Loss Harvesting Engine**
3. **Portfolio Optimization Engine**
4. **Risk Management System**
5. **Rebalancing Engine**
6. **Client Portal/API**
7. **Data Management System**

---

## 3. Detailed Component Specifications

### 3.1 Portfolio Management System

#### 3.1.1 Account Structure
- **Separately Managed Accounts (SMAs)**: Each client has individual account
- **Tax Lots**: Track individual purchases with cost basis, purchase date, quantity
- **Position Tracking**: Real-time positions, cash balances, pending trades
- **Benchmark Assignment**: Each portfolio linked to benchmark (S&P 500, Russell 3000, custom)

#### 3.1.2 Data Requirements
- **Position Data**: 
  - Security identifier (CUSIP, ticker, ISIN)
  - Quantity held
  - Cost basis per tax lot
  - Purchase date
  - Current market value
- **Account Data**:
  - Account ID
  - Client information
  - Tax status (taxable vs. tax-advantaged)
  - Investment constraints (sector restrictions, ESG screens, etc.)
- **Transaction History**: All trades with timestamps, prices, quantities

#### 3.1.3 Database Schema (Inferred)
**Recommended**: PostgreSQL or similar relational database

**Tables:**
- `accounts`: Account metadata, client info, benchmark assignment
- `positions`: Current positions per account
- `tax_lots`: Individual tax lots with cost basis, purchase date
- `transactions`: Trade history
- `benchmarks`: Benchmark definitions and constituent weights
- `securities`: Security master data (ticker, CUSIP, sector, etc.)
- `market_data`: Price history, corporate actions
- `rebalancing_events`: Scheduled and executed rebalancing records

### 3.2 Tax-Loss Harvesting Engine

#### 3.2.1 Core Functionality
**Primary Objective**: Identify and realize tax losses while maintaining portfolio characteristics

**Key Processes:**

1. **Loss Identification**
   - Scan all tax lots for unrealized losses
   - Calculate potential tax benefit (loss × tax rate)
   - Prioritize losses by magnitude and tax benefit

2. **Wash Sale Detection**
   - **Wash Sale Rule**: Cannot claim loss if substantially identical security purchased 30 days before or after sale
   - Check for purchases/sales in 61-day window (30 days before + sale date + 30 days after)
   - Identify replacement securities that would violate wash sale rule
   - Track wash sale violations across all accounts (including related accounts)

3. **Replacement Security Selection**
   - Find similar but not "substantially identical" securities
   - Criteria:
     - Same sector/industry exposure
     - Similar risk characteristics (beta, volatility)
     - High correlation with sold security
     - Different enough to avoid wash sale (different CUSIP)
   - Examples:
     - Sell AAPL, buy MSFT (both tech, but different companies)
     - Sell SPY, buy VOO (both S&P 500 ETFs, but different issuers)
     - Sell individual stock, buy sector ETF

4. **Tax Benefit Calculation**
   - Short-term loss: Offset short-term gains (taxed at ordinary income rate)
   - Long-term loss: Offset long-term gains (taxed at lower rate)
   - Net loss carryforward: Track unused losses for future years

#### 3.2.2 Algorithm Flow

```
FOR each account:
  1. Get current positions and tax lots
  2. Calculate unrealized gains/losses for each tax lot
  3. Identify tax lots with losses (unrealized_loss < 0)
  4. FOR each losing tax lot:
     a. Check wash sale constraints (30-day window)
     b. Find replacement securities (similar exposure, different CUSIP)
     c. Calculate tax benefit: loss_amount × applicable_tax_rate
     d. Calculate tracking error impact of replacement
     e. Score opportunity: tax_benefit - tracking_error_cost
  5. Select top opportunities (maximize tax benefit, minimize tracking error)
  6. Generate trade recommendations
  7. Execute trades (if approved/automated)
```

#### 3.2.3 Tax Rules Implementation

**Wash Sale Detection:**
- Check transactions in window: [sale_date - 30 days, sale_date + 30 days]
- Check across all accounts (related accounts rule)
- Track replacement securities purchased within window

**Tax Lot Selection:**
- **FIFO (First-In-First-Out)**: Default for most accounts
- **Specific Identification**: Allow client to specify which lots to sell
- **Tax-Loss Harvesting Priority**: Prefer short-term losses (higher tax rate offset)

**Long-Term vs. Short-Term:**
- Short-term: Holding period < 1 year (taxed at ordinary income rate)
- Long-term: Holding period ≥ 1 year (taxed at lower capital gains rate)
- Prefer harvesting short-term losses (higher tax benefit)

### 3.3 Portfolio Optimization Engine

#### 3.3.1 Optimization Objective

**Primary Objective Function:**
```
Minimize: Tracking Error (vs. benchmark)
Subject to:
  - Tax-loss harvesting opportunities (maximize realized losses)
  - Risk constraints (sector exposure, factor exposure)
  - Transaction costs
  - Minimum position sizes
  - Turnover limits
```

#### 3.3.2 Optimization Formulation

**Mathematical Model:**

```
Minimize: ||w - w_benchmark||_2  (Tracking Error)
          + λ_transaction × TransactionCosts
          - λ_tax × TaxBenefit

Subject to:
  - Σ w_i = 1  (Fully invested)
  - w_i ≥ 0  (Long-only, no shorting)
  - |w_i - w_current| ≤ TurnoverLimit  (Turnover constraint)
  - |w_sector - w_benchmark_sector| ≤ SectorDeviationLimit
  - RiskModel(w) ≤ RiskLimit
  - TaxLossHarvesting(w, tax_lots) ≥ MinTaxBenefit
```

Where:
- `w`: Portfolio weights vector
- `w_benchmark`: Benchmark weights vector
- `λ_transaction`: Transaction cost penalty
- `λ_tax`: Tax benefit reward
- `RiskModel`: Risk model (e.g., Barra, Axioma, custom factor model)

#### 3.3.3 Optimization Solver

**Recommended Approach**: Quadratic Programming (QP) solver

**Options:**
1. **CVXPY** (Python): High-level convex optimization
2. **scipy.optimize**: Built-in Python optimization
3. **Gurobi**: Commercial solver (fastest, requires license)
4. **CPLEX**: IBM commercial solver
5. **OSQP**: Open-source QP solver

**Implementation:**
```python
# Pseudo-code
import cvxpy as cp

# Decision variables
w = cp.Variable(n_securities)  # Portfolio weights
trades = cp.Variable(n_securities)  # Trade amounts

# Objective
tracking_error = cp.norm(w - w_benchmark, 2)
transaction_costs = cp.sum(cp.abs(trades) * transaction_cost_rate)
tax_benefit = calculate_tax_benefit(trades, tax_lots)

objective = cp.Minimize(
    tracking_error 
    + lambda_transaction * transaction_costs
    - lambda_tax * tax_benefit
)

# Constraints
constraints = [
    cp.sum(w) == 1,  # Fully invested
    w >= 0,  # Long-only
    cp.abs(w - w_current) <= turnover_limit,  # Turnover
    risk_model_constraints(w),
    sector_exposure_constraints(w),
]

problem = cp.Problem(objective, constraints)
problem.solve()
```

#### 3.3.4 Risk Model

**Purpose**: Measure and control portfolio risk relative to benchmark

**Components:**
1. **Factor Model**: Multi-factor risk model (e.g., Barra, Axioma-style)
   - Factors: Market, Size, Value, Momentum, Quality, Low Volatility, etc.
   - Factor exposures: How much portfolio loads on each factor
   - Factor covariance: Risk from factor exposures

2. **Specific Risk**: Stock-specific risk (idiosyncratic)

3. **Risk Metrics**:
   - **Tracking Error**: Standard deviation of active returns
   - **Information Ratio**: Active return / Tracking Error
   - **Factor Exposure**: Portfolio factor loadings vs. benchmark

**Risk Model Providers** (Industry Standard):
- **Barra (MSCI Barra)**: Industry-standard multi-factor risk model
- **Axioma**: Alternative risk model provider
- **Northfield**: Risk analytics provider
- **Custom Factor Model**: Build proprietary factor model

**For Local Implementation:**
- Start with simple factor model (Fama-French factors)
- Use publicly available factor data (Kenneth French website)
- Build custom factor model using fundamental data

### 3.4 Rebalancing Engine

#### 3.4.1 Rebalancing Frequency

**Parametric's Approach** (Inferred):
- **Continuous Monitoring**: System monitors portfolios daily
- **Opportunistic Rebalancing**: Trade when tax-loss harvesting opportunities arise
- **Scheduled Rebalancing**: Monthly or quarterly systematic rebalancing
- **Event-Driven**: Rebalance on significant market moves or client deposits/withdrawals

**Recommended Implementation:**
- **Daily Monitoring**: Check for tax-loss harvesting opportunities daily
- **Monthly Rebalancing**: Systematic rebalancing to maintain tracking
- **Threshold-Based**: Rebalance when tracking error exceeds threshold (e.g., 50 bps)

#### 3.4.2 Rebalancing Triggers

1. **Tax-Loss Harvesting Opportunity**: Loss exceeds threshold (e.g., $1,000)
2. **Tracking Error Threshold**: Tracking error exceeds limit (e.g., 50-100 bps)
3. **Scheduled Rebalancing**: Monthly/quarterly systematic rebalancing
4. **Cash Flows**: Client deposits or withdrawals
5. **Corporate Actions**: Mergers, spinoffs, stock splits
6. **Benchmark Changes**: Benchmark reconstitution (e.g., S&P 500 additions/deletions)

#### 3.4.3 Rebalancing Process

```
1. Calculate current portfolio vs. target (benchmark)
2. Identify deviations:
   - Tracking error
   - Sector deviations
   - Factor exposure deviations
3. Identify tax-loss harvesting opportunities
4. Run optimization to:
   - Minimize tracking error
   - Maximize tax benefits
   - Respect constraints
5. Generate trade list
6. Check pre-trade compliance (wash sales, constraints)
7. Execute trades (or send for approval)
8. Update positions and tax lots
9. Generate trade confirmation and reporting
```

### 3.5 Data Management System

#### 3.5.1 Market Data Requirements

**Price Data:**
- Daily OHLCV (Open, High, Low, Close, Volume)
- Intraday data (for execution)
- Corporate actions (splits, dividends, mergers)
- **Frequency**: Daily updates, real-time for execution

**Data Providers** (Industry Options):
- **Bloomberg**: Comprehensive but expensive
- **Refinitiv (formerly Thomson Reuters)**: Alternative to Bloomberg
- **FactSet**: Portfolio analytics and data
- **Yahoo Finance API**: Free, limited (good for prototyping)
- **Alpha Vantage**: Free tier available
- **IEX Cloud**: Good for US equities
- **Polygon.io**: Real-time and historical market data

**For Local Implementation:**
- Start with free APIs (Yahoo Finance, Alpha Vantage, IEX Cloud)
- Use `yfinance` Python library
- Consider paid APIs for production (IEX Cloud, Polygon.io)

#### 3.5.2 Security Master Data

**Required Fields:**
- Ticker symbol
- CUSIP (Committee on Uniform Securities Identification Procedures)
- ISIN (International Securities Identification Number)
- Company name
- Sector classification (GICS, SIC)
- Industry classification
- Exchange listing
- Security type (common stock, ETF, etc.)

**Data Sources:**
- **SEC EDGAR**: Company filings
- **CRSP**: Center for Research in Security Prices (academic)
- **Compustat**: Financial data
- **Free Sources**: Yahoo Finance, company websites

#### 3.5.3 Benchmark Data

**Benchmark Constituents:**
- S&P 500: 500 large-cap US stocks
- Russell 3000: 3000 US stocks (broad market)
- Custom benchmarks: Client-defined

**Data Requirements:**
- Constituent list (updated quarterly for Russell, as-needed for S&P)
- Constituent weights (market-cap weighted)
- Total return index values

**Data Sources:**
- **S&P Dow Jones Indices**: Official S&P 500 data
- **FTSE Russell**: Russell index data
- **Free Sources**: Wikipedia, index provider websites

#### 3.5.4 Tax Data

**Tax Rates:**
- Federal capital gains rates (short-term, long-term)
- State tax rates (varies by state)
- Net Investment Income Tax (NIIT) rates
- Client-specific tax brackets

**Tax Rules:**
- Wash sale rules (30-day window)
- Long-term vs. short-term holding periods
- Tax loss carryforward rules
- Qualified dividend rates

**Implementation:**
- Hardcode tax rules (based on IRS regulations)
- Allow client-specific tax rate configuration
- Update annually for tax law changes

### 3.6 Client Portal / API

#### 3.6.1 Access Methods

**Parametric's Approach** (Inferred):
- **Wealth Manager Integration**: Parametric integrates with wealth management platforms (e.g., Merrill Lynch)
- **API Access**: Programmatic access for wealth managers
- **Client Portal**: Web-based interface for advisors/clients
- **Reporting**: Automated reports (monthly statements, tax reports)

#### 3.6.2 API Design (Recommended)

**RESTful API Endpoints:**

```
GET  /api/accounts                    # List accounts
GET  /api/accounts/{id}                # Get account details
GET  /api/accounts/{id}/positions      # Get current positions
GET  /api/accounts/{id}/tax-lots       # Get tax lot details
GET  /api/accounts/{id}/transactions   # Get transaction history
GET  /api/accounts/{id}/performance    # Get performance metrics

POST /api/accounts/{id}/rebalance      # Trigger rebalancing
POST /api/accounts/{id}/trades          # Submit trades
GET  /api/accounts/{id}/rebalance-preview  # Preview rebalancing

GET  /api/securities                   # Search securities
GET  /api/benchmarks/{id}/constituents # Get benchmark constituents

GET  /api/reports/{id}/tax-report      # Generate tax report
GET  /api/reports/{id}/performance-report  # Generate performance report
```

#### 3.6.3 User Interface (Stage 2 - Future)

**Technology Stack Options:**
- **React**: Frontend framework (recommended)
- **Node.js**: Backend API server
- **TypeScript**: Type-safe development
- **Material-UI / Ant Design**: UI component library

**Key Screens:**
1. **Dashboard**: Portfolio overview, performance, tax benefits
2. **Positions**: Current holdings, tax lots, unrealized gains/losses
3. **Tax-Loss Harvesting**: Available opportunities, executed harvests
4. **Rebalancing**: Rebalancing history, pending rebalances
5. **Reports**: Tax reports, performance reports, statements
6. **Settings**: Account configuration, constraints, preferences

---

## 4. Technical Implementation Details

### 4.1 Technology Stack (Recommended)

#### 4.1.1 Backend

**Language**: Python 3.10+

**Core Libraries:**
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **scipy**: Scientific computing, optimization
- **cvxpy**: Convex optimization
- **sqlalchemy**: Database ORM
- **fastapi**: Modern API framework (alternative: Flask)
- **pydantic**: Data validation

**Database:**
- **PostgreSQL**: Primary database (recommended)
- **SQLite**: For development/prototyping
- **Redis**: Caching layer (optional)

**Data Access:**
- **yfinance**: Yahoo Finance API wrapper
- **requests**: HTTP requests for APIs
- **pandas-datareader**: Alternative data source

#### 4.1.2 Optimization & Analytics

**Optimization:**
- **cvxpy**: Convex optimization (recommended)
- **scipy.optimize**: Alternative optimization methods
- **Gurobi/CPLEX**: Commercial solvers (for production scale)

**Risk Analytics:**
- **pyfolio**: Portfolio analytics (Zipline project)
- **empyrical**: Performance metrics
- **pandas**: Custom risk calculations

**Factor Models:**
- **statsmodels**: Statistical modeling
- **scikit-learn**: Machine learning (for factor construction)
- Custom implementation using fundamental data

#### 4.1.3 Infrastructure

**Development:**
- **Docker**: Containerization
- **docker-compose**: Multi-container orchestration
- **pytest**: Testing framework
- **black**: Code formatting
- **mypy**: Type checking

**Deployment** (Future):
- **AWS / GCP / Azure**: Cloud hosting
- **Kubernetes**: Container orchestration (for scale)
- **PostgreSQL**: Managed database service
- **Redis**: Managed cache service

### 4.2 Database Schema (Detailed)

#### 4.2.1 Core Tables

```sql
-- Accounts
CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    client_name VARCHAR(255) NOT NULL,
    account_type VARCHAR(50),  -- 'taxable', 'ira', '401k', etc.
    benchmark_id INTEGER REFERENCES benchmarks(benchmark_id),
    tax_rate_short_term DECIMAL(5,4),  -- e.g., 0.37 for 37%
    tax_rate_long_term DECIMAL(5,4),   -- e.g., 0.20 for 20%
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Securities Master
CREATE TABLE securities (
    security_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    cusip VARCHAR(9),
    isin VARCHAR(12),
    company_name VARCHAR(255),
    sector VARCHAR(50),
    industry VARCHAR(100),
    exchange VARCHAR(10),
    security_type VARCHAR(20),  -- 'stock', 'etf', 'bond', etc.
    created_at TIMESTAMP DEFAULT NOW()
);

-- Benchmarks
CREATE TABLE benchmarks (
    benchmark_id SERIAL PRIMARY KEY,
    benchmark_name VARCHAR(100) NOT NULL,  -- 'S&P 500', 'Russell 3000'
    benchmark_type VARCHAR(50),  -- 'index', 'custom'
    created_at TIMESTAMP DEFAULT NOW()
);

-- Benchmark Constituents (weights)
CREATE TABLE benchmark_constituents (
    benchmark_id INTEGER REFERENCES benchmarks(benchmark_id),
    security_id INTEGER REFERENCES securities(security_id),
    weight DECIMAL(10,8) NOT NULL,  -- e.g., 0.05 for 5%
    effective_date DATE NOT NULL,
    PRIMARY KEY (benchmark_id, security_id, effective_date)
);

-- Positions (current holdings)
CREATE TABLE positions (
    position_id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(account_id),
    security_id INTEGER REFERENCES securities(security_id),
    quantity DECIMAL(15,6) NOT NULL,
    market_value DECIMAL(15,2),
    last_updated TIMESTAMP DEFAULT NOW(),
    UNIQUE(account_id, security_id)
);

-- Tax Lots (individual purchases)
CREATE TABLE tax_lots (
    tax_lot_id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(account_id),
    security_id INTEGER REFERENCES securities(security_id),
    purchase_date DATE NOT NULL,
    purchase_price DECIMAL(10,4) NOT NULL,
    quantity DECIMAL(15,6) NOT NULL,
    cost_basis DECIMAL(15,2) NOT NULL,  -- purchase_price * quantity
    remaining_quantity DECIMAL(15,6) NOT NULL,  -- Updated when partially sold
    status VARCHAR(20) DEFAULT 'open',  -- 'open', 'closed', 'wash_sale'
    created_at TIMESTAMP DEFAULT NOW()
);

-- Transactions
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(account_id),
    security_id INTEGER REFERENCES securities(security_id),
    transaction_type VARCHAR(20) NOT NULL,  -- 'buy', 'sell', 'dividend', 'split'
    transaction_date TIMESTAMP NOT NULL,
    quantity DECIMAL(15,6),
    price DECIMAL(10,4),
    total_amount DECIMAL(15,2),
    tax_lot_id INTEGER REFERENCES tax_lots(tax_lot_id),  -- For sells
    realized_gain_loss DECIMAL(15,2),  -- For sells
    wash_sale_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Market Data (price history)
CREATE TABLE market_data (
    security_id INTEGER REFERENCES securities(security_id),
    date DATE NOT NULL,
    open_price DECIMAL(10,4),
    high_price DECIMAL(10,4),
    low_price DECIMAL(10,4),
    close_price DECIMAL(10,4) NOT NULL,
    volume BIGINT,
    adjusted_close DECIMAL(10,4),  -- Adjusted for splits/dividends
    PRIMARY KEY (security_id, date)
);

-- Rebalancing Events
CREATE TABLE rebalancing_events (
    rebalancing_id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(account_id),
    rebalancing_date DATE NOT NULL,
    rebalancing_type VARCHAR(50),  -- 'scheduled', 'tax_loss', 'threshold', 'manual'
    tracking_error_before DECIMAL(8,6),
    tracking_error_after DECIMAL(8,6),
    tax_benefit DECIMAL(15,2),
    status VARCHAR(20) DEFAULT 'pending',  -- 'pending', 'approved', 'executed', 'cancelled'
    created_at TIMESTAMP DEFAULT NOW()
);

-- Rebalancing Trades (proposed trades)
CREATE TABLE rebalancing_trades (
    trade_id SERIAL PRIMARY KEY,
    rebalancing_id INTEGER REFERENCES rebalancing_events(rebalancing_id),
    security_id INTEGER REFERENCES securities(security_id),
    trade_type VARCHAR(10) NOT NULL,  -- 'buy', 'sell'
    quantity DECIMAL(15,6) NOT NULL,
    price DECIMAL(10,4),
    tax_lot_id INTEGER REFERENCES tax_lots(tax_lot_id),  -- For sells
    estimated_tax_benefit DECIMAL(15,2),
    status VARCHAR(20) DEFAULT 'pending',
    executed_at TIMESTAMP
);
```

### 4.3 Core Algorithms

#### 4.3.1 Tax-Loss Harvesting Algorithm

```python
def identify_tax_loss_opportunities(account_id, min_loss_threshold=1000):
    """
    Identify tax-loss harvesting opportunities for an account.
    
    Returns:
        List of tax lot opportunities with replacement securities
    """
    # Get all open tax lots with unrealized losses
    tax_lots = get_tax_lots(account_id, status='open')
    current_prices = get_current_prices([lot.security_id for lot in tax_lots])
    
    opportunities = []
    
    for lot in tax_lots:
        current_price = current_prices[lot.security_id]
        unrealized_loss = (current_price - lot.purchase_price) * lot.remaining_quantity
        
        if unrealized_loss < -min_loss_threshold:
            # Check wash sale constraints
            if not violates_wash_sale(lot, account_id):
                # Find replacement securities
                replacements = find_replacement_securities(
                    lot.security_id,
                    exclude_securities=get_wash_sale_securities(lot, account_id)
                )
                
                for replacement in replacements:
                    # Calculate tax benefit
                    tax_rate = get_tax_rate(account_id, lot.purchase_date)
                    tax_benefit = abs(unrealized_loss) * tax_rate
                    
                    # Estimate tracking error impact
                    tracking_error_impact = estimate_tracking_error_impact(
                        lot.security_id,
                        replacement.security_id,
                        lot.remaining_quantity
                    )
                    
                    # Score opportunity
                    score = tax_benefit - tracking_error_impact
                    
                    opportunities.append({
                        'tax_lot': lot,
                        'replacement': replacement,
                        'unrealized_loss': unrealized_loss,
                        'tax_benefit': tax_benefit,
                        'tracking_error_impact': tracking_error_impact,
                        'score': score
                    })
    
    # Sort by score (highest tax benefit, lowest tracking error)
    opportunities.sort(key=lambda x: x['score'], reverse=True)
    
    return opportunities

def violates_wash_sale(tax_lot, account_id):
    """
    Check if selling this tax lot would violate wash sale rules.
    """
    sale_date = datetime.now().date()
    window_start = sale_date - timedelta(days=30)
    window_end = sale_date + timedelta(days=30)
    
    # Check for purchases/sales in 61-day window
    recent_transactions = get_transactions(
        account_id,
        start_date=window_start,
        end_date=window_end,
        security_id=tax_lot.security_id
    )
    
    # Check for substantially identical securities
    # (Same CUSIP or very similar securities)
    return len(recent_transactions) > 0
```

#### 4.3.2 Portfolio Optimization Algorithm

```python
import cvxpy as cp
import numpy as np

def optimize_portfolio(account_id, tax_loss_opportunities=None):
    """
    Optimize portfolio to minimize tracking error while maximizing tax benefits.
    """
    # Get current portfolio and benchmark
    current_weights = get_current_weights(account_id)
    benchmark_weights = get_benchmark_weights(account_id)
    n_securities = len(current_weights)
    
    # Decision variables
    w = cp.Variable(n_securities)  # Target weights
    trades = w - current_weights    # Trade amounts
    
    # Objective: Minimize tracking error + transaction costs - tax benefits
    tracking_error = cp.norm(w - benchmark_weights, 2)
    transaction_costs = cp.sum(cp.abs(trades) * TRANSACTION_COST_RATE)
    
    # Tax benefit (if tax-loss harvesting opportunities exist)
    tax_benefit = 0
    if tax_loss_opportunities:
        tax_benefit = calculate_tax_benefit_from_opportunities(
            trades,
            tax_loss_opportunities
        )
    
    objective = cp.Minimize(
        tracking_error 
        + LAMBDA_TRANSACTION * transaction_costs
        - LAMBDA_TAX * tax_benefit
    )
    
    # Constraints
    constraints = [
        cp.sum(w) == 1,                    # Fully invested
        w >= 0,                            # Long-only
        cp.abs(trades) <= TURNOVER_LIMIT,  # Turnover constraint
        # Sector constraints
        sector_exposure_constraints(w, benchmark_weights),
        # Risk constraints
        risk_model_constraints(w, benchmark_weights),
    ]
    
    # Solve
    problem = cp.Problem(objective, constraints)
    problem.solve(solver=cp.OSQP)
    
    if problem.status == 'optimal':
        return w.value, trades.value
    else:
        raise OptimizationError(f"Optimization failed: {problem.status}")
```

### 4.4 Rebalancing Workflow

```python
def rebalance_account(account_id, rebalancing_type='scheduled'):
    """
    Main rebalancing workflow for an account.
    """
    # 1. Calculate current tracking error
    current_te = calculate_tracking_error(account_id)
    
    # 2. Identify tax-loss harvesting opportunities
    tax_opportunities = identify_tax_loss_opportunities(account_id)
    
    # 3. Check if rebalancing is needed
    if current_te < TRACKING_ERROR_THRESHOLD and not tax_opportunities:
        return None  # No rebalancing needed
    
    # 4. Run optimization
    optimal_weights, trades = optimize_portfolio(
        account_id,
        tax_loss_opportunities=tax_opportunities
    )
    
    # 5. Generate trade list
    trade_list = generate_trade_list(account_id, trades, tax_opportunities)
    
    # 6. Pre-trade compliance checks
    compliance_checks = check_compliance(trade_list, account_id)
    if not compliance_checks['passed']:
        raise ComplianceError(compliance_checks['errors'])
    
    # 7. Create rebalancing event record
    rebalancing_event = create_rebalancing_event(
        account_id,
        rebalancing_type=rebalancing_type,
        trades=trade_list,
        tracking_error_before=current_te
    )
    
    # 8. Execute trades (or send for approval)
    if AUTO_EXECUTE:
        execute_trades(trade_list, account_id)
        update_positions(account_id, trade_list)
        update_tax_lots(account_id, trade_list)
    else:
        # Send for approval
        send_for_approval(rebalancing_event)
    
    return rebalancing_event
```

---

## 5. Implementation Phases

### Phase 1: Core Infrastructure (Weeks 1-4)
- [ ] Database schema design and implementation
- [ ] Security master data loading
- [ ] Market data ingestion pipeline
- [ ] Basic portfolio management (positions, tax lots)
- [ ] Account management system

### Phase 2: Tax-Loss Harvesting Engine (Weeks 5-8)
- [ ] Tax lot tracking system
- [ ] Wash sale detection algorithm
- [ ] Replacement security identification
- [ ] Tax benefit calculation
- [ ] Tax-loss harvesting opportunity identification

### Phase 3: Portfolio Optimization (Weeks 9-12)
- [ ] Benchmark data management
- [ ] Tracking error calculation
- [ ] Basic optimization engine (QP solver)
- [ ] Risk model implementation (simple factor model)
- [ ] Sector/exposure constraints

### Phase 4: Rebalancing Engine (Weeks 13-16)
- [ ] Rebalancing trigger logic
- [ ] Integration of tax-loss harvesting with optimization
- [ ] Trade generation and validation
- [ ] Pre-trade compliance checks
- [ ] Trade execution workflow

### Phase 5: API & Reporting (Weeks 17-20)
- [ ] RESTful API implementation
- [ ] Performance reporting
- [ ] Tax reporting (realized gains/losses)
- [ ] Client statements
- [ ] Dashboard endpoints

### Phase 6: Testing & Refinement (Weeks 21-24)
- [ ] Unit testing
- [ ] Integration testing
- [ ] Backtesting on historical data
- [ ] Performance optimization
- [ ] Documentation

### Phase 7: UI Development (Future - Stage 2)
- [ ] React frontend setup
- [ ] Dashboard implementation
- [ ] Position management UI
- [ ] Tax-loss harvesting interface
- [ ] Reporting interface

---

## 6. Key Metrics & Success Criteria

### 6.1 Performance Metrics
- **After-Tax Return**: Portfolio return after taxes
- **Tax Alpha**: Additional return from tax-loss harvesting (typically 0.5-1.5% annually)
- **Tracking Error**: Standard deviation of active returns (target: < 100 bps)
- **Information Ratio**: Active return / Tracking Error (target: > 0.5)

### 6.2 Operational Metrics
- **Rebalancing Frequency**: Number of rebalancing events per account per year
- **Tax-Loss Harvesting Rate**: Percentage of accounts with harvested losses
- **Average Tax Benefit**: Average tax savings per account per year
- **Execution Quality**: Trade execution vs. expected prices

### 6.3 System Metrics
- **API Response Time**: < 200ms for standard queries
- **Optimization Solve Time**: < 5 seconds per account
- **Data Update Latency**: Market data updated within 1 hour of market close
- **System Uptime**: > 99.9%

---

## 7. Risk Considerations

### 7.1 Tax Risks
- **Wash Sale Violations**: Incorrectly identifying wash sales could lead to IRS penalties
- **Tax Law Changes**: Changes in tax code require system updates
- **State Tax Complexity**: Different states have different tax rules

### 7.2 Operational Risks
- **Data Quality**: Incorrect market data or security master data
- **Execution Risk**: Trades not executed at expected prices
- **System Failures**: Downtime during market hours

### 7.3 Compliance Risks
- **Regulatory Compliance**: SEC, FINRA regulations for investment advisors
- **Client Suitability**: Ensuring strategies are suitable for clients
- **Reporting Requirements**: Accurate tax reporting and statements

---

## 8. Future Enhancements

### 8.1 Advanced Features
- **Multi-Account Optimization**: Optimize across related accounts
- **Charitable Giving Integration**: Donate appreciated securities
- **ESG Screening**: Environmental, Social, Governance constraints
- **Custom Factor Models**: Proprietary risk models
- **Options Overlay**: Use options for tax-efficient strategies

### 8.2 Scalability
- **Multi-Client Support**: Support thousands of accounts
- **Cloud Deployment**: AWS/GCP infrastructure
- **Real-Time Processing**: Real-time market data and execution
- **Distributed Computing**: Parallel optimization for multiple accounts

### 8.3 Integration
- **Custodian Integration**: Direct integration with custodians (e.g., Schwab, Fidelity)
- **Wealth Management Platform Integration**: Integration with advisor platforms
- **Tax Software Integration**: Export to tax preparation software

---

## 9. References & Resources

### 9.1 Academic Papers
- "Tax-Loss Harvesting and Wash Sales" - Academic research on tax-loss harvesting
- "Direct Indexing: The Next Generation of Portfolio Customization" - Industry white papers

### 9.2 Industry Resources
- **IRS Publication 550**: Investment Income and Expenses
- **IRS Publication 544**: Sales and Other Dispositions of Assets
- **Barra Risk Model Handbook**: Multi-factor risk models
- **Axioma Risk Model Documentation**: Alternative risk model approach

### 9.3 Technology Resources
- **CVXPY Documentation**: https://www.cvxpy.org/
- **PostgreSQL Documentation**: https://www.postgresql.org/docs/
- **FastAPI Documentation**: https://fastapi.tiangolo.com/
- **yfinance Documentation**: https://pypi.org/project/yfinance/

---

## 10. Appendix

### 10.1 Glossary

- **Direct Indexing**: Owning individual stocks that replicate an index, rather than an ETF
- **Tax-Loss Harvesting**: Selling securities at a loss to offset capital gains
- **Wash Sale**: IRS rule preventing claiming loss if substantially identical security purchased within 30 days
- **Separately Managed Account (SMA)**: Individual account managed by investment advisor
- **Tracking Error**: Standard deviation of difference between portfolio and benchmark returns
- **Tax Lot**: Individual purchase of a security with specific cost basis and purchase date
- **Rebalancing**: Adjusting portfolio weights to maintain target allocation

### 10.2 Example Scenarios

#### Scenario 1: Basic Tax-Loss Harvesting
- Client holds 100 shares of AAPL purchased at $150
- Current price: $140 (unrealized loss of $1,000)
- System identifies opportunity and recommends:
  - Sell 100 shares of AAPL (realize $1,000 loss)
  - Buy 100 shares of MSFT (similar tech exposure, different CUSIP)
  - Tax benefit: $1,000 × 20% (long-term rate) = $200 tax savings

#### Scenario 2: Monthly Rebalancing
- Portfolio tracking error exceeds 50 bps threshold
- System runs optimization to:
  - Minimize tracking error
  - Identify any tax-loss harvesting opportunities
  - Respect turnover and sector constraints
- Generates trade list and executes rebalancing

---

**Document Status**: Draft v1.0  
**Last Updated**: November 16, 2024  
**Next Review**: After Phase 1 completion

