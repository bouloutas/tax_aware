# Tax-Aware Portfolio Management System

A Parametric-style tax-aware portfolio management system with automated tax-loss harvesting and portfolio optimization.

## Overview

This system replicates the core functionality of Parametric Portfolio Associates' Direct Indexing platform:
- **Tax-Loss Harvesting**: Automated identification and realization of tax losses
- **Portfolio Optimization**: Minimize tracking error while maximizing tax benefits
- **Rebalancing**: Continuous monitoring and opportunistic rebalancing
- **Separately Managed Accounts**: Individual account management with tax lot tracking

## Current Status

**Phase 1: Core Infrastructure** - ✅ **COMPLETE**
- Database schema (SQLAlchemy models)
- Account management
- Security master data management
- Market data ingestion (yfinance)
- Position and tax lot management
- Benchmark data management

**Phase 2: Tax-Loss Harvesting Engine** - ✅ **COMPLETE**
- Wash sale detection (61-day window)
- Replacement security identification
- Tax benefit calculation
- Opportunity identification and scoring

**Phase 3: Portfolio Optimization** - ✅ **COMPLETE**
- Tracking error calculation
- Portfolio optimization engine (CVXPY)
- Risk model implementation
- Sector/exposure constraints

**Phase 4: Rebalancing Engine** - ✅ **COMPLETE**
- Rebalancing trigger logic
- Trade generation and validation
- Pre-trade compliance checks
- Trade execution workflow

**Phase 5: API & Reporting** - ✅ **COMPLETE**
- RESTful API implementation (FastAPI)
- Performance reporting endpoints
- Tax reporting endpoints
- Client statements
- Dashboard endpoints

## Project Structure

```
~/tax_aware/parametric/
├── src/
│   ├── api/              # REST API endpoints (Phase 5) 🔄
│   ├── core/             # Core business logic ✅
│   │   ├── database.py   # SQLAlchemy models
│   │   ├── config.py     # Configuration management
│   │   ├── account_manager.py
│   │   └── position_manager.py
│   ├── data/             # Data access layer ✅
│   │   ├── security_master.py
│   │   ├── market_data.py
│   │   └── benchmark_data.py
│   ├── optimization/     # Portfolio optimization ✅
│   │   ├── tracking_error.py
│   │   ├── risk_model.py
│   │   └── optimizer.py
│   ├── rebalancing/      # Rebalancing engine ✅
│   │   ├── rebalancer.py
│   │   ├── trade_generator.py
│   │   └── compliance.py
│   ├── tax_harvesting/   # Tax-loss harvesting ✅
│   │   ├── wash_sale.py
│   │   ├── replacement_security.py
│   │   ├── tax_benefit.py
│   │   └── opportunity_finder.py
│   └── utils/            # Utility functions
├── scripts/
│   ├── init_database.py  # Initialize database
│   ├── test_setup.py     # Test basic functionality
│   ├── test_tax_harvesting.py  # Test tax-loss harvesting
│   ├── test_optimization.py    # Test optimization
│   └── test_rebalancing.py     # Test rebalancing
├── tests/                # Unit and integration tests
├── docs/                 # Documentation
├── data/
│   ├── raw/              # Raw data files
│   └── processed/        # Processed data
├── PRD.md                # Product Requirements Document
├── implementation.md     # Implementation log
└── requirements.txt      # Python dependencies
```

## Setup

### 1. Environment Setup

```bash
# Activate conda environment
conda activate dgx-spark

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Setup

**Option A: SQLite (Development)**
```bash
# Set environment variable
export USE_SQLITE=true

# Initialize database
python scripts/init_database.py
```

**Option B: PostgreSQL (Production)**
```bash
# Install PostgreSQL (if not installed)
# sudo apt-get install postgresql postgresql-contrib

# Create database
createdb tax_aware_portfolio

# Set environment variable (or use .env file)
export DATABASE_URL=postgresql://user:password@localhost:5432/tax_aware_portfolio

# Initialize database
python scripts/init_database.py
```

### 3. Test Setup

```bash
# Run test scripts to verify installation
python scripts/test_setup.py
python scripts/test_tax_harvesting.py
python scripts/test_optimization.py
python scripts/test_rebalancing.py
```

## Usage Examples

### Create Account and Positions

```python
from src.core.config import Config
from src.core.database import create_database_engine, get_session_factory
from src.core.account_manager import AccountManager
from src.core.position_manager import PositionManager
from src.data.security_master import SecurityMaster
from datetime import date
from decimal import Decimal

# Initialize database session
engine = create_database_engine(Config.get_database_url())
Session = get_session_factory(engine)
session = Session()

# Create security
security_master = SecurityMaster(session)
aapl = security_master.get_or_create_security(
    ticker="AAPL",
    company_name="Apple Inc.",
    sector="Technology"
)

# Create account
account_mgr = AccountManager(session)
account = account_mgr.create_account(
    client_name="John Doe",
    account_type="taxable"
)

# Create tax lot (purchase)
position_mgr = PositionManager(session)
tax_lot = position_mgr.create_tax_lot(
    account_id=account.account_id,
    security_id=aapl.security_id,
    purchase_date=date(2024, 1, 15),
    purchase_price=Decimal("180.00"),
    quantity=Decimal("100")
)

session.close()
```

### Find Tax-Loss Harvesting Opportunities

```python
from src.tax_harvesting import TaxLossHarvestingFinder

# Find opportunities
finder = TaxLossHarvestingFinder(session)
opportunities = finder.find_opportunities(
    account_id=account.account_id,
    min_loss_threshold=Decimal("1000"),
    max_opportunities=10
)

# Process opportunities
for opp in opportunities:
    print(f"Ticker: {opp.ticker}")
    print(f"Unrealized Loss: ${abs(opp.unrealized_loss):,.2f}")
    print(f"Tax Benefit: ${opp.tax_benefit:,.2f}")
    print(f"Wash Sale Violation: {opp.wash_sale_violation}")
    print(f"Replacement Securities: {len(opp.replacement_securities)}")
    print()
```

### Optimize Portfolio

```python
from src.optimization import PortfolioOptimizer

# Optimize portfolio
optimizer = PortfolioOptimizer(session)
result = optimizer.optimize_with_tax_harvesting(
    account_id=account.account_id,
    tax_loss_opportunities=opportunities,
    max_tracking_error=0.01  # 100 bps
)

print(f"Tracking Error: {result['tracking_error']:.4f}")
print(f"Tax Benefit: ${result['tax_benefit']:,.2f}")
```

### Rebalance Portfolio

```python
from src.rebalancing import Rebalancer

# Rebalance account
rebalancer = Rebalancer(session)
result = rebalancer.rebalance_account(
    account_id=account.account_id,
    rebalancing_type="threshold",
    auto_execute=False  # Set to True to auto-execute trades
)

print(f"Status: {result['status']}")
print(f"Trades Generated: {len(result['trades'])}")
print(f"Tax Benefit: ${result['tax_benefit']:,.2f}")
```


### Run API Server

```bash
# Start the API server
python scripts/run_api.py

# Or use uvicorn directly
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at:
- **API**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **OpenAPI Schema**: http://localhost:8000/openapi.json

### API Usage Examples

```python
import requests

# Create account
response = requests.post("http://localhost:8000/api/accounts/", json={
    "client_name": "John Doe",
    "account_type": "taxable",
    "tax_rate_short_term": 0.37,
    "tax_rate_long_term": 0.20
})
account = response.json()

# Get tax-loss harvesting opportunities
response = requests.get(f"http://localhost:8000/api/tax-harvesting/opportunities/{account['account_id']}")
opportunities = response.json()

# Optimize portfolio
response = requests.post("http://localhost:8000/api/optimization/optimize", json={
    "account_id": account["account_id"],
    "include_tax_harvesting": True,
    "max_tracking_error": 0.01
})
optimization_result = response.json()

# Rebalance account
response = requests.post("http://localhost:8000/api/rebalancing/rebalance", json={
    "account_id": account["account_id"],
    "rebalancing_type": "threshold",
    "auto_execute": False
})
rebalance_result = response.json()

# Get performance report
response = requests.get(f"http://localhost:8000/api/reporting/performance/{account['account_id']}")
performance = response.json()
```

## Key Features

### Tax-Loss Harvesting
- **Wash Sale Detection**: Automatically detects IRS wash sale violations (61-day window)
- **Replacement Securities**: Finds suitable replacements maintaining portfolio characteristics
- **Tax Benefit Calculation**: Calculates tax savings from realized losses
- **Opportunity Scoring**: Ranks opportunities by tax benefit and feasibility

### Portfolio Optimization
- **Tracking Error Minimization**: Optimizes portfolio to minimize tracking error vs. benchmark
- **Tax-Aware Optimization**: Incorporates tax-loss harvesting opportunities
- **Risk Constraints**: Sector exposure limits, turnover constraints
- **Multiple Solvers**: OSQP, ECOS, SCS support

### Rebalancing
- **Multiple Triggers**: Threshold-based, tax-loss, scheduled, manual
- **Compliance Checking**: Pre-trade validation (wash sales, quantities, etc.)
- **Trade Generation**: Converts optimization results to executable trades
- **Automatic Execution**: Optional auto-execution with compliance checks

### Portfolio Management
- **Tax Lot Tracking**: Individual cost basis tracking for each purchase
- **Position Management**: Automatic position updates on trades
- **Transaction History**: Complete audit trail of all transactions
- **Realized Gain/Loss**: Automatic calculation on sales

## Statistics

- **Total Python Files**: 30
- **Phase 1 (Core)**: 9 files
- **Phase 2 (Tax Harvesting)**: 5 files
- **Phase 3 (Optimization)**: 4 files
- **Phase 4 (Rebalancing)**: 4 files
- **Scripts**: 5 files
- **Total Lines of Code**: ~3,500+ lines

## Next Steps

See `implementation.md` for detailed progress and next steps.

## Documentation

- **PRD.md**: Complete Product Requirements Document
- **implementation.md**: Detailed implementation log with progress tracking

## License

Internal use only.
