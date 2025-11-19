# Test Setup Guide
## Inputs and Prerequisites for Running Comprehensive Tests

**Last Updated**: November 16, 2024

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Required Inputs](#required-inputs)
3. [Optional Inputs](#optional-inputs)
4. [Setup Steps](#setup-steps)
5. [Test Data Requirements](#test-data-requirements)
6. [Running Tests](#running-tests)

---

## Prerequisites

### 1. Environment Setup

```bash
# Activate conda environment
conda activate dgx-spark

# Verify all packages installed
python scripts/verify_environment.py
```

**Required**: All packages from `requirements.txt` must be installed (see previous verification).

### 2. Database Configuration

**Option A: SQLite (Recommended for Testing)**
- No setup required - tests create temporary SQLite databases
- Automatically cleaned up after tests

**Option B: PostgreSQL (For Integration Tests)**
- PostgreSQL server running
- Database created: `tax_aware_portfolio_test`
- Connection string in environment or `.env` file

---

## Required Inputs

### 1. Internet Connection

**Why**: Tests download market data from yfinance API

**What's Downloaded**:
- Historical price data for test securities (AAPL, MSFT, GOOGL, AMZN, TSLA)
- Period: Last 3 months (configurable)
- Data includes: OHLCV (Open, High, Low, Close, Volume)

**Note**: If internet is unavailable, tests will skip market data-dependent tests or use mock data.

---

### 2. Test Securities (Automatic)

**No manual input required** - Tests automatically create:

| Ticker | Company Name | Sector | Industry |
|--------|--------------|--------|----------|
| AAPL | Apple Inc. | Technology | Consumer Electronics |
| MSFT | Microsoft Corporation | Technology | Software |
| GOOGL | Alphabet Inc. | Technology | Internet |
| AMZN | Amazon.com Inc. | Consumer Discretionary | E-Commerce |
| TSLA | Tesla Inc. | Consumer Discretionary | Automotive |

**How it works**: Test fixtures automatically create these securities in the test database.

---

### 3. Test Account Configuration

**No manual input required** - Tests automatically create test accounts with:

```python
{
    "client_name": "Test Client",
    "account_type": "taxable",
    "tax_rate_short_term": 0.37,  # 37%
    "tax_rate_long_term": 0.20,   # 20%
    "benchmark_id": <auto-assigned>
}
```

---

## Optional Inputs

### 1. Barra Risk Model Data

**Status**: ✅ **AVAILABLE** - Imported from ~/tax_aware/barra/

**Location**: `data/raw/barra/`
- Release Date: 2025-09-30
- Files: factor_covariance, factor_returns, style_exposures, specific_risk
- GVKEY-Ticker Mapping: Available (2,388 mappings)

**Usage**: 
- Risk model automatically uses Barra data if available
- Tests can use Barra factor exposures for more realistic risk calculations
- See `data/raw/barra/INTEGRATION.md` for details

**To Refresh**: Run `python scripts/import_barra_data.py`

---

### 2. Historical Benchmark Data

**Status**: Optional - Tests use simple equal-weight benchmark

**If Available**:
- S&P 500 constituents as of 9/30/2024
- Constituent weights
- Total return index values

**Location**: `data/raw/benchmarks/` (create if needed)

---

### 3. Custom Test Data

**Status**: Optional - For advanced testing scenarios

**If Needed**:
- Custom security lists
- Historical transaction data
- Pre-configured portfolios

**Location**: `data/raw/test_data/` (create if needed)

---

## Setup Steps

### Step 1: Verify Environment

```bash
cd /home/tasos/tax_aware/parametric
conda activate dgx-spark
python scripts/verify_environment.py
```

**Expected Output**: `✅ All packages installed!`

---

### Step 2: Check Internet Connection (for Market Data)

```bash
# Test yfinance connectivity
python -c "import yfinance as yf; print(yf.Ticker('AAPL').history(period='5d').shape)"
```

**Expected Output**: Should show DataFrame shape (e.g., `(5, 6)`)

**If No Internet**: Tests will skip market data tests or use mocks.

---

### Step 3: Run Basic Setup Test

```bash
# Test database initialization
python scripts/test_setup.py
```

**Expected Output**: 
```
✓ Database initialized
✓ Created security: AAPL
✓ Created account: 1
...
✓ All tests passed!
```

---

### Step 4: Run Comprehensive Tests

```bash
# Run all tests
pytest tests/test_comprehensive.py -v

# Run specific test class
pytest tests/test_comprehensive.py::TestAccountCreation -v

# Run with coverage
pytest tests/test_comprehensive.py --cov=src --cov-report=html
```

---

## Test Data Requirements

### Automatic Test Data Generation

The test suite automatically generates:

1. **Database Schema**: Created fresh for each test
2. **Test Securities**: 5 securities (AAPL, MSFT, GOOGL, AMZN, TSLA)
3. **Market Data**: Downloaded from yfinance (if available)
4. **Test Accounts**: Created with default tax rates
5. **Test Positions**: Created with various weights/scenarios
6. **Test Tax Lots**: Created with different purchase dates/prices

### Test Scenarios Covered

**Simple Tests** (No external data needed):
- Database schema creation
- Account creation
- Security master data
- Tax lot creation

**Market Data Tests** (Requires internet):
- Market data download
- Price history retrieval
- Current price lookups

**Optimization Tests** (Requires market data):
- Tracking error calculation
- Portfolio optimization
- Risk model calculations

**Integration Tests** (Requires market data):
- Complete tax-loss harvesting workflow
- Complete rebalancing workflow

---

## Running Tests

### Basic Test Run

```bash
# Run all tests
pytest tests/test_comprehensive.py -v

# Run with output
pytest tests/test_comprehensive.py -v -s

# Run specific phase
pytest tests/test_comprehensive.py::TestDatabaseSchema -v
pytest tests/test_comprehensive.py::TestTaxLossHarvestingOpportunities -v
```

### Test with Coverage

```bash
# Generate coverage report
pytest tests/test_comprehensive.py --cov=src --cov-report=html

# View coverage report
# Open: htmlcov/index.html in browser
```

### Test Specific Components

```bash
# Phase 1: Core Infrastructure
pytest tests/test_comprehensive.py::TestDatabaseSchema -v
pytest tests/test_comprehensive.py::TestAccountCreation -v
pytest tests/test_comprehensive.py::TestSecurityMaster -v
pytest tests/test_comprehensive.py::TestMarketData -v
pytest tests/test_comprehensive.py::TestTaxLotCreation -v

# Phase 2: Tax-Loss Harvesting
pytest tests/test_comprehensive.py::TestWashSaleDetection -v
pytest tests/test_comprehensive.py::TestReplacementSecurity -v
pytest tests/test_comprehensive.py::TestTaxBenefitCalculation -v
pytest tests/test_comprehensive.py::TestTaxLossHarvestingOpportunities -v

# Phase 3: Portfolio Optimization
pytest tests/test_comprehensive.py::TestTrackingError -v
pytest tests/test_comprehensive.py::TestPortfolioOptimization -v

# Phase 4: Rebalancing
pytest tests/test_comprehensive.py::TestRebalancingCheck -v
pytest tests/test_comprehensive.py::TestCompliance -v

# Integration Tests
pytest tests/test_comprehensive.py::TestTaxLossHarvestingWorkflow -v
pytest tests/test_comprehensive.py::TestRebalancingWorkflow -v

# End-to-End Tests
pytest tests/test_comprehensive.py::TestE2ENewAccountSetup -v
```

---

## Minimal Inputs to Start Testing

### Absolute Minimum (Tests Will Run):

1. ✅ **Conda environment activated**: `conda activate dgx-spark`
2. ✅ **All packages installed**: Verified by `verify_environment.py`
3. ✅ **Python 3.10+**: Already available in dgx-spark

**Result**: Basic tests (database, accounts, securities) will run successfully.

---

### Recommended (For Full Test Coverage):

1. ✅ **Internet connection**: For market data downloads
2. ✅ **yfinance API access**: Free, no API key needed
3. ⚠️ **Barra data** (optional): For advanced risk model tests

**Result**: All tests including market data, optimization, and integration tests will run.

---

## Test Execution Flow

```
1. Test Fixture Setup
   ├── Create temporary database (SQLite)
   ├── Initialize schema
   ├── Create test securities (AAPL, MSFT, etc.)
   └── Create test account

2. Market Data Download (if internet available)
   ├── Download price history for test securities
   └── Store in test database

3. Test Execution
   ├── Create test positions
   ├── Run test scenarios
   └── Validate outputs

4. Cleanup
   ├── Close database connections
   └── Delete temporary database file
```

---

## Troubleshooting

### Issue: Tests fail with "No module named 'src'"

**Solution**:
```bash
cd /home/tasos/tax_aware/parametric
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pytest tests/test_comprehensive.py -v
```

---

### Issue: Market data download fails

**Solution**: Tests will skip market data tests or use mocks. To test without internet:
```bash
# Set environment variable to use mock data
export USE_MOCK_MARKET_DATA=true
pytest tests/test_comprehensive.py -v
```

---

### Issue: Database connection errors

**Solution**: Tests use SQLite by default (no setup needed). If using PostgreSQL:
```bash
# Set database URL
export DATABASE_URL=postgresql://user:pass@localhost:5432/tax_aware_portfolio_test
pytest tests/test_comprehensive.py -v
```

---

### Issue: Import errors

**Solution**: Verify environment:
```bash
conda activate dgx-spark
python scripts/verify_environment.py
pip install -r requirements.txt
```

---

## Quick Start Checklist

- [ ] Conda environment `dgx-spark` activated
- [ ] All packages installed (`verify_environment.py` passes)
- [ ] Internet connection available (for market data tests)
- [ ] In project directory: `/home/tasos/tax_aware/parametric`
- [ ] Run: `pytest tests/test_comprehensive.py -v`

---

## Expected Test Output

```
tests/test_comprehensive.py::TestDatabaseSchema::test_all_tables_exist PASSED
tests/test_comprehensive.py::TestAccountCreation::test_create_account PASSED
tests/test_comprehensive.py::TestSecurityMaster::test_create_security PASSED
...
================================ test session starts ================================
platform linux -- Python 3.10.19, pytest-8.4.2
collected 20+ items

tests/test_comprehensive.py::TestDatabaseSchema::test_all_tables_exist PASSED [  5%]
tests/test_comprehensive.py::TestAccountCreation::test_create_account PASSED [ 10%]
...
============================= 20+ passed in XX.XXs ==============================
```

---

## Summary

**Minimum Required Inputs**:
- ✅ Conda environment with packages
- ✅ Python 3.10+
- ✅ Project directory access

**Recommended for Full Testing**:
- ✅ Internet connection (for market data)
- ⚠️ Barra data (optional, for advanced tests)

**No Manual Data Entry Required**: All test data is automatically generated by test fixtures.

---

**Ready to Test?** Run:
```bash
cd /home/tasos/tax_aware/parametric
conda activate dgx-spark
pytest tests/test_comprehensive.py -v
```

