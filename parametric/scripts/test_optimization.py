#!/usr/bin/env python3
"""
Test script for portfolio optimization functionality.

Usage:
    python scripts/test_optimization.py
"""
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.core.database import create_database_engine, get_session_factory, init_database
from src.core.account_manager import AccountManager
from src.core.position_manager import PositionManager
from src.data.benchmark_data import BenchmarkManager
from src.data.market_data import MarketDataManager
from src.data.security_master import SecurityMaster
from src.optimization import PortfolioOptimizer, TrackingErrorCalculator
from src.tax_harvesting import TaxLossHarvestingFinder


def main():
    """Test portfolio optimization functionality."""
    print("=" * 70)
    print("Testing Portfolio Optimization Engine")
    print("=" * 70)

    # Initialize database
    print("\n1. Initializing database...")
    db_url = Config.get_database_url()
    print(f"   Database URL: {db_url}")

    engine = create_database_engine(db_url, echo=False)
    init_database(engine)
    print("   ✓ Database initialized")

    Session = get_session_factory(engine)
    session = Session()

    try:
        # Create securities
        print("\n2. Creating securities...")
        security_master = SecurityMaster(session)
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        securities = {}
        for ticker in tickers:
            sec = security_master.get_or_create_security(
                ticker=ticker, company_name=f"{ticker} Inc.", sector="Technology"
            )
            securities[ticker] = sec
            print(f"   ✓ Created {ticker}")

        # Download market data
        print("\n3. Downloading market data (this may take a moment)...")
        market_data_mgr = MarketDataManager(session)
        for ticker in tickers:
            try:
                market_data_mgr.download_and_store_price_data(ticker, period="3mo")
                print(f"   ✓ Downloaded data for {ticker}")
            except Exception as e:
                print(f"   ⚠ Warning: Could not download data for {ticker}: {e}")

        # Create benchmark
        print("\n4. Creating benchmark...")
        benchmark_mgr = BenchmarkManager(session)
        benchmark = benchmark_mgr.create_benchmark("Test Index", benchmark_type="custom")

        # Add benchmark constituents (equal weight for simplicity)
        weight = Decimal("0.20")  # 20% each
        for ticker in tickers:
            benchmark_mgr.add_constituent(
                benchmark_id=benchmark.benchmark_id,
                security_id=securities[ticker].security_id,
                weight=weight,
                effective_date=date.today(),
            )
        print(f"   ✓ Created benchmark with {len(tickers)} constituents")

        # Create account
        print("\n5. Creating account...")
        account_mgr = AccountManager(session)
        account = account_mgr.create_account(
            client_name="Test Client",
            account_type="taxable",
            benchmark_id=benchmark.benchmark_id,
        )
        print(f"   ✓ Created account: {account.account_id}")

        # Create positions (not equal weight to create tracking error)
        print("\n6. Creating positions...")
        position_mgr = PositionManager(session)

        # Get current prices
        prices = {}
        for ticker in tickers:
            price = market_data_mgr.get_latest_price(securities[ticker].security_id)
            if price:
                prices[ticker] = price

        if prices:
            # Create positions with different weights than benchmark
            position_weights = {"AAPL": 0.30, "MSFT": 0.25, "GOOGL": 0.20, "AMZN": 0.15, "TSLA": 0.10}
            total_value = Decimal("100000")  # $100k portfolio

            for ticker, weight in position_weights.items():
                if ticker in prices:
                    price = prices[ticker]
                    quantity = (total_value * Decimal(str(weight))) / price
                    tax_lot = position_mgr.create_tax_lot(
                        account_id=account.account_id,
                        security_id=securities[ticker].security_id,
                        purchase_date=date(2024, 1, 1),
                        purchase_price=price * Decimal("0.95"),  # Purchased at 95% of current (small gain)
                        quantity=quantity,
                    )
                    print(f"   ✓ Created position: {ticker} ({weight:.0%} weight)")

        # Calculate tracking error
        print("\n7. Calculating tracking error...")
        tracking_calc = TrackingErrorCalculator(session)
        te_result = tracking_calc.calculate_tracking_error(account.account_id, lookback_days=60)
        print(f"   ✓ Tracking Error: {te_result['tracking_error']:.4f} ({te_result['tracking_error']*10000:.2f} bps)")
        print(f"   ✓ Portfolio Return: {te_result['portfolio_return']:.2%}")
        print(f"   ✓ Benchmark Return: {te_result['benchmark_return']:.2%}")
        print(f"   ✓ Information Ratio: {te_result['information_ratio']:.2f}")

        # Find tax-loss harvesting opportunities
        print("\n8. Finding tax-loss harvesting opportunities...")
        finder = TaxLossHarvestingFinder(session)
        opportunities = finder.find_opportunities(
            account_id=account.account_id,
            min_loss_threshold=Decimal("100"),
            max_opportunities=5,
        )
        print(f"   ✓ Found {len(opportunities)} opportunities")

        # Optimize portfolio
        print("\n9. Optimizing portfolio...")
        optimizer = PortfolioOptimizer(session)
        opt_result = optimizer.optimize_with_tax_harvesting(
            account_id=account.account_id,
            tax_loss_opportunities=opportunities,
            max_tracking_error=0.01,  # 100 bps max tracking error
        )

        print(f"   ✓ Optimization Status: {opt_result['status']}")
        print(f"   ✓ Optimal Tracking Error: {opt_result['tracking_error']:.4f}")
        print(f"   ✓ Tax Benefit: ${opt_result['tax_benefit']:,.2f}")

        if not opt_result["optimal_weights"].empty:
            print("\n   Optimal Weights:")
            for security_id, weight in opt_result["optimal_weights"].items():
                security = session.query(securities["AAPL"].__class__).filter(
                    securities["AAPL"].__class__.security_id == security_id
                ).first()
                ticker = security.ticker if security else f"SEC_{security_id}"
                print(f"     {ticker}: {weight:.2%}")

        if opt_result.get("tax_harvest_trades"):
            print("\n   Tax-Loss Harvesting Trades:")
            for trade in opt_result["tax_harvest_trades"]:
                print(f"     Sell {trade['sell_ticker']} → Buy {trade['buy_ticker']}")
                print(f"       Tax Benefit: ${trade['tax_benefit']:,.2f}")

        print("\n" + "=" * 70)
        print("✓ Portfolio optimization test completed!")
        print("=" * 70)

    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        import traceback

        traceback.print_exc()
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()

