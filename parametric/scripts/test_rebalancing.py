#!/usr/bin/env python3
"""
Test script for rebalancing engine functionality.

Usage:
    python scripts/test_rebalancing.py
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
from src.rebalancing import Rebalancer


def main():
    """Test rebalancing engine functionality."""
    print("=" * 70)
    print("Testing Rebalancing Engine")
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
        tickers = ["AAPL", "MSFT", "GOOGL"]
        securities = {}
        for ticker in tickers:
            sec = security_master.get_or_create_security(
                ticker=ticker, company_name=f"{ticker} Inc.", sector="Technology"
            )
            securities[ticker] = sec
            print(f"   ✓ Created {ticker}")

        # Download market data
        print("\n3. Downloading market data...")
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
        weight = Decimal("0.333")  # Equal weight
        for ticker in tickers:
            benchmark_mgr.add_constituent(
                benchmark_id=benchmark.benchmark_id,
                security_id=securities[ticker].security_id,
                weight=weight,
                effective_date=date.today(),
            )
        print(f"   ✓ Created benchmark")

        # Create account
        print("\n5. Creating account...")
        account_mgr = AccountManager(session)
        account = account_mgr.create_account(
            client_name="Test Client",
            account_type="taxable",
            benchmark_id=benchmark.benchmark_id,
        )
        print(f"   ✓ Created account: {account.account_id}")

        # Create positions (different from benchmark to create tracking error)
        print("\n6. Creating positions...")
        position_mgr = PositionManager(session)
        prices = {}
        for ticker in tickers:
            price = market_data_mgr.get_latest_price(securities[ticker].security_id)
            if price:
                prices[ticker] = price

        if prices:
            # Create positions with different weights
            position_weights = {"AAPL": 0.50, "MSFT": 0.30, "GOOGL": 0.20}
            total_value = Decimal("100000")

            for ticker, weight in position_weights.items():
                if ticker in prices:
                    price = prices[ticker]
                    quantity = (total_value * Decimal(str(weight))) / price
                    tax_lot = position_mgr.create_tax_lot(
                        account_id=account.account_id,
                        security_id=securities[ticker].security_id,
                        purchase_date=date(2024, 1, 1),
                        purchase_price=price * Decimal("0.90"),  # Purchased at 90% = loss
                        quantity=quantity,
                    )
                    print(f"   ✓ Created position: {ticker} ({weight:.0%} weight)")

        # Check if rebalancing is needed
        print("\n7. Checking if rebalancing is needed...")
        rebalancer = Rebalancer(session)
        check_result = rebalancer.check_rebalancing_needed(account.account_id, rebalancing_type="threshold")
        print(f"   Rebalancing needed: {check_result['rebalancing_needed']}")
        print(f"   Reason: {check_result['reason']}")
        print(f"   Current tracking error: {check_result['current_tracking_error']:.4f}")
        print(f"   Tax opportunities: {check_result['tax_opportunities']}")

        # Perform rebalancing
        if check_result["rebalancing_needed"]:
            print("\n8. Performing rebalancing...")
            rebalance_result = rebalancer.rebalance_account(
                account_id=account.account_id,
                rebalancing_type="threshold",
                auto_execute=False,  # Don't auto-execute for testing
            )

            print(f"   Status: {rebalance_result['status']}")
            print(f"   Rebalancing Event ID: {rebalance_result['rebalancing_event_id']}")
            print(f"   Tracking Error Before: {rebalance_result['tracking_error_before']:.4f}")
            print(f"   Tracking Error After: {rebalance_result['tracking_error_after']:.4f}")
            print(f"   Tax Benefit: ${rebalance_result['tax_benefit']:,.2f}")
            print(f"   Trades Generated: {len(rebalance_result['trades'])}")
            print(f"   Message: {rebalance_result['message']}")

            if rebalance_result["trades"]:
                print("\n   Sample Trades:")
                for i, trade in enumerate(rebalance_result["trades"][:5], 1):
                    if isinstance(trade, dict):
                        print(f"     {i}. {trade.get('trade_type', 'N/A')} - Security {trade.get('security_id', 'N/A')}")
                    else:
                        print(f"     {i}. {trade}")

        print("\n" + "=" * 70)
        print("✓ Rebalancing engine test completed!")
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

