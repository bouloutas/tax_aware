#!/usr/bin/env python3
"""
Test script for tax-loss harvesting functionality.

Usage:
    python scripts/test_tax_harvesting.py
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
from src.data.market_data import MarketDataManager
from src.data.security_master import SecurityMaster
from src.tax_harvesting import TaxLossHarvestingFinder


def main():
    """Test tax-loss harvesting functionality."""
    print("=" * 70)
    print("Testing Tax-Loss Harvesting Engine")
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
        aapl = security_master.get_or_create_security(
            ticker="AAPL", company_name="Apple Inc.", sector="Technology", industry="Consumer Electronics"
        )
        msft = security_master.get_or_create_security(
            ticker="MSFT", company_name="Microsoft Corporation", sector="Technology", industry="Software"
        )
        googl = security_master.get_or_create_security(
            ticker="GOOGL", company_name="Alphabet Inc.", sector="Technology", industry="Internet"
        )
        print(f"   ✓ Created securities: AAPL, MSFT, GOOGL")

        # Download market data
        print("\n3. Downloading market data (this may take a moment)...")
        market_data_mgr = MarketDataManager(session)
        for ticker in ["AAPL", "MSFT", "GOOGL"]:
            try:
                market_data_mgr.download_and_store_price_data(ticker, period="1mo")
                print(f"   ✓ Downloaded data for {ticker}")
            except Exception as e:
                print(f"   ⚠ Warning: Could not download data for {ticker}: {e}")

        # Create account
        print("\n4. Creating account...")
        account_mgr = AccountManager(session)
        account = account_mgr.create_account(
            client_name="Test Client",
            account_type="taxable",
            tax_rate_short_term=Decimal("0.37"),
            tax_rate_long_term=Decimal("0.20"),
        )
        print(f"   ✓ Created account: {account.account_id}")

        # Create positions with losses (purchased at higher prices)
        print("\n5. Creating tax lots with unrealized losses...")
        position_mgr = PositionManager(session)

        # Get current prices
        aapl_price = market_data_mgr.get_latest_price(aapl.security_id)
        msft_price = market_data_mgr.get_latest_price(msft.security_id)

        if aapl_price:
            # Create tax lot with purchase price higher than current (loss)
            purchase_price_aapl = aapl_price * Decimal("1.10")  # 10% higher = loss
            tax_lot_aapl = position_mgr.create_tax_lot(
                account_id=account.account_id,
                security_id=aapl.security_id,
                purchase_date=date(2024, 1, 15),
                purchase_price=purchase_price_aapl,
                quantity=Decimal("100"),
            )
            print(f"   ✓ Created AAPL tax lot: {tax_lot_aapl.tax_lot_id} (purchase: ${purchase_price_aapl}, current: ${aapl_price})")

        if msft_price:
            purchase_price_msft = msft_price * Decimal("1.15")  # 15% higher = loss
            tax_lot_msft = position_mgr.create_tax_lot(
                account_id=account.account_id,
                security_id=msft.security_id,
                purchase_date=date(2024, 2, 1),
                purchase_price=purchase_price_msft,
                quantity=Decimal("50"),
            )
            print(f"   ✓ Created MSFT tax lot: {tax_lot_msft.tax_lot_id} (purchase: ${purchase_price_msft}, current: ${msft_price})")

        # Find tax-loss harvesting opportunities
        print("\n6. Finding tax-loss harvesting opportunities...")
        finder = TaxLossHarvestingFinder(session)
        opportunities = finder.find_opportunities(
            account_id=account.account_id,
            min_loss_threshold=Decimal("100"),  # Lower threshold for testing
            max_opportunities=10,
        )

        if opportunities:
            print(f"\n   Found {len(opportunities)} opportunities:")
            print("\n   " + "-" * 65)
            for i, opp in enumerate(opportunities, 1):
                print(f"\n   Opportunity {i}:")
                print(f"   - Ticker: {opp.ticker}")
                print(f"   - Tax Lot ID: {opp.tax_lot_id}")
                print(f"   - Unrealized Loss: ${abs(opp.unrealized_loss):,.2f}")
                print(f"   - Tax Benefit: ${opp.tax_benefit:,.2f}")
                print(f"   - Wash Sale Violation: {'Yes' if opp.wash_sale_violation else 'No'}")
                print(f"   - Score: {opp.score:.2f}")
                if opp.replacement_securities:
                    print(f"   - Replacement Securities:")
                    for j, replacement in enumerate(opp.replacement_securities[:3], 1):
                        print(f"     {j}. {replacement['ticker']} ({replacement.get('company_name', 'N/A')})")
                        print(f"        Similarity: {replacement.get('similarity_score', 0):.2f}, "
                              f"Correlation: {replacement.get('correlation', 0):.2f}")
                else:
                    print(f"   - No replacement securities found")
        else:
            print("   ⚠ No tax-loss harvesting opportunities found")
            print("   (This may be because:")
            print("    - Current prices are higher than purchase prices")
            print("    - Losses are below the minimum threshold")
            print("    - Wash sale violations detected)")

        print("\n" + "=" * 70)
        print("✓ Tax-loss harvesting test completed!")
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

