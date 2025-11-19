#!/usr/bin/env python3
"""
Test script to verify database setup and basic operations.
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

def main():
    print("=" * 60)
    print("Testing Tax-Aware Portfolio Management System Setup")
    print("=" * 60)
    print("\n1. Initializing database...")
    db_url = Config.get_database_url()
    print(f"   Database URL: {db_url}")
    engine = create_database_engine(db_url, echo=False)
    init_database(engine)
    print("   ✓ Database initialized")
    Session = get_session_factory(engine)
    session = Session()
    try:
        print("\n2. Testing Security Master...")
        security_master = SecurityMaster(session)
        aapl = security_master.get_or_create_security(
            ticker="AAPL", company_name="Apple Inc.", sector="Technology")
        print(f"   ✓ Created security: {aapl.ticker}")
        print("\n3. Testing Account Manager...")
        account_mgr = AccountManager(session)
        account = account_mgr.create_account(client_name="Test Client", account_type="taxable")
        print(f"   ✓ Created account: {account.account_id}")
        print("\n4. Testing Position Manager...")
        position_mgr = PositionManager(session)
        tax_lot = position_mgr.create_tax_lot(
            account_id=account.account_id, security_id=aapl.security_id,
            purchase_date=date(2024, 1, 15), purchase_price=Decimal("180.00"),
            quantity=Decimal("100"))
        print(f"   ✓ Created tax lot: {tax_lot.tax_lot_id}")
        print("\n" + "=" * 60)
        print("✓ All tests passed! System is ready for use.")
        print("=" * 60)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main()
