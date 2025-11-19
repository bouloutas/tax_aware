#!/usr/bin/env python3
from datetime import date, timedelta
from decimal import Decimal

from src.core.database import create_database_engine, get_session_factory
from src.core.config import Config
from src.data.market_data import MarketDataManager
from src.data.benchmark_data import BenchmarkManager
from src.optimization.tracking_error import TrackingErrorCalculator

AS_OF = date(2024, 9, 30)
LOOKBACK_DAYS = 60


def main():
    engine = create_database_engine(Config.get_database_url(), echo=False)
    Session = get_session_factory(engine)
    session = Session()

    mkt = MarketDataManager(session)
    bench = BenchmarkManager(session)
    te_calc = TrackingErrorCalculator(session)

    # Ensure SPY exists and has recent prices
    from src.core.database import Security, Account
    spy = session.query(Security).filter(Security.ticker == 'SPY').first()
    if not spy:
        spy = Security(ticker='SPY', company_name='SPDR S&P 500 ETF Trust')
        session.add(spy)
        session.commit()
        session.refresh(spy)

    # Download around AS_OF
    mkt.download_and_store_price_data('SPY', start_date=AS_OF - timedelta(days=120), end_date=AS_OF + timedelta(days=7))

    # Create/update benchmark using SPY = 100%
    b = bench.get_benchmark_by_name('SP500_SPY')
    if not b:
        b = bench.create_benchmark(benchmark_name='SP500_SPY')
    bench.add_constituent(b.benchmark_id, spy.security_id, Decimal('1.0'), AS_OF)

    # Assign to the first account
    account = session.query(Account).first()
    if account and account.benchmark_id != b.benchmark_id:
        from src.core.account_manager import AccountManager
        AccountManager(session).update_account(account.account_id, benchmark_id=b.benchmark_id)

    # Compute TE vs SPY
    end_date = AS_OF
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    result = te_calc.calculate_tracking_error(account.account_id, start_date=start_date, end_date=end_date)

    print("Tracking Error vs S&P 500 (SPY proxy):")
    for k, v in result.items():
        print(f"  {k}: {v}")

    session.close()

if __name__ == '__main__':
    main()
