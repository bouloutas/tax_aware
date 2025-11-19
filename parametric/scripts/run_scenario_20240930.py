#!/usr/bin/env python3
from datetime import date, timedelta
from decimal import Decimal
import os
import pandas as pd

from src.core.database import create_database_engine, get_session_factory, init_database
from src.core.config import Config
from src.core.account_manager import AccountManager
from src.core.position_manager import PositionManager
from src.data.market_data import MarketDataManager
from src.data.benchmark_data import BenchmarkManager
from src.data.barra_loader import BarraDataLoader
from src.data.gvkey_mapper import GVKEYMapper

PORTFOLIO = {
    "MSFT": {"weight": 0.20, "embedded": 3.14},   # +314%
    "DHR":  {"weight": 0.20, "embedded": -0.25},  # -25%
    "META": {"weight": 0.20, "embedded": 0.20},   # +20%
    "XOM":  {"weight": 0.20, "embedded": -0.30},  # -30%
    "PFE":  {"weight": 0.20, "embedded": -0.24},  # -24%
}

AS_OF = date(2024, 9, 30)
TOTAL_VALUE = Decimal("1000000")  # $1,000,000


def compute_acquisition_price(current_price: Decimal, embedded: float) -> Decimal:
    # embedded is +g for gains, negative for losses; current = cost * (1+g) -> cost = current/(1+g)
    denom = Decimal(str(1.0 + embedded))
    if denom == 0:
        raise ValueError("Invalid embedded return leading to zero denominator")
    return (current_price / denom).quantize(Decimal("0.0001"))


def main():
    # Use configured database (PostgreSQL if DATABASE_URL set, else SQLite per Config)
    engine = create_database_engine(Config.get_database_url(), echo=False)
    init_database(engine)
    Session = get_session_factory(engine)
    session = Session()

    acct_mgr = AccountManager(session)
    pos_mgr = PositionManager(session)
    mkt = MarketDataManager(session)
    bench_mgr = BenchmarkManager(session)

    # Create account and equal-weight benchmark over the 5 names
    account = acct_mgr.create_account(
        client_name="Scenario 2024-09-30",
        account_type="taxable",
        tax_rate_short_term=Decimal("0.37"),
        tax_rate_long_term=Decimal("0.20"),
    )

    benchmark = bench_mgr.create_benchmark(benchmark_name="EQW-5")

    acct_mgr.update_account(account.account_id, benchmark_id=benchmark.benchmark_id)

    # For each ticker: ensure market data for 2024-09-30 close; compute acquisition price; create tax lot > 1y old
    rows = []
    for ticker, info in PORTFOLIO.items():
        # Ensure security exists
        from src.core.database import Security
        sec = session.query(Security).filter(Security.ticker == ticker).first()
        if not sec:
            sec = Security(ticker=ticker, company_name=ticker)
            session.add(sec)
            session.commit()
            session.refresh(sec)
        # Download a bit around the date to store price history
        mkt.download_and_store_price_data(ticker, start_date=date(2024, 9, 1), end_date=date(2024, 10, 7))
        close = mkt.get_price_on_date(sec.security_id, AS_OF)
        if close is None:
            # fallback to latest before date
            close = mkt.get_latest_price(sec.security_id)
        close = Decimal(str(close))
        acq = compute_acquisition_price(close, info["embedded"])  # may be higher or lower than close
        # quantity so that weight matches TOTAL_VALUE at close
        qty = (TOTAL_VALUE * Decimal(str(info["weight"])) / close).quantize(Decimal("0.0001"))
        # purchase date > 1 year ago
        purchase_date = AS_OF - timedelta(days=400)
        pos_mgr.create_tax_lot(
            account_id=account.account_id,
            security_id=sec.security_id,
            purchase_date=purchase_date,
            purchase_price=acq,
            quantity=qty,
        )
        rows.append({
            "ticker": ticker,
            "close_2024_09_30": float(close),
            "embedded": info["embedded"],
            "acquisition_price": float(acq),
            "quantity": float(qty),
            "weight": info["weight"],
        })

    # Add benchmark constituents effective AS_OF (equal-weight)
    for ticker in PORTFOLIO.keys():
        from src.core.database import Security
        sec = session.query(Security).filter(Security.ticker == ticker).first()
        bench_mgr.add_constituent(benchmark.benchmark_id, sec.security_id, Decimal("0.20"), AS_OF)

    df = pd.DataFrame(rows)
    print("\nScenario Portfolio @ 2024-09-30")
    print(df.to_string(index=False, float_format=lambda x: f"{x:,.4f}"))

    # Try to load Barra data summary
    try:
        loader = BarraDataLoader()
        rel = loader.find_latest_release()
        print(f"\nBarra release available: {rel}")
    except Exception as e:
        print(f"\nBarra data not available: {e}")

    # Print how to input this portfolio
    print("\nHow to input this portfolio:")
    print("1) Create account and benchmark (equal-weight 5 names)")
    print("2) Download market data for tickers, ensure 2024-09-30 close exists")
    print("3) For each ticker, compute acquisition price: cost = close / (1 + embedded_return)")
    print("4) Create long-term tax lots dated > 365 days before 2024-09-30 with that cost and quantity")
    print("This script did all the above into data/scenario_20240930.db")

    session.close()

if __name__ == "__main__":
    main()
