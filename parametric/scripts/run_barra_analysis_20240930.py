#!/usr/bin/env python3
from collections import defaultdict
from decimal import Decimal
from typing import Dict

import numpy as np
import pandas as pd

from src.core.database import create_database_engine, get_session_factory, init_database, Security
from src.core.config import Config
from src.optimization.tracking_error import TrackingErrorCalculator
from src.optimization.optimizer import PortfolioOptimizer
from src.data.barra_loader import BarraDataLoader
from src.data.gvkey_mapper import GVKEYMapper

AS_OF = pd.Timestamp('2024-09-30').date()


def get_current_weights(session) -> pd.DataFrame:
    # Use TrackingErrorCalculator helper to get current weights for the single account
    from src.core.database import Account
    account = session.query(Account).first()
    te = TrackingErrorCalculator(session)
    return account.account_id, te.get_current_weights(account.account_id)


def compute_barra_portfolio_exposures(weights_df: pd.DataFrame) -> Dict[str, float]:
    loader = BarraDataLoader()
    release_date = loader.find_latest_release()
    exposures_df = loader.load_style_exposures(release_date)
    mapper = GVKEYMapper()

    # Map tickers -> gvkey once
    ticker_to_gv = {}
    for _, row in weights_df.iterrows():
        ticker_to_gv[row['ticker']] = mapper.ticker_to_gvkey(row['ticker'])

    # Build portfolio exposures
    portfolio_exposures = defaultdict(float)
    for _, row in weights_df.iterrows():
        ticker = row['ticker']
        weight = float(row['weight'])
        gvkey = ticker_to_gv.get(ticker)
        if not gvkey:
            continue
        sec_exp = exposures_df[exposures_df['gvkey'].astype(str).str.lstrip('0') == str(gvkey).lstrip('0')]
        for _, e in sec_exp.iterrows():
            portfolio_exposures[e['factor']] += weight * float(e['exposure'])

    return dict(portfolio_exposures)


def compute_barra_te_from_cov(active_exposures: Dict[str, float]) -> float:
    loader = BarraDataLoader()
    release_date = loader.find_latest_release()
    cov_df = loader.load_factor_covariance(release_date)

    factors = sorted(set(cov_df['factor_i']).union(set(cov_df['factor_j'])))
    idx = {f: i for i, f in enumerate(factors)}
    cov = np.zeros((len(factors), len(factors)))
    for _, r in cov_df.iterrows():
        i = idx[r['factor_i']]
        j = idx[r['factor_j']]
        cov[i, j] = float(r['covariance'])
        cov[j, i] = float(r['covariance'])

    x = np.zeros(len(factors))
    for f, val in active_exposures.items():
        if f in idx:
            x[idx[f]] = float(val)
    te = float(np.sqrt(max(0.0, x @ cov @ x)))
    return te


def main():
    # Connect
    engine = create_database_engine(Config.get_database_url(), echo=False)
    Session = get_session_factory(engine)
    session = Session()

    # Current weights
    account_id, weights_df = get_current_weights(session)
    print("Current Weights:")
    print(weights_df.to_string(index=False, float_format=lambda x: f"{x:,.6f}"))

    # Barra exposures for portfolio and benchmark
    portfolio_exp = compute_barra_portfolio_exposures(weights_df)

    # Benchmark weights
    te_calc = TrackingErrorCalculator(session)
    bench_df = te_calc.get_benchmark_weights(account_id, effective_date=AS_OF)
    bench_exp = compute_barra_portfolio_exposures(bench_df)

    # Active exposures (portfolio - benchmark)
    all_factors = sorted(set(portfolio_exp) | set(bench_exp))
    active = {f: portfolio_exp.get(f, 0.0) - bench_exp.get(f, 0.0) for f in all_factors}

    # Compute TE from covariance
    barra_te = compute_barra_te_from_cov(active)

    print("\nTop Portfolio Factor Exposures (Barra, absolute top 10):")
    top_f = sorted(portfolio_exp.items(), key=lambda kv: abs(kv[1]), reverse=True)[:10]
    for f, v in top_f:
        print(f"  {f:30s} {v:+.6f}")

    print("\nTop Active Factor Exposures (absolute top 10):")
    top_a = sorted(active.items(), key=lambda kv: abs(kv[1]), reverse=True)[:10]
    for f, v in top_a:
        print(f"  {f:30s} {v:+.6f}")

    print(f"\nBarra Tracking Error (from covariance): {barra_te:.6f}")

    # Run optimization (should be near no-op since portfolio equals benchmark)
    optimizer = PortfolioOptimizer(session)
    result = optimizer.optimize_portfolio(account_id=account_id, max_tracking_error=None)
    print("\nOptimization Result:")
    print(f"  Status: {result['status']}")
    print(f"  Objective: {result['objective_value']:.6f}")
    print(f"  Tracking Error (norm proxy): {result['tracking_error']:.6f}")
    print(f"  Optimal Weights (non-zero): {len(result['optimal_weights'])}")
    # Show trades if any
    trades = result['trades']
    if trades:
        print("  Trades (first 10):")
        for sid, tw in list(trades.items())[:10]:
            print(f"    security_id={sid}: {tw:+.6f}")
    else:
        print("  Trades: none")

    session.close()

if __name__ == "__main__":
    main()
