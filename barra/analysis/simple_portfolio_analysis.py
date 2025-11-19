"""Simple portfolio risk/TE analysis using the in-house Barra clone."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Dict, Iterable

import duckdb
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.config import ANALYTICS_DB

PRICE_DB = Path("/home/tasos/T9_APFS/backtest/stock_analysis_backtest.duckdb")
ETF_DB = Path("/home/tasos/T9_APFS/backtest/multiasset_new.duckdb")


def parse_weights(assignments: Iterable[str]) -> Dict[str, float]:
    weights: Dict[str, float] = {}
    for item in assignments:
        if "=" not in item:
            raise argparse.ArgumentTypeError(f"Invalid weight '{item}', use TICKER=WEIGHT")
        ticker, value = item.split("=", 1)
        ticker = ticker.strip().upper()
        try:
            weight = float(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"Invalid weight for '{ticker}': {value}") from exc
        weights[ticker] = weight
    total = sum(weights.values())
    if total == 0:
        raise argparse.ArgumentTypeError("Weights sum to zero")
    if abs(total - 1.0) > 1e-6:
        weights = {k: v / total for k, v in weights.items()}
    return weights


def get_gvkey_map(tickers: Iterable[str]) -> pd.Series:
    tickers = tuple(tickers)
    query = f"""
        SELECT ticker, GVKEY AS gvkey
        FROM analytics.gvkey_ticker_mapping
        WHERE ticker IN ({','.join(['?'] * len(tickers))})
    """
    with duckdb.connect(ANALYTICS_DB.as_posix(), read_only=True) as con:
        df = con.execute(query, tickers).fetchdf()
    if df.empty or df["ticker"].nunique() != len(tickers):
        missing = set(tickers) - set(df["ticker"].tolist())
        raise ValueError(f"Missing GVKEY mapping for: {sorted(missing)}")
    return df.set_index("ticker")["gvkey"]


def load_exposure_matrix(gvkeys: Iterable[str], as_of: dt.date) -> pd.DataFrame:
    gvkeys = tuple(gvkeys)
    params = (as_of,) + gvkeys
    placeholders = ",".join(["?"] * len(gvkeys))
    queries = {
        "style_factor_exposures": f"""
            SELECT GVKEY AS gvkey, factor, exposure
            FROM analytics.style_factor_exposures
            WHERE month_end_date=? AND gvkey IN ({placeholders})
        """,
        "industry_exposures": f"""
            SELECT GVKEY AS gvkey, factor, exposure
            FROM analytics.industry_exposures
            WHERE month_end_date=? AND gvkey IN ({placeholders})
        """,
        "country_exposures": f"""
            SELECT GVKEY AS gvkey, factor, exposure
            FROM analytics.country_exposures
            WHERE month_end_date=? AND gvkey IN ({placeholders})
        """,
    }
    frames = []
    with duckdb.connect(ANALYTICS_DB.as_posix(), read_only=True) as con:
        for sql in queries.values():
            frames.append(con.execute(sql, params).fetchdf())
    exposures = pd.concat(frames, ignore_index=True)
    if exposures.empty:
        raise ValueError("No exposures found for requested gvkeys")
    pivot = exposures.pivot_table(index="gvkey", columns="factor", values="exposure", fill_value=0.0)
    return pivot


def load_covariance(as_of: dt.date) -> pd.DataFrame:
    query = """
        SELECT factor_i, factor_j, covariance
        FROM analytics.factor_covariance
        WHERE month_end_date=?
    """
    with duckdb.connect(ANALYTICS_DB.as_posix(), read_only=True) as con:
        df = con.execute(query, [as_of]).fetchdf()
    if df.empty:
        raise ValueError(f"No covariance matrix stored for {as_of}")
    pivot = df.pivot(index="factor_i", columns="factor_j", values="covariance")
    return pivot.fillna(0.0)


def load_specific_risk(gvkeys: Iterable[str], as_of: dt.date) -> pd.Series:
    placeholders = ",".join(["?"] * len(gvkeys))
    query = f"""
        SELECT GVKEY AS gvkey, specific_var
        FROM analytics.specific_risk
        WHERE month_end_date=? AND gvkey IN ({placeholders})
    """
    params = (as_of,) + tuple(gvkeys)
    with duckdb.connect(ANALYTICS_DB.as_posix(), read_only=True) as con:
        df = con.execute(query, params).fetchdf()
    if df.empty:
        raise ValueError("Missing specific risk rows")
    return df.set_index("gvkey")["specific_var"]


def compute_model_risk(weights: pd.Series, as_of: dt.date) -> Dict[str, object]:
    exposures = load_exposure_matrix(weights.index, as_of)
    cov = load_covariance(as_of)
    factors = cov.index.union(cov.columns)
    exposures = exposures.reindex(columns=factors, fill_value=0.0)
    cov = cov.reindex(index=factors, columns=factors, fill_value=0.0)
    exposure_matrix = exposures.loc[weights.index]
    portfolio_exp = weights.values @ exposure_matrix.values
    cov_matrix = cov.values
    factor_var = float(portfolio_exp @ cov_matrix @ portfolio_exp)
    spec = load_specific_risk(weights.index, as_of)
    specific_var = float((weights ** 2 * spec.reindex(weights.index).fillna(0.0)).sum())
    total_var = factor_var + specific_var
    monthly_vol = np.sqrt(total_var)
    annual_vol = monthly_vol * np.sqrt(12)
    marginal = cov_matrix @ portfolio_exp
    contrib = portfolio_exp * marginal
    contrib_series = pd.Series(contrib, index=factors)
    top_factors = contrib_series.reindex(contrib_series.abs().sort_values(ascending=False).index)[:10]
    return {
        "as_of": as_of.isoformat(),
        "factor_variance": factor_var,
        "specific_variance": specific_var,
        "total_variance": total_var,
        "monthly_vol": monthly_vol,
        "annual_vol": annual_vol,
        "portfolio_exposures": pd.Series(portfolio_exp, index=factors).to_dict(),
        "factor_contributions": top_factors.to_dict(),
    }


def fetch_equity_returns(tickers: Iterable[str], start: dt.date, end: dt.date) -> pd.DataFrame:
    tickers = tuple(tickers)
    query = f"""
        SELECT CAST(datadate AS DATE) AS trade_date, ticker, daily_return
        FROM daily_data
        WHERE ticker IN ({','.join(['?'] * len(tickers))})
          AND datadate > ? AND datadate <= ?
    """
    params = tickers + (start, end)
    with duckdb.connect(PRICE_DB.as_posix(), read_only=True) as con:
        df = con.execute(query, params).fetchdf()
    pivot = df.pivot_table(index="trade_date", columns="ticker", values="daily_return")
    return pivot.sort_index().dropna(how="any")


def fetch_spy_returns(start: dt.date, end: dt.date) -> pd.Series:
    query = """
        WITH ordered AS (
            SELECT CAST(datadate AS DATE) AS trade_date,
                   adjusted_price_close,
                   LAG(adjusted_price_close) OVER (ORDER BY datadate) AS prev_price
            FROM compustat_data.ETF_DAILY_PRICES
            WHERE ticker='SPY' AND datadate > ? AND datadate <= ?
        )
        SELECT trade_date, (adjusted_price_close / prev_price) - 1 AS r
        FROM ordered
        WHERE prev_price IS NOT NULL
    """
    with duckdb.connect(ETF_DB.as_posix(), read_only=True) as con:
        df = con.execute(query, [start, end]).fetchdf()
    return df.set_index("trade_date")["r"].sort_index()


def compute_tracking_error(weights: Dict[str, float], start: dt.date, end: dt.date) -> Dict[str, object]:
    equities = fetch_equity_returns(weights.keys(), start, end)
    spy = fetch_spy_returns(start, end)
    common = equities.index.intersection(spy.index)
    equities = equities.loc[common]
    spy = spy.loc[common]
    weights_series = pd.Series(weights)
    port = equities.mul(weights_series).sum(axis=1)
    active = port - spy
    daily_te = float(active.std(ddof=1))
    annual_te = daily_te * np.sqrt(252)
    summary = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": len(common),
        "daily_tracking_error": daily_te,
        "annual_tracking_error": annual_te,
        "active_return_mean": float(active.mean()),
        "portfolio_return": float((1 + port).prod() - 1),
        "spy_return": float((1 + spy).prod() - 1),
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute Barra risk + realized TE for a simple portfolio")
    parser.add_argument("--as-of", required=True, help="Month-end date for exposures (YYYY-MM-DD)")
    parser.add_argument("--start", required=True, help="Start date for realized TE (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date for realized TE (YYYY-MM-DD)")
    parser.add_argument(
        "--weight",
        dest="weights",
        action="append",
        required=True,
        help="Ticker=weight (weights will be normalized if they don't sum to 1)",
    )
    parser.add_argument("--json-out", help="Optional path to dump JSON results")
    args = parser.parse_args()
    as_of = dt.datetime.strptime(args.as_of, "%Y-%m-%d").date()
    start = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
    end = dt.datetime.strptime(args.end, "%Y-%m-%d").date()
    weights = parse_weights(args.weights)
    gvkey_map = get_gvkey_map(weights.keys())
    weights_by_gvkey = pd.Series(weights).rename(index=gvkey_map.to_dict())
    model = compute_model_risk(weights_by_gvkey, as_of)
    realized = compute_tracking_error(weights, start, end)
    result = {
        "weights": weights,
        "gvkeys": gvkey_map.to_dict(),
        "model": model,
        "realized": realized,
    }
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
