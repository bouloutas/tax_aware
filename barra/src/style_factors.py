"""Factor calculators for Phase 2 style factors."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import duckdb
import numpy as np
import pandas as pd

from .config import (
    ANALYTICS_DB,
    MULTIASSET_DB,
    CURRENCY_BETA_LOOKBACK_MONTHS,
    CURRENCY_BETA_MIN_OBS,
    USE_EXTERNAL_MARKET_PROXY,
    ENFORCE_PIT,
    MULTI_HORIZON_MOMENTUM,
    INCLUDE_MOMENTUM_REVERSAL,
    MULTI_HORIZON_GROWTH,
)
from .utils import winsorize_series, zscore
from .logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class FactorExposure:
    gvkey: str
    factor: str
    exposure: float


class FactorCalculator:
    def __init__(self, analytics_db: str = ANALYTICS_DB.as_posix()) -> None:
        self.conn = duckdb.connect(analytics_db, read_only=True)

    def close(self) -> None:
        self.conn.close()

    def size(self, as_of: dt.date) -> pd.DataFrame:
        """Return Size factor exposures (log market cap)."""
        as_of = _to_date(as_of)
        query = """
        SELECT GVKEY, month_end_market_cap
        FROM analytics.monthly_prices
        WHERE month_end_date = ?
          AND month_end_market_cap IS NOT NULL
          AND month_end_market_cap > 0
        """
        df = self.conn.execute(query, [as_of]).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])

        df["log_mcap"] = df["month_end_market_cap"].apply(float).apply(lambda x: np.log(x))
        df["log_mcap"] = winsorize_series(df["log_mcap"])
        df["exposure"] = zscore(df["log_mcap"])
        exposures = df[["GVKEY", "exposure"]].rename(columns={"GVKEY": "gvkey"})
        exposures["factor"] = "size"
        return exposures[["gvkey", "factor", "exposure"]]

    def beta(
        self, as_of: dt.date, lookback_months: int = 60, min_obs: int = 36
    ) -> pd.DataFrame:
        """Market beta from rolling regression vs. market index.
        
        Uses external SPY if USE_EXTERNAL_MARKET_PROXY=True, otherwise cap-weighted.
        """
        as_of = _to_date(as_of)
        start_date = _shift_months(as_of, -lookback_months)
        
        # Log which market proxy is being used
        if USE_EXTERNAL_MARKET_PROXY:
            logger.info(f"Computing beta using external market proxy (SPY)", 
                       extra={"as_of": str(as_of), "market_proxy": "SPY"})
        
        # Note: analytics.market_index_returns already blends SPY and internal
        # The 'source' column indicates which was used for each date
        query = """
        WITH panel AS (
            SELECT
                mr.GVKEY AS gvkey,
                mr.month_end_date,
                mr.monthly_return,
                mi.market_return
            FROM analytics.monthly_returns mr
            INNER JOIN analytics.market_index_returns mi
                ON mr.month_end_date = mi.month_end_date
            WHERE mr.month_end_date > ?
              AND mr.month_end_date <= ?
              AND mr.monthly_return IS NOT NULL
              AND mi.market_return IS NOT NULL
        ),
        agg AS (
            SELECT
                gvkey,
                COUNT(*) AS n_obs,
                SUM(monthly_return) AS sum_stock,
                SUM(market_return) AS sum_market,
                SUM(monthly_return * market_return) AS sum_cross,
                SUM(market_return * market_return) AS sum_market_sq
            FROM panel
            GROUP BY gvkey
        )
        SELECT * FROM agg
        """
        df = self.conn.execute(query, [start_date, as_of]).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df = df[df["n_obs"] >= min_obs].copy()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["mean_stock"] = df["sum_stock"] / df["n_obs"]
        df["mean_market"] = df["sum_market"] / df["n_obs"]
        df["covariance"] = df["sum_cross"] - df["n_obs"] * df["mean_stock"] * df["mean_market"]
        df["var_market"] = df["sum_market_sq"] - df["n_obs"] * (df["mean_market"] ** 2)
        df = df[df["var_market"].abs() > 0].copy()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["beta_raw"] = df["covariance"] / df["var_market"]
        df["beta_raw"] = winsorize_series(df["beta_raw"])
        df["exposure"] = zscore(df["beta_raw"])
        exposures = df[["gvkey", "exposure"]].copy()
        exposures["factor"] = "beta"
        return exposures[["gvkey", "factor", "exposure"]]

    def momentum(
        self,
        as_of: dt.date,
        lookback_months: int = 12,
        skip_recent: int = 1,
        min_obs: int = 11,
    ) -> pd.DataFrame:
        """12-2 momentum with skip-month logic.
        
        If MULTI_HORIZON_MOMENTUM=True, uses enhanced multi-horizon momentum.
        """
        # Use enhanced multi-horizon momentum if enabled
        if MULTI_HORIZON_MOMENTUM:
            from .enhanced_factors import compute_multi_horizon_momentum
            return compute_multi_horizon_momentum(
                self.conn, 
                as_of,
                include_reversal=INCLUDE_MOMENTUM_REVERSAL
            )
        
        # Baseline 12-2 momentum
        as_of = _to_date(as_of)
        end_date = _shift_months(as_of, -skip_recent)
        long_window_start = _shift_months(as_of, -(lookback_months + skip_recent))
        long_window_end = _shift_months(as_of, -(skip_recent + 1))
        query = """
        SELECT
            GVKEY AS gvkey,
            month_end_date,
            monthly_return
        FROM analytics.monthly_returns
        WHERE month_end_date >= ?
          AND month_end_date <= ?
          AND monthly_return IS NOT NULL
        """
        df = self.conn.execute(query, [long_window_start, end_date]).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["month_end_date"] = pd.to_datetime(df["month_end_date"]).dt.tz_localize(None)
        end_ts = pd.Timestamp(end_date)
        long_start_ts = pd.Timestamp(long_window_start)
        long_end_ts = pd.Timestamp(long_window_end)
        exposures = []
        for gvkey, group in df.groupby("gvkey"):
            group = group.sort_values("month_end_date")
            recent_row = group[group["month_end_date"] <= end_ts].tail(1)
            if recent_row.empty:
                continue
            recent_date = recent_row["month_end_date"].iloc[0]
            if recent_date <= long_end_ts:
                continue
            r_1m = recent_row["monthly_return"].iloc[-1]
            history = group[
                (group["month_end_date"] >= long_start_ts)
                & (group["month_end_date"] <= long_end_ts)
            ]
            if len(history) < min_obs:
                continue
            r_12m = (1 + history["monthly_return"]).prod() - 1
            momentum = (1 + r_1m) * (1 + r_12m) - 1
            exposures.append((gvkey, momentum))
        if not exposures:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        out = pd.DataFrame(exposures, columns=["gvkey", "momentum_raw"])
        out["momentum_raw"] = winsorize_series(out["momentum_raw"])
        out["exposure"] = zscore(out["momentum_raw"])
        out["factor"] = "momentum"
        return out[["gvkey", "factor", "exposure"]]

    def earnings_yield(self, as_of: dt.date, min_quarters: int = 4) -> pd.DataFrame:
        """TTM earnings divided by market cap."""
        as_of = _to_date(as_of)
        query = """
        WITH ordered AS (
            SELECT
                gvkey,
                quarter_end_date,
                SUM(COALESCE(IBQ, 0)) OVER (
                    PARTITION BY gvkey
                    ORDER BY quarter_end_date
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) AS ib_ttm,
                SUM(CASE WHEN IBQ IS NOT NULL THEN 1 ELSE 0 END) OVER (
                    PARTITION BY gvkey
                    ORDER BY quarter_end_date
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) AS ib_obs,
                ROW_NUMBER() OVER (
                    PARTITION BY gvkey
                    ORDER BY quarter_end_date DESC
                ) AS rn
            FROM analytics.fundamentals_quarterly
            WHERE quarter_end_date <= ?
        ),
        latest_ib AS (
            SELECT gvkey, ib_ttm, ib_obs
            FROM ordered
            WHERE rn = 1
        )
        SELECT
            p.GVKEY AS gvkey,
            ib.ib_ttm,
            ib.ib_obs,
            p.month_end_market_cap
        FROM analytics.monthly_prices p
        JOIN latest_ib ib ON p.GVKEY = ib.gvkey
        WHERE p.month_end_date = ?
          AND p.month_end_market_cap IS NOT NULL
          AND p.month_end_market_cap > 0
        """
        df = self.conn.execute(query, [as_of, as_of]).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df = df[df["ib_obs"] >= min_quarters].copy()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["earnings_yield_raw"] = df["ib_ttm"] / df["month_end_market_cap"]
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["earnings_yield_raw"])
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["earnings_yield_raw"] = winsorize_series(df["earnings_yield_raw"])
        df["exposure"] = zscore(df["earnings_yield_raw"])
        exposures = df[["gvkey", "exposure"]].copy()
        exposures["factor"] = "earnings_yield"
        return exposures[["gvkey", "factor", "exposure"]]

    def book_to_price(self, as_of: dt.date) -> pd.DataFrame:
        """Book equity divided by market cap.
        
        Enforces PIT if ENFORCE_PIT=True (uses effective_date).
        """
        as_of = _to_date(as_of)
        
        # Build PIT filter condition
        pit_condition = ""
        params = [as_of, as_of]  # Default: month_end_date twice
        
        if ENFORCE_PIT:
            pit_condition = "AND effective_date <= ?"
            params = [as_of, as_of, as_of]  # month_end_date, effective_date, month_end_date
            logger.info(f"Computing book_to_price with PIT enforcement", 
                       extra={"as_of": str(as_of), "pit_enabled": True})
        
        query = f"""
        WITH ordered AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY GVKEY
                    ORDER BY month_end_date DESC
                ) AS rn
            FROM analytics.fundamentals_annual
            WHERE month_end_date <= ?
              {pit_condition}
        ),
        latest_book AS (
            SELECT
                GVKEY,
                COALESCE(book_equity_t_minus_1, CEQ) AS book_equity
            FROM ordered
            WHERE rn = 1
        )
        SELECT
            p.GVKEY AS gvkey,
            lb.book_equity,
            p.month_end_market_cap
        FROM analytics.monthly_prices p
        JOIN latest_book lb ON p.GVKEY = lb.GVKEY
        WHERE p.month_end_date = ?
          AND lb.book_equity IS NOT NULL
          AND lb.book_equity > 0
          AND p.month_end_market_cap IS NOT NULL
          AND p.month_end_market_cap > 0
        """
        
        df = self.conn.execute(query, params).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["bp_ratio"] = df["book_equity"] / df["month_end_market_cap"]
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["bp_ratio"])
        df = df[df["bp_ratio"] > 0]
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["bp_log"] = np.log(df["bp_ratio"])
        df["bp_log"] = winsorize_series(df["bp_log"])
        df["exposure"] = zscore(df["bp_log"])
        exposures = df[["gvkey", "exposure"]].copy()
        exposures["factor"] = "book_to_price"
        return exposures[["gvkey", "factor", "exposure"]]

    def growth(self, as_of: dt.date) -> pd.DataFrame:
        """Year-over-year sales growth.
        
        If MULTI_HORIZON_GROWTH=True, uses enhanced multi-horizon earnings growth.
        """
        # Use enhanced multi-horizon growth if enabled
        if MULTI_HORIZON_GROWTH:
            from .enhanced_factors import compute_multi_horizon_growth
            return compute_multi_horizon_growth(self.conn, as_of)
        
        # Baseline 1-year sales growth
        as_of = _to_date(as_of)
        query = """
        WITH dedup AS (
            SELECT *
            FROM (
                SELECT
                    GVKEY,
                    formation_year,
                    SALE,
                    month_end_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY GVKEY, formation_year
                        ORDER BY month_end_date DESC
                    ) AS rn
                FROM analytics.fundamentals_annual
                WHERE month_end_date <= ?
                  AND SALE IS NOT NULL
            )
            WHERE rn = 1
        )
        , ranked AS (
            SELECT
                GVKEY,
                formation_year,
                SALE,
                LAG(SALE) OVER (
                    PARTITION BY GVKEY
                    ORDER BY formation_year
                ) AS prev_sale,
                ROW_NUMBER() OVER (
                    PARTITION BY GVKEY
                    ORDER BY formation_year DESC
                ) AS recency_rank
            FROM dedup
        )
        SELECT
            mp.GVKEY,
            r.SALE,
            r.prev_sale
        FROM ranked r
        JOIN analytics.monthly_prices mp
          ON mp.GVKEY = r.GVKEY
        WHERE mp.month_end_date = ?
          AND r.recency_rank = 1
          AND r.prev_sale IS NOT NULL
          AND r.prev_sale > 0
        """
        df = self.conn.execute(query, [as_of, as_of]).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["growth_raw"] = (df["SALE"] / df["prev_sale"]) - 1
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["growth_raw"])
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["growth_raw"] = winsorize_series(df["growth_raw"])
        df["exposure"] = zscore(df["growth_raw"])
        exposures = df[["GVKEY", "exposure"]].rename(columns={"GVKEY": "gvkey"})
        exposures["factor"] = "growth"
        return exposures[["gvkey", "factor", "exposure"]]

    def earnings_variability(self, as_of: dt.date, lookback_quarters: int = 8) -> pd.DataFrame:
        """Standard deviation of quarterly IB, negated so stable earnings score higher."""
        as_of = _to_date(as_of)
        query = """
        WITH ordered AS (
            SELECT
                GVKEY,
                IBQ,
                ROW_NUMBER() OVER (
                    PARTITION BY GVKEY
                    ORDER BY quarter_end_date DESC
                ) AS rn_desc
            FROM analytics.fundamentals_quarterly
            WHERE quarter_end_date <= ?
              AND IBQ IS NOT NULL
        )
        , windowed AS (
            SELECT
                GVKEY,
                IBQ
            FROM ordered
            WHERE rn_desc <= ?
        ),
        agg AS (
            SELECT
                GVKEY,
                STDDEV_SAMP(IBQ) AS ib_std,
                COUNT(*) AS n_obs
            FROM windowed
            WHERE IBQ IS NOT NULL
            GROUP BY GVKEY
            HAVING COUNT(*) >= ?
        )
        SELECT
            mp.GVKEY,
            agg.ib_std
        FROM analytics.monthly_prices mp
        JOIN agg ON mp.GVKEY = agg.GVKEY
        WHERE mp.month_end_date = ?
        """
        min_obs = max(4, lookback_quarters // 2)
        df = self.conn.execute(
            query, [as_of, lookback_quarters, min_obs, as_of]
        ).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["variability_raw"] = -df["ib_std"]
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["variability_raw"])
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["variability_raw"] = winsorize_series(df["variability_raw"])
        df["exposure"] = zscore(df["variability_raw"])
        exposures = df[["GVKEY", "exposure"]].rename(columns={"GVKEY": "gvkey"})
        exposures["factor"] = "earnings_variability"
        return exposures[["gvkey", "factor", "exposure"]]

    def leverage(self, as_of: dt.date) -> pd.DataFrame:
        """Debt-to-asset leverage ratio."""
        as_of = _to_date(as_of)
        query = """
        WITH ordered AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY GVKEY
                    ORDER BY month_end_date DESC
                ) AS rn
            FROM analytics.fundamentals_annual
            WHERE month_end_date <= ?
        )
        , latest AS (
            SELECT
                GVKEY,
                COALESCE(DLTT, 0) AS dltt,
                COALESCE(DLC, 0) AS dlc,
                AT
            FROM ordered
            WHERE rn = 1
              AND AT IS NOT NULL
              AND AT > 0
        )
        SELECT
            mp.GVKEY,
            l.dltt,
            l.dlc,
            l.AT
        FROM analytics.monthly_prices mp
        JOIN latest l ON mp.GVKEY = l.GVKEY
        WHERE mp.month_end_date = ?
        """
        df = self.conn.execute(query, [as_of, as_of]).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["leverage_raw"] = (df["dltt"] + df["dlc"]) / df["AT"]
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["leverage_raw"])
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["leverage_raw"] = winsorize_series(df["leverage_raw"])
        df["exposure"] = zscore(df["leverage_raw"])
        exposures = df[["GVKEY", "exposure"]].rename(columns={"GVKEY": "gvkey"})
        exposures["factor"] = "leverage"
        return exposures[["gvkey", "factor", "exposure"]]

    def dividend_yield(self, as_of: dt.date) -> pd.DataFrame:
        """Trailing 12-month dividend yield (Dividends / market cap)."""
        as_of = _to_date(as_of)
        query = """
        WITH ordered AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY GVKEY
                    ORDER BY month_end_date DESC
                ) AS rn
            FROM analytics.fundamentals_annual
            WHERE month_end_date <= ?
        ),
        latest AS (
            SELECT GVKEY, DVC
            FROM ordered
            WHERE rn = 1
              AND DVC IS NOT NULL
        )
        SELECT
            p.GVKEY AS gvkey,
            l.DVC,
            p.month_end_market_cap
        FROM analytics.monthly_prices p
        JOIN latest l ON p.GVKEY = l.GVKEY
        WHERE p.month_end_date = ?
          AND p.month_end_market_cap IS NOT NULL
          AND p.month_end_market_cap > 0
          AND l.DVC >= 0
        """
        df = self.conn.execute(query, [as_of, as_of]).fetchdf()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["dividend_yield_raw"] = df["DVC"] / df["month_end_market_cap"]
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["dividend_yield_raw"])
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["dividend_yield_raw"] = winsorize_series(df["dividend_yield_raw"])
        df["exposure"] = zscore(df["dividend_yield_raw"])
        key_col = "gvkey" if "gvkey" in df.columns else "GVKEY"
        exposures = df[[key_col, "exposure"]].rename(columns={key_col: "gvkey"})
        exposures["factor"] = "dividend_yield"
        return exposures[["gvkey", "factor", "exposure"]]

    def currency_sensitivity(
        self,
        as_of: dt.date,
        lookback_months: int | None = None,
        min_obs: int | None = None,
    ) -> pd.DataFrame:
        """FX beta vs. UUP ETF (USD Index proxy)."""
        as_of = _to_date(as_of)
        lookback = lookback_months or CURRENCY_BETA_LOOKBACK_MONTHS
        min_required = min_obs or CURRENCY_BETA_MIN_OBS
        start_date = _shift_months(as_of, -lookback)

        # Fetch UUP monthly returns from multiasset DuckDB
        fx_query = """
        WITH ranked AS (
            SELECT
                DATE_TRUNC('month', DATADATE)::DATE AS month_start,
                DATADATE,
                ADJUSTED_PRICE_CLOSE,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE_TRUNC('month', DATADATE)
                    ORDER BY DATADATE DESC
                ) AS rn
            FROM COMPUSTAT_DATA.ETF_DAILY_PRICES
            WHERE ticker = 'UUP'
              AND DATADATE >= ?
              AND DATADATE <= ?
        ),
        monthlies AS (
            SELECT
                DATADATE::DATE AS month_end_date,
                ADJUSTED_PRICE_CLOSE,
                LAG(ADJUSTED_PRICE_CLOSE) OVER (ORDER BY DATADATE) AS prev_price
            FROM ranked
            WHERE rn = 1
        )
        SELECT
            month_end_date,
            ADJUSTED_PRICE_CLOSE / NULLIF(prev_price, 0) - 1 AS fx_return
        FROM monthlies
        WHERE prev_price IS NOT NULL
        """
        with duckdb.connect(MULTIASSET_DB.as_posix(), read_only=True) as fx_con:
            fx_df = fx_con.execute(fx_query, [start_date, as_of]).fetchdf()
        if fx_df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])

        stock_query = """
        SELECT
            GVKEY,
            month_end_date,
            monthly_return
        FROM analytics.monthly_returns
        WHERE month_end_date > ?
          AND month_end_date <= ?
          AND monthly_return IS NOT NULL
        """
        stock_df = self.conn.execute(stock_query, [start_date, as_of]).fetchdf()
        if stock_df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])

        panel = stock_df.merge(fx_df, on="month_end_date", how="inner")
        if panel.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])

        panel["cross"] = panel["monthly_return"] * panel["fx_return"]
        panel["fx_sq"] = panel["fx_return"] * panel["fx_return"]
        grouped = panel.groupby("GVKEY")
        stats = grouped.agg(
            n_obs=("month_end_date", "count"),
            sum_stock=("monthly_return", "sum"),
            sum_fx=("fx_return", "sum"),
            sum_cross=("cross", "sum"),
            sum_fx_sq=("fx_sq", "sum"),
        ).reset_index()
        stats.rename(columns={"GVKEY": "gvkey"}, inplace=True)
        stats = stats[stats["n_obs"] >= min_required]
        if stats.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        stats["mean_stock"] = stats["sum_stock"] / stats["n_obs"]
        stats["mean_fx"] = stats["sum_fx"] / stats["n_obs"]
        stats["covariance"] = stats["sum_cross"] - stats["n_obs"] * stats["mean_stock"] * stats["mean_fx"]
        stats["var_fx"] = stats["sum_fx_sq"] - stats["n_obs"] * (stats["mean_fx"] ** 2)
        stats = stats[stats["var_fx"].abs() > 0]
        if stats.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        stats["currency_beta"] = stats["covariance"] / stats["var_fx"]
        stats["currency_beta"] = winsorize_series(stats["currency_beta"])
        stats["exposure"] = zscore(stats["currency_beta"])
        exposures = stats[["gvkey", "exposure"]].copy()
        exposures["factor"] = "currency_sensitivity"
        return exposures[["gvkey", "factor", "exposure"]]
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df = df[df["n_obs"] >= min_obs].copy()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["mean_stock"] = df["sum_stock"] / df["n_obs"]
        df["mean_fx"] = df["sum_fx"] / df["n_obs"]
        df["covariance"] = df["sum_cross"] - df["n_obs"] * df["mean_stock"] * df["mean_fx"]
        df["var_fx"] = df["sum_fx_sq"] - df["n_obs"] * (df["mean_fx"] ** 2)
        df = df[df["var_fx"].abs() > 0].copy()
        if df.empty:
            return pd.DataFrame(columns=["gvkey", "factor", "exposure"])
        df["currency_beta"] = df["covariance"] / df["var_fx"]
        df["currency_beta"] = winsorize_series(df["currency_beta"])
        df["exposure"] = zscore(df["currency_beta"])
        exposures = df[["gvkey", "exposure"]].copy()
        exposures["factor"] = "currency_sensitivity"
        return exposures[["gvkey", "factor", "exposure"]]


__all__ = ["FactorCalculator", "FactorExposure"]


def _shift_months(value: dt.date, months: int) -> dt.date:
    """Shift a date by N calendar months preserving trading-day offsets."""
    ts = pd.Timestamp(value)
    ts = ts + pd.DateOffset(months=months)
    return ts.date()


def _to_date(value: dt.date) -> dt.date:
    """Normalize any datetime/date to a date."""
    return pd.Timestamp(value).date()
