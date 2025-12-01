"""CLI for computing and persisting style factor exposures."""
from __future__ import annotations

import argparse
import datetime as dt
from typing import Iterable, List, Sequence

import duckdb
import pandas as pd

from .config import (
    ANALYTICS_DB,
    CURRENCY_BETA_LOOKBACK_MONTHS,
    CURRENCY_BETA_MIN_OBS,
    IMPUTATION_WARNING_THRESHOLD,
    ORTHOGONALIZE_FACTORS,
)
from .style_factors import FactorCalculator
from .logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_FACTORS: Sequence[str] = (
    "size",
    "beta",
    "momentum",
    "earnings_yield",
    "book_to_price",
    "growth",
    "earnings_variability",
    "leverage",
    "dividend_yield",
    "currency_sensitivity",
)


def parse_date(value: str | None) -> dt.date:
    if value is None:
        today = pd.Timestamp.today()
        return (today + pd.offsets.MonthEnd(0)).date()
    return pd.Timestamp(value).date()


def compute_style_exposures(
    as_of: dt.date,
    factors: Iterable[str] | None = None,
    currency_lookback: int = CURRENCY_BETA_LOOKBACK_MONTHS,
    currency_min_obs: int = CURRENCY_BETA_MIN_OBS,
) -> pd.DataFrame:
    factors = tuple(factors or DEFAULT_FACTORS)
    calc = FactorCalculator()
    frames: List[pd.DataFrame] = []
    try:
        for factor in factors:
            if not hasattr(calc, factor):
                raise ValueError(f"Factor '{factor}' not implemented on FactorCalculator")
            if factor == "currency_sensitivity":
                result = getattr(calc, factor)(
                    as_of,
                    lookback_months=currency_lookback,
                    min_obs=currency_min_obs,
                )
            else:
                result = getattr(calc, factor)(as_of)
            if result.empty:
                continue
            frames.append(result)
    finally:
        calc.close()
    if not frames:
        return pd.DataFrame(columns=["gvkey", "factor", "exposure", "month_end_date", "computed_at", "flags"])
    exposures = pd.concat(frames, ignore_index=True)
    if "flags" not in exposures.columns:
        exposures["flags"] = ""
    exposures["flags"] = exposures["flags"].fillna("")
    
    # Apply industry median imputation
    exposures = apply_industry_imputation(exposures, as_of)
    
    # Store raw exposures before orthogonalization
    exposures["exposure_raw"] = exposures["exposure"].copy()
    
    # Phase 2: Apply orthogonalization if enabled
    if ORTHOGONALIZE_FACTORS:
        logger.info(f"Applying factor orthogonalization for {as_of}", 
                   extra={"as_of": str(as_of), "orthogonalize": True})
        
        from .orthogonalization import (
            center_and_scale,
            orthogonalize_style_factors,
            apply_cross_sectional_constraints
        )
        
        # Step 1: Center and scale all factors
        exposures = center_and_scale(exposures)
        
        # Step 2: Orthogonalize style factors against size
        exposures = orthogonalize_style_factors(exposures, base_factor='size')
        
        # Step 3: Re-apply cross-sectional constraints
        exposures = apply_cross_sectional_constraints(exposures)
        
        logger.info(f"Orthogonalization complete", extra={"n_factors": len(exposures['factor'].unique())})
    
    exposures["month_end_date"] = pd.Timestamp(as_of).date()
    exposures["computed_at"] = pd.Timestamp.utcnow()
    exposures["flags"] = exposures["flags"].fillna("")
    return exposures


def persist_style_exposures(df: pd.DataFrame, as_of: dt.date, db_path=ANALYTICS_DB) -> int:
    if df.empty:
        return 0
    df = df.drop_duplicates(subset=["gvkey", "factor"])
    df = df[["month_end_date", "gvkey", "factor", "exposure", "computed_at", "flags"]]
    conn = duckdb.connect(db_path.as_posix())
    try:
        conn.execute(
            "DELETE FROM analytics.style_factor_exposures WHERE month_end_date = ?",
            [as_of],
        )
        conn.execute("USE analytics")
        conn.append("style_factor_exposures", df)
    finally:
        conn.close()
    return len(df)


def compute_industry_exposures(as_of: dt.date) -> pd.DataFrame:
    """Map each GVKEY to sector/industry/sub-industry exposures."""
    industry_map = _load_industry_map(as_of)
    df = industry_map.copy()
    if df.empty:
        return pd.DataFrame(columns=["gvkey", "factor", "level", "label", "exposure", "month_end_date", "computed_at", "flags"])
    rows = []
    for row in df.itertuples(index=False):
        for level, label in [
            ("sector", getattr(row, "gics_sector")),
            ("industry", getattr(row, "gics_industry")),
            ("sub_industry", getattr(row, "gics_sub_industry")),
        ]:
            label_val = label or "Unclassified"
            factor = f"{level}:{label_val}"
            rows.append((row.gvkey, factor, level, label_val, 1.0))
    exposures = pd.DataFrame(rows, columns=["gvkey", "factor", "level", "label", "exposure"])
    validate_industry_one_hot(exposures)
    exposures["month_end_date"] = pd.Timestamp(as_of).date()
    exposures["computed_at"] = pd.Timestamp.utcnow()
    exposures["flags"] = ""
    return exposures


def persist_industry_exposures(df: pd.DataFrame, as_of: dt.date, db_path=ANALYTICS_DB) -> int:
    if df.empty:
        return 0
    conn = duckdb.connect(db_path.as_posix())
    try:
        conn.execute(
            "DELETE FROM analytics.industry_exposures WHERE month_end_date = ?",
            [as_of],
        )
        conn.register("industry_df", df)
        conn.execute(
            """
            INSERT INTO analytics.industry_exposures
                (month_end_date, gvkey, level, factor, label, exposure, computed_at, flags)
            SELECT month_end_date, gvkey, level, factor, label, exposure, computed_at, flags
            FROM industry_df
            """
        )
    finally:
        conn.close()
    return len(df)


def compute_country_exposures(as_of: dt.date, label: str = "United States") -> pd.DataFrame:
    industry_map = _load_industry_map(as_of)
    df = industry_map[["gvkey"]].drop_duplicates()
    if df.empty:
        return pd.DataFrame(columns=["gvkey", "factor", "label", "exposure", "month_end_date", "computed_at", "flags"])
    df["factor"] = "country:US"
    df["label"] = label
    df["exposure"] = 1.0
    df["month_end_date"] = pd.Timestamp(as_of).date()
    df["computed_at"] = pd.Timestamp.utcnow()
    df["flags"] = ""
    return df[["gvkey", "factor", "label", "exposure", "month_end_date", "computed_at", "flags"]]


def persist_country_exposures(df: pd.DataFrame, as_of: dt.date, db_path=ANALYTICS_DB) -> int:
    if df.empty:
        return 0
    conn = duckdb.connect(db_path.as_posix())
    try:
        conn.execute(
            "DELETE FROM analytics.country_exposures WHERE month_end_date = ?",
            [as_of],
        )
        conn.register("country_df", df)
        conn.execute(
            """
            INSERT INTO analytics.country_exposures
                (month_end_date, gvkey, factor, label, exposure, computed_at, flags)
            SELECT month_end_date, gvkey, factor, label, exposure, computed_at, flags
            FROM country_df
            """
        )
    finally:
        conn.close()
    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute and store factor exposures.")
    parser.add_argument(
        "--date",
        dest="as_of",
        help="Month-end date (YYYY-MM-DD). Defaults to latest month-end.",
    )
    parser.add_argument(
        "--factor",
        dest="factors",
        action="append",
        help=(
            "Specific style factor(s) to compute. Defaults to size, beta, momentum, "
            "earnings_yield, book_to_price, growth, earnings_variability, leverage, dividend_yield."
        ),
    )
    parser.add_argument(
        "--currency-lookback",
        type=int,
        default=CURRENCY_BETA_LOOKBACK_MONTHS,
        help="Months for currency beta regression (default %(default)s).",
    )
    parser.add_argument(
        "--currency-min-obs",
        type=int,
        default=CURRENCY_BETA_MIN_OBS,
        help="Minimum overlapping observations for currency beta (default %(default)s).",
    )
    parser.add_argument(
        "--skip-industry",
        action="store_true",
        help="Skip computing industry exposures.",
    )
    parser.add_argument(
        "--skip-country",
        action="store_true",
        help="Skip computing country exposures.",
    )
    args = parser.parse_args()
    as_of = parse_date(args.as_of)
    exposures = compute_style_exposures(
        as_of,
        args.factors,
        currency_lookback=args.currency_lookback,
        currency_min_obs=args.currency_min_obs,
    )
    inserted_style = persist_style_exposures(exposures, as_of)
    msg_parts = [f"style={inserted_style}"]
    if not args.skip_industry:
        industry_df = compute_industry_exposures(as_of)
        inserted_industry = persist_industry_exposures(industry_df, as_of)
        msg_parts.append(f"industry={inserted_industry}")
    if not args.skip_country:
        country_df = compute_country_exposures(as_of)
        inserted_country = persist_country_exposures(country_df, as_of)
        msg_parts.append(f"country={inserted_country}")
    if not exposures.empty:
        coverage = (
            exposures.assign(imputed=exposures["flags"].str.contains("imputed", na=False))
            .groupby("factor")
            .agg(total=("gvkey", "count"), imputed=("imputed", "sum"))
            .reset_index()
        )
        coverage["imputed_pct"] = coverage["imputed"] / coverage["total"].replace(0, pd.NA)
        high_impute = coverage[coverage["imputed_pct"] > IMPUTATION_WARNING_THRESHOLD]
        if not high_impute.empty:
            print(
                "WARNING: High imputation detected for factors:\n"
                + high_impute.to_string(index=False)
            )
        print("Factor coverage/imputation summary:\n" + coverage.to_string(index=False))
    print(f"Computed exposures for {as_of}: " + ", ".join(msg_parts))


def _load_industry_map(as_of: dt.date) -> pd.DataFrame:
    as_of = pd.Timestamp(as_of).date()
    conn = duckdb.connect(ANALYTICS_DB.as_posix(), read_only=True)
    try:
        df = conn.execute(
            """
            SELECT
                mp.GVKEY AS gvkey,
                COALESCE(map.gics_sector, 'Unclassified') AS gics_sector,
                COALESCE(map.gics_industry, 'Unclassified') AS gics_industry,
                COALESCE(map.gics_code, 'Unclassified') AS gics_sub_industry
            FROM analytics.monthly_prices mp
            LEFT JOIN analytics.gvkey_ticker_mapping map
              ON mp.GVKEY = map.GVKEY
            WHERE mp.month_end_date = ?
            """,
            [as_of],
        ).fetchdf()
    finally:
        conn.close()
    return df


def apply_industry_imputation(exposures: pd.DataFrame, as_of: dt.date) -> pd.DataFrame:
    """Fill missing exposures with industry medians (fallback to global median/zero)."""
    industry_map = _load_industry_map(as_of)[["gvkey", "gics_industry"]]
    if industry_map.empty:
        exposures["flags"] = exposures["flags"].fillna("")
        return exposures
    frames = []
    factors = exposures["factor"].unique()
    for factor in factors:
        factor_obs = exposures[exposures["factor"] == factor][["gvkey", "exposure", "flags"]]
        merged = industry_map.merge(factor_obs, on="gvkey", how="left")
        merged["flags"] = merged["flags"].fillna("")
        missing = merged["exposure"].isna()
        if missing.any():
            available = merged.loc[~missing]
            if not available.empty:
                industry_medians = available.groupby("gics_industry")["exposure"].median()
                merged.loc[missing, "exposure"] = merged.loc[missing, "gics_industry"].map(industry_medians)
                global_median = available["exposure"].median()
                merged.loc[missing & merged["exposure"].isna(), "exposure"] = global_median
            merged["exposure"] = merged["exposure"].fillna(0.0)
            merged.loc[missing & (merged["exposure"] != 0.0), "flags"] = "imputed=industry"
            merged.loc[missing & (merged["flags"] == ""), "flags"] = "imputed=global"
        merged["factor"] = factor
        frames.append(merged[["gvkey", "factor", "exposure", "flags"]])
    return pd.concat(frames, ignore_index=True)


def validate_industry_one_hot(df: pd.DataFrame) -> None:
    for level in ("sector", "industry", "sub_industry"):
        subset = df[df["level"] == level]
        counts = subset.groupby("gvkey")["exposure"].count()
        if not counts.eq(1).all():
            bad = counts[counts != 1].index.tolist()
            raise ValueError(f"Industry exposure validation failed for level {level}: {bad[:5]}")
        if not subset["exposure"].eq(1.0).all():
            raise ValueError(f"Industry exposure values not equal to 1 for level {level}")


if __name__ == "__main__":
    main()
