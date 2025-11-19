import numpy as np
import pandas as pd


def winsorize_series(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """Clamp series to percentile thresholds."""
    low_val = series.quantile(lower)
    high_val = series.quantile(upper)
    return series.clip(lower=low_val, upper=high_val)


def zscore(series: pd.Series) -> pd.Series:
    """Standardize to mean 0, std 1 while ignoring NaNs."""
    mean = series.mean()
    std = series.std(ddof=0)
    if std == 0 or np.isnan(std):
        return series * 0
    return (series - mean) / std
