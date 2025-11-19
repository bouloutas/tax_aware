import datetime as dt
from pathlib import Path

import pandas as pd

from src import export_reports


def test_export_reports_writes_csv(tmp_path, monkeypatch):
    as_of = dt.date(2025, 9, 30)
    exposures = pd.DataFrame(
        {
            "month_end_date": [as_of],
            "gvkey": ["1"],
            "factor": ["size"],
            "exposure": [0.5],
            "flags": [""],
        }
    )
    returns = pd.DataFrame(
        {
            "month_end_date": [as_of],
            "factor": ["size"],
            "factor_return": [0.01],
        }
    )

    monkeypatch.setitem(export_reports.ARTIFACT_LOADERS, "style_exposures", lambda date: exposures)
    monkeypatch.setitem(export_reports.ARTIFACT_LOADERS, "factor_returns", lambda date: returns)

    outputs = export_reports.export_reports(as_of, tmp_path, ["csv"])

    exposures_path = Path(outputs["style_exposures_csv"])
    returns_path = Path(outputs["factor_returns_csv"])
    assert exposures_path.exists()
    assert returns_path.exists()

    exposures_loaded = pd.read_csv(exposures_path)
    returns_loaded = pd.read_csv(returns_path)
    exposures_loaded["month_end_date"] = pd.to_datetime(exposures_loaded["month_end_date"]).dt.date
    returns_loaded["month_end_date"] = pd.to_datetime(returns_loaded["month_end_date"]).dt.date
    exposures_loaded["gvkey"] = exposures_loaded["gvkey"].astype(str)
    exposures_loaded["flags"] = exposures_loaded["flags"].fillna("").astype(str)
    pd.testing.assert_frame_equal(exposures_loaded, exposures)
    pd.testing.assert_frame_equal(returns_loaded, returns)


def test_export_reports_covariance(monkeypatch, tmp_path):
    as_of = dt.date(2025, 9, 30)
    cov_df = pd.DataFrame(
        {
            "month_end_date": [as_of],
            "factor_i": ["size"],
            "factor_j": ["value"],
            "covariance": [0.02],
        }
    )

    monkeypatch.setitem(export_reports.ARTIFACT_LOADERS, "factor_covariance", lambda date: cov_df)

    outputs = export_reports.export_reports(
        as_of,
        tmp_path,
        ["parquet"],
        artifacts=["factor_covariance"],
    )

    path = Path(outputs["factor_covariance_parquet"])
    assert path.exists()
    loaded = pd.read_parquet(path)
    pd.testing.assert_frame_equal(loaded, cov_df)


def test_load_portfolio_summary(monkeypatch):
    as_of = dt.date(2025, 9, 30)
    weights = pd.DataFrame(
        {
            "gvkey": ["1", "2"],
            "month_end_market_cap": [200, 100],
        }
    )
    exposures = pd.DataFrame(
        {
            "month_end_date": [as_of, as_of, as_of, as_of],
            "gvkey": ["1", "1", "2", "2"],
            "factor": ["size", "value", "size", "value"],
            "exposure": [1.0, 0.5, 0.2, 1.5],
            "flags": ["", "", "", ""],
        }
    )
    cov = pd.DataFrame(
        {
            "month_end_date": [as_of, as_of, as_of, as_of],
            "factor_i": ["size", "size", "value", "value"],
            "factor_j": ["size", "value", "size", "value"],
            "covariance": [0.04, 0.01, 0.01, 0.09],
        }
    )
    specific = pd.DataFrame(
        {
            "month_end_date": [as_of, as_of],
            "gvkey": ["1", "2"],
            "specific_var": [0.02, 0.03],
        }
    )

    monkeypatch.setattr(export_reports, "load_top_constituents", lambda *args, **kwargs: weights)
    monkeypatch.setattr(export_reports, "load_style_exposures", lambda *args, **kwargs: exposures)
    monkeypatch.setattr(export_reports, "load_factor_covariance", lambda *args, **kwargs: cov)
    monkeypatch.setattr(export_reports, "load_specific_risk", lambda *args, **kwargs: specific)

    summary = export_reports.load_portfolio_summary(as_of, top_n=2)

    assert (summary["type"] == "factor").sum() == 2
    total_row = summary[summary["factor"] == "total"].iloc[0]
    assert total_row["variance_contribution"] > 0
    assert summary["top_n"].nunique() == 1


def test_export_reports_portfolio_top_n(monkeypatch, tmp_path):
    as_of = dt.date(2025, 9, 30)
    called = {}

    def fake_loader(date, top_n):
        called["date"] = date
        called["top_n"] = top_n
        return pd.DataFrame({"value": [1]})

    monkeypatch.setitem(export_reports.ARTIFACT_LOADERS, "portfolio_summary", fake_loader)

    export_reports.export_reports(
        as_of,
        tmp_path,
        ["csv"],
        artifacts=["portfolio_summary"],
        portfolio_top_n=123,
    )

    assert called["top_n"] == 123
