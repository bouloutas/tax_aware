import argparse
import datetime as dt

import pandas as pd

from src import cli


class DummyArgs(argparse.Namespace):
    """Helper namespace to make tests less verbose."""


def test_run_factors_command(monkeypatch):
    as_of = dt.date(2025, 9, 30)
    exposures = pd.DataFrame(
        {
            "gvkey": ["1", "2"],
            "factor": ["size", "size"],
            "exposure": [0.1, 0.2],
            "flags": ["", "imputed=industry"],
        }
    )

    def fake_compute_style(as_of_input, **_):
        assert as_of_input == as_of
        return exposures

    style_calls = {}

    def fake_persist_style(df, as_of_input):
        style_calls["called"] = True
        assert as_of_input == as_of
        pd.testing.assert_frame_equal(df, exposures)
        return len(df)

    def fake_compute_industry(as_of_input):
        assert as_of_input == as_of
        return pd.DataFrame({"dummy": [1]})

    def fake_persist_industry(df, as_of_input):
        assert as_of_input == as_of
        return len(df)

    def fake_compute_country(as_of_input):
        assert as_of_input == as_of
        return pd.DataFrame({"dummy": [1]})

    def fake_persist_country(df, as_of_input):
        assert as_of_input == as_of
        return len(df)

    monkeypatch.setattr(cli, "compute_style_exposures", fake_compute_style)
    monkeypatch.setattr(cli, "persist_style_exposures", fake_persist_style)
    monkeypatch.setattr(cli, "compute_industry_exposures", fake_compute_industry)
    monkeypatch.setattr(cli, "persist_industry_exposures", fake_persist_industry)
    monkeypatch.setattr(cli, "compute_country_exposures", fake_compute_country)
    monkeypatch.setattr(cli, "persist_country_exposures", fake_persist_country)

    args = DummyArgs(
        date=as_of.strftime("%Y-%m-%d"),
        factors=None,
        currency_lookback=24,
        currency_min_obs=18,
        skip_industry=False,
        skip_country=False,
    )

    result = cli.run_factors_command(args)

    assert style_calls["called"]
    assert result.details["style_rows"] == len(exposures)
    assert result.details["industry_rows"] == 1
    assert result.details["country_rows"] == 1
    assert not result.details["coverage"].empty


def test_run_risk_command(monkeypatch):
    as_of = dt.date(2025, 9, 30)

    class FakeRegression:
        def __init__(self):
            self.closed = False

        def regress(self, target):
            assert target == as_of
            return type(
                "Result",
                (),
                {
                    "factor_returns": pd.DataFrame({"factor": ["size"], "return": [0.1]}),
                    "residuals": pd.DataFrame({"gvkey": ["1"], "residual": [0.01]}),
                    "specific_risk": pd.DataFrame({"gvkey": ["1"], "specific_var": [0.0001]}),
                },
            )()

        def persist(self, _):
            return None

        def close(self):
            self.closed = True

    class FakeCovariance:
        def __init__(self):
            self.closed = False

        def compute_covariance(self, target, lookback_months=None):
            assert target == as_of
            assert lookback_months == 36
            return pd.DataFrame(
                {
                    "factor_i": ["size"],
                    "factor_j": ["size"],
                    "covariance": [0.02],
                    "month_end_date": [as_of],
                }
            )

        def persist(self, cov_df):
            assert len(cov_df) == 1

        def close(self):
            self.closed = True

    monkeypatch.setattr(cli, "RegressionEngine", FakeRegression)
    monkeypatch.setattr(cli, "CovarianceEngine", FakeCovariance)

    args = DummyArgs(date=as_of.strftime("%Y-%m-%d"), cov_lookback=36, skip_covariance=False)
    result = cli.run_risk_command(args)

    assert result.details["factor_count"] == 1
    assert result.details["residual_count"] == 1
    assert result.details["covariance_rows"] == 1


def test_run_pipeline_command(monkeypatch):
    as_of = dt.date(2025, 9, 30)
    called = {}

    def fake_run_pipeline(target):
        called["as_of"] = target

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)

    args = DummyArgs(date=as_of.strftime("%Y-%m-%d"))
    result = cli.run_pipeline_command(args)

    assert called["as_of"] == as_of
    assert result.details["as_of"] == as_of
