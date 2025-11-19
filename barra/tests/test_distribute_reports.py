import datetime as dt
import json

from src import distribute_reports
from src import export_reports


def test_distribute_creates_release(tmp_path, monkeypatch):
    as_of = dt.date(2025, 9, 30)
    called = {}

    def fake_export(as_of_date, output_dir, formats, artifacts=None, portfolio_top_n=0):
        called["as_of"] = as_of_date
        called["output_dir"] = output_dir
        called["formats"] = tuple(formats)
        called["portfolio_top_n"] = portfolio_top_n
        outputs = {}
        for fmt in formats:
            path = output_dir / f"style_exposures_{as_of_date}.{fmt}"
            path.write_text("dummy")
            outputs[f"style_exposures_{fmt}"] = path
        return outputs

    monkeypatch.setattr(export_reports, "export_reports", fake_export)

    release = distribute_reports.distribute(
        as_of,
        tmp_path,
        formats=("csv",),
        artifacts=None,
        portfolio_top_n=75,
    )

    assert release.exists()
    manifest = json.loads((release / "manifest.json").read_text())
    assert manifest["as_of"] == as_of.isoformat()
    assert manifest["portfolio_top_n"] == 75
    assert (tmp_path / "latest").is_symlink()
    assert called["portfolio_top_n"] == 75
