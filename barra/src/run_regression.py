"""CLI for running Phase 3 factor return regressions."""
from __future__ import annotations

import argparse
import datetime as dt

from .regression import RegressionEngine


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute and store factor returns/specific returns.")
    parser.add_argument("--date", required=True, help="Month-end date (YYYY-MM-DD)")
    args = parser.parse_args()
    as_of = parse_date(args.date)
    engine = RegressionEngine()
    try:
        result = engine.regress(as_of)
        engine.persist(result)
        print(f"Stored factor returns for {as_of}")
        
        # Phase 2: Compute and persist smoothed specific risk
        smoothed = engine.compute_smoothed_specific_risk(as_of)
        if not smoothed.empty:
            engine.persist_smoothed_specific_risk(as_of, smoothed)
            print(f"Stored smoothed specific risk for {as_of}")
    finally:
        engine.close()


if __name__ == "__main__":
    main()
