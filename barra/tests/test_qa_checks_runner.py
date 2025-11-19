"""Pytest wrapper for the analytics QA checks."""
from __future__ import annotations

import os

from .qa_checks import parse_date, run_checks


def test_qa_checks_pass() -> None:
    """Surface QA failures directly in pytest output."""
    as_of_str = os.environ.get("QA_CHECK_DATE", "2025-09-30")
    as_of = parse_date(as_of_str)
    results = run_checks(as_of)
    failures = [(name, detail) for name, passed, detail in results if not passed]
    assert not failures, f"QA checks failed for {as_of_str}: {failures}"
