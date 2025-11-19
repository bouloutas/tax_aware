# Changelog

## 2025-11-15
- Added `src/run_workflow.py` to orchestrate analytics rebuild, pipeline, QA, and distribution (`python -m src.run_workflow --date YYYY-MM-DD`).
- Introduced `analysis/simple_portfolio_analysis.py` + `analysis/README.md` + `notebooks/portfolio_te.ipynb` for portfolio risk & tracking-error reporting.
- Updated `tests/qa_checks.py` residual tolerance (1e-2) and ran quarter-end QA sweep (all pass).
- Refresh docs (`implementation.md`, `tests.md`) with Phase 5 status, portfolio tools, and operations runbook.
