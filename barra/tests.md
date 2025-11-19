# Barra Risk Model – Test Plan

This document enumerates validation steps required to ensure the Barra implementation matches the PRD and replicates the reference risk model as closely as possible. Tests should be automated where feasible (PyTest/notebooks) and run for every release.

---

## 1. Analytics DB Build Verification
- **Rebuild smoke test**: `python -m src.build_analytics_db` must finish without errors. Capture runtime + final table counts in build log.
- **Table presence**: `analytics.fundamentals_annual`, `analytics.fundamentals_quarterly`, `analytics.monthly_prices`, `analytics.monthly_returns`, `analytics.market_index_returns`, `analytics.gvkey_ticker_mapping`, `analytics.style_factor_exposures`, `analytics.industry_exposures`, `analytics.country_exposures`, `analytics.factor_returns`, `analytics.specific_returns`, `analytics.specific_risk` must exist post-build.
- **Row counts** (tolerance ±1% vs. historical baseline):
  - `fundamentals_annual` ≈ 530k rows.
  - `fundamentals_quarterly` ≈ 2.06M rows (matches unique GVKEY × quarter combos).
  - `monthly_prices` ≈ 250k rows.
  - `market_index_returns` ≈ number of distinct month-end dates in prices.
  - `style_factor_exposures` = `#stocks * #factors` for each stored month-end (currently 10 style factors; expect ≥8 exposures per stock depending on data coverage/imputation).
  - `industry_exposures` = `#stocks * 3` (sector/industry/sub-industry rows) per stored month-end.
  - `country_exposures` = `#stocks` per stored month-end.
  - `factor_returns` = # of active factors (style + industry + country) per stored month-end.
  - `specific_returns`/`specific_risk` = `#stocks` per stored month-end.
  - `factor_covariance` = `#factors^2` per stored month-end once sufficient history exists.
- **Attachments**: Ensure scripts attach `/home/tasos/T9_APFS/compustat.duckdb` and `/home/tasos/T9_APFS/backtest/stock_analysis_backtest.duckdb` exactly once; builder should not fail with “database already attached”.
- **Automated QA**: `python tests/qa_checks.py --date YYYY-MM-DD` (or `QA_CHECK_DATE=YYYY-MM-DD pytest tests/test_qa_checks_runner.py`) must pass; this script enforces table presence, row-count snapshots, exposure integrity, regression residual stats, and covariance dimensions.

---

## 2. Data Integrity Checks
### 2.1 Compustat Fundamentals
- **Schema check**: `analytics.fundamentals_annual` must include `GVKEY`, `month_end_date`, `formation_year`, `CEQ`, `IB`, `DLTT`, `DLC`, `AT`, `LT`, `SALE`, `OIADP`, `OANCF`, `DVC`, `fundamentals_datadate`. Document any fields intentionally absent (e.g., `SALEI` currently unavailable).
- **Quarterly schema**: `analytics.fundamentals_quarterly` must include `GVKEY`, `quarter_end_date`, `fiscal_year`, `fiscal_quarter`, `fiscal_year_end_month`, `fiscal_year_start_month`, `CEQQ`, `IBQ`, `DLTTQ`, `DLCQ`, `ATQ`, `LTQ`, `SALEQ`, `OIADPQ`, `operating_cash_flow_ytd`, `operating_cash_flow_quarter`, `operating_cash_flow_ttm`. Alert if fiscal calendar math drifts (e.g., fiscal quarter not in 1–4).
- **Non-null thresholds**: For each field, compute % missing per year; alert if >5% missing compared to trailing 12-month average.
- **Value sanity**:
  - Price > 0, Shares > 0 (flag zero/negative).
  - Book value, sales, operating cash flow non-negative unless business context explains otherwise.
- **Point-in-time consistency**: Confirm no records have `datadate` > `filed_date` or future-dated entries.

### 2.2 Price Data (`stock_analysis_backtest.duckdb`)
- **Table presence**: `daily_data`, `monthly_returns`, `market_index_returns`.
- **Coverage**: For the equity universe, ensure at least 60 monthly returns history for >90% of names.
- **Gap detection**: For each ticker, identify missing months (should match market holidays only).
- **Return sanity**: Daily and monthly returns bounded (e.g., [-0.9, +5.0]); flag outliers for manual review.

### 2.3 ID Mapping
- **Join coverage**: Validate that `id_bridge` map resolves >99% of GVKEYs to price records.
- **Dupes**: Check for duplicate GVKEY→ticker mappings; ensure latest record wins or tie-breaker logic enforced.

---

## 3. Factor Exposure Tests
For each style factor:
- **Input completeness**: Before calculation, assert all necessary fields exist and pass non-null thresholds for the rebalance date.
- **Range tests**:
  - Beta: expected within [-1, +3]; flag exposures beyond.
  - Momentum: log transform results roughly within ±4 std dev.
  - Size: log market cap distribution matches historical quartiles.
  - Earnings Yield / Book-to-Price / Dividend Yield: check for negative exposures only where fundamentals justify.
  - Growth: YOY sales growth must fall within reasonable bounds (e.g., [-100%, +300%]).
  - Earnings Variability: ensure exposures reflect negated stddev of IBQ (more stable → higher score).
  - Leverage: ratio bounded (e.g., <=5) and no divide-by-zero events.
  - Currency Sensitivity: FX beta vs. UUP (from `multiasset_new.duckdb`); verify regression inputs cover ≥`CURRENCY_BETA_MIN_OBS` and factor distribution is centered after z-scoring.
- **Winsorization**: Verify that post-winsorization min/max equals threshold percentiles; ensure z-scores have mean ~0, std ~1.
- **Imputation**: For stocks with missing data, confirm industry median values are used and flagged.
- **No NaNs**: Final factor exposure tables must contain no NaNs; use asserts when writing to DuckDB.
- **Historical note**: Early history (2009‑2010) relies on sparse fundamentals, so imputation share may exceed the 30% warning threshold—review the `tests/qa_checks.py` output and treat it as a documented warning rather than a hard failure for those months.

### Industry Factors
- **One-hot integrity**: Each stock should belong to exactly one sector/industry (sum of exposures per stock = 1).
- **Missing GICS**: If GICS missing, ensure “Unclassified” bucket exists with exposure = 1.
- **Validation hook**: `run_factors` now raises if any GVKEY has 0/2+ rows per level or exposure ≠ 1; QA should keep this check enabled.

### Country Factor
- **Constant exposure**: All stocks should show exposure = 1.0.
- **Historical coverage**: `analytics.country_exposures` must exist for every month-end 2017-01-31 through the latest pipeline date (currently 2025-09-30); QA harness should be run on the latest date whenever exposures are refreshed.

---

## 4. Factor Return Regression Tests
1. **Design matrix rank**: For each month, verify the regression matrix has full rank (no perfect multicollinearity). If rank-deficient, fall back to regularization and log the event.
2. **Weights**: Confirm WLS weights sum to 1 per regression and correspond to market caps.
3. **Residual diagnostics**:
   - Mean of residuals ≈ 0.
   - Residual variance distribution stable over time.
4. **Factor return plausibility**:
   - Compare against Barra USE3/USE4 factors (if available) to ensure correlation >0.8 for overlapping factors.
   - Magnitude check: monthly style factor returns typically within ±5%; flag larger moves.
5. **Regression coverage**: # of stocks used per month > threshold (e.g., >500). Trigger alerts if count drops abruptly.
6. **Persistence**: `python -m src.run_regression --date YYYY-MM-DD` should populate `analytics.factor_returns`, `analytics.specific_returns`, and `analytics.specific_risk`; verify row counts and stamped timestamps. Once ≥2 months of factor returns exist, `python -m src.run_covariance --date YYYY-MM-DD` should persist a full covariance matrix (expect symmetric entries); until then the CLI should log that history is insufficient.

---

## 5. Covariance & Specific Risk Tests
1. **Factor covariance matrix**:
   - Positive semi-definite (all eigenvalues ≥ 0).
   - Condition number within acceptable range (<1e6).
2. **Specific risk**:
   - Non-negative variances.
   - Rolling averages track historical levels; detect sudden jumps/drops.
3. **Total covariance**:
   - `Σ_total = B Σ_F B' + Σ_S` should be PSD; confirm via eigenvalue check.
   - For random portfolios, predicted variance should be non-negative.

---

## 6. End-to-End Portfolio Tests
1. **Benchmark replication**: Build a portfolio matching Barra’s published sample (e.g., equal-weighted universe) and compare predicted risk to Barra numbers if available.
2. **Factor attribution**: For historical periods, compute actual vs predicted portfolio returns; confirm that factor contributions plus specific returns sum to realized returns.
3. **Stress cases**:
   - Single stock portfolio (should equal specific variance + factor exposure contributions).
   - Sector-concentrated portfolio (industry factors should dominate).
4. **Sensitivity tests**: Shock factor covariance matrix (e.g., +10% variance) and confirm portfolio risk changes accordingly.

---

## 7. Pipeline & Regression Tests
1. **CLI smoke tests**:
   - `python -m src.run_factors --date YYYY-MM-DD` completes and populates `style_factor_exposures` with `len(universe) * len(factors)` rows plus matching industry/country exposures (3 + 1 rows per stock). Verify delete/reload semantics by re-running the same date twice and ensure imputed rows carry `flags` (`imputed=industry` or `imputed=global`). CLI output must include the coverage summary table; raise alerts if any factor exceeds the configured imputation threshold (currently 30%).
2. **Re-run reproducibility**: Running the same date twice yields identical outputs (hash comparison).
3. **Incremental updates**: When adding a new month, ensure only new rows are appended and historical data untouched. `python -m src.run_pipeline --date YYYY-MM-DD` should orchestrate all steps; capture logs when covariance is skipped due to insufficient history.
4. **Index maintenance**: If DuckDB ever raises duplicate-key errors writing style exposures, run `python -m src.maintenance --rebuild-style-index` before re-running the pipeline; the command rebuilds `analytics.idx_style_factor_exposures_unique`.

---

## 8. Monitoring & Alerts
1. **Data freshness**: Alerts if latest compustat data is older than X days or price data lags by >2 trading days.
2. **Null spike**: Automated job to flag any factor exposure table where >1% entries are NaN/inf.
3. **Factor drift**: Monitor mean and std of exposures per factor; trigger alert if deviates beyond 3σ from trailing 12-month stats.
4. **Regression failure**: Pager/notification when regression falls back to regularized solution or fails rank check.

---

## 9. Documentation & Acceptance
1. Maintain a test results log (per release) summarizing pass/fail status of the above checks.
2. Before declaring parity with Barra:
   - Provide quantitative comparison (corr, RMSE) between in-house factor returns and Barra USE3/USE4.
   - Document any deviations (e.g., missing currency factor detail, different market universe).
3. QA sign-off requires green status on:
   - Data integrity suite
   - Factor exposures check
   - Regression diagnostics
   - Covariance PSD checks
   - Portfolio-level validations

---

## 10. Application CLI
- **Unit coverage**: `tests/test_cli.py` stubs the factor, regression, and covariance engines to ensure the consolidated CLI dispatches into each Phase 2/3 module without touching DuckDB. Run `pytest tests/test_cli.py` whenever the CLI wiring changes.
- **Smoke test**: `python -m src.cli run-pipeline --date YYYY-MM-DD` should complete without errors for the latest month before promoting a release build.

---

## 11. Reporting & Exports
- **Export regression**: `tests/test_export_reports.py` mocks DuckDB queries to confirm `src/export_reports.py` writes exposures, factor returns, covariance slices, specific risk, and the new `portfolio_summary` artifact (which depends on the top-N selector). Run this whenever modifying export wiring.
- **Distribution helper**: `tests/test_distribute_reports.py` stubs the exporter to guarantee `src/distribute_reports.py` writes manifests/releases correctly and propagates the top-N parameter; keep this test green when adjusting packaging.
- **Data drop test**: `python -m src.export_reports --date YYYY-MM-DD --output-dir exports --format csv --format parquet --artifact style_exposures --artifact factor_returns --artifact factor_covariance --artifact specific_risk --artifact portfolio_summary --portfolio-top-n 100` must finish without errors for the latest pipeline month before sharing files externally. For client-ready bundles, follow with `python -m src.distribute_reports --date YYYY-MM-DD --portfolio-top-n 100` to stamp `exports/releases/`.

---

## 12. Portfolio TE / Benchmark Analyses
- Script: `analysis/simple_portfolio_analysis.py` computes model risk (analytics exposures/covariance/specific risk) and realized tracking error vs. SPY over arbitrary windows. Sample command lives in `analysis/README.md`; current output stored in `analysis/simple_portfolio_report.json`.
- Inputs: `stock_analysis_backtest.daily_data` for equities and `multiasset_new.compustat_data.ETF_DAILY_PRICES` for SPY. Weights auto-normalize if they don’t sum to 1.

---

This test plan should evolve as the implementation matures. Each new feature or data source must add corresponding validation steps to preserve Barra fidelity.
