-- Supplemental Phase 1 script to surface quarterly Compustat fundamentals.
-- Run via src.build_analytics_db after phase1_create_analytics_views.sql to
-- build analytics.fundamentals_quarterly for style factor inputs.

ATTACH '/home/tasos/T9_APFS/compustat.duckdb' AS compustat (READ_ONLY);
ATTACH '/home/tasos/T9_APFS/backtest/stock_analysis_backtest.duckdb' AS price (READ_ONLY);
CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.fundamentals_quarterly;
CREATE TABLE analytics.fundamentals_quarterly AS
WITH quarterly_base AS (
    SELECT
        base.GVKEY,
        base.DATADATE::DATE AS datadate,
        base.FYR,
        base.CONSOL,
        base.DATAFMT,
        base.CEQQ AS ceqq,
        base.IBQ,
        base.DLTTQ,
        base.DLCQ,
        base.ATQ,
        base.LTQ,
        base.SALEQ,
        base.OIADPQ,
        ROW_NUMBER() OVER (
            PARTITION BY base.GVKEY, base.DATADATE
            ORDER BY base.DATAFMT = 'STD' DESC, base.CONSOL = 'C' DESC
        ) AS rn
    FROM compustat.main.CO_IFNDQ AS base
    WHERE base.DATAFMT IN ('STD', 'SUMM_STD')
      AND base.CONSOL IN ('C', 'P')
),
cashflow_ytd AS (
    SELECT
        cf.GVKEY,
        cf.DATADATE::DATE AS datadate,
        cf.FYR,
        cf.OANCFY,
        ROW_NUMBER() OVER (
            PARTITION BY cf.GVKEY, cf.DATADATE
            ORDER BY cf.DATAFMT = 'STD' DESC, cf.CONSOL = 'C' DESC
        ) AS rn
    FROM compustat.main.CO_IFNDYTD AS cf
    WHERE cf.DATAFMT IN ('STD', 'SUMM_STD')
      AND cf.CONSOL IN ('C', 'P')
),
selected_quarters AS (
    SELECT
        qb.GVKEY,
        qb.datadate,
        qb.FYR,
        qb.CONSOL,
        qb.DATAFMT,
        qb.ceqq,
        qb.IBQ,
        qb.DLTTQ,
        qb.DLCQ,
        qb.ATQ,
        qb.LTQ,
        qb.SALEQ,
        qb.OIADPQ,
        cf.OANCFY
    FROM quarterly_base qb
    LEFT JOIN cashflow_ytd cf
      ON qb.GVKEY = cf.GVKEY
     AND qb.datadate = cf.datadate
     AND cf.rn = 1
    WHERE qb.rn = 1
),
enriched AS (
    SELECT
        sq.*,
        COALESCE(NULLIF(sq.FYR, 0), 12) AS fiscal_year_end_month,
        ((COALESCE(NULLIF(sq.FYR, 0), 12) % 12) + 1) AS fiscal_year_start_month,
        EXTRACT(YEAR FROM sq.datadate) AS calendar_year,
        ((EXTRACT(MONTH FROM sq.datadate) - 1) / 3)::INTEGER + 1 AS calendar_quarter,
        CASE
            WHEN sq.FYR IS NULL OR sq.FYR = 0 THEN EXTRACT(YEAR FROM sq.datadate)
            WHEN EXTRACT(MONTH FROM sq.datadate) > sq.FYR THEN EXTRACT(YEAR FROM sq.datadate) + 1
            ELSE EXTRACT(YEAR FROM sq.datadate)
        END AS fiscal_year
    FROM selected_quarters sq
),
with_cashflow AS (
    SELECT
        e.*,
        LAG(e.OANCFY) OVER (
            PARTITION BY e.GVKEY, e.fiscal_year
            ORDER BY e.datadate
        ) AS prev_oancfy,
        ((
            ((EXTRACT(MONTH FROM e.datadate)::INTEGER - e.fiscal_year_start_month + 12) % 12) / 3
        )::INTEGER + 1) AS fiscal_quarter
    FROM enriched e
)
SELECT
    GVKEY,
    datadate AS quarter_end_date,
    CAST(DATE_TRUNC('month', datadate) + INTERVAL 1 MONTH - INTERVAL 1 DAY AS DATE) AS month_end_date,
    fiscal_year,
    fiscal_quarter,
    fiscal_year_end_month AS fiscal_year_end_month,
    fiscal_year_start_month,
    calendar_year,
    calendar_quarter,
    FYR,
    CONSOL,
    DATAFMT,
    ceqq,
    IBQ,
    DLTTQ,
    DLCQ,
    ATQ,
    LTQ,
    SALEQ,
    OIADPQ,
    OANCFY AS operating_cash_flow_ytd,
    CASE
        WHEN OANCFY IS NULL THEN NULL
        WHEN prev_oancfy IS NULL THEN OANCFY
        ELSE OANCFY - prev_oancfy
    END AS operating_cash_flow_quarter,
    SUM(
        CASE
            WHEN OANCFY IS NULL THEN NULL
            WHEN prev_oancfy IS NULL THEN OANCFY
            ELSE OANCFY - prev_oancfy
        END
    ) OVER (
        PARTITION BY GVKEY
        ORDER BY datadate
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS operating_cash_flow_ttm
FROM with_cashflow
ORDER BY GVKEY, quarter_end_date;

COMMENT ON TABLE analytics.fundamentals_quarterly IS
    'Point-in-time quarterly fundamentals sourced from Compustat CO_IFNDQ/CO_IFNDYTD with derived fiscal calendar + cash flow features.';
