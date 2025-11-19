-- Phase 1 data-layer materialization script.
-- Run via: duckdb -c "INSTALL httpfs LOAD httpfs" -f phase1_create_analytics_views.sql
-- Assumes DuckDB 1.0+ and that the target analytics DB resides at /home/tasos/tax_aware/barra/barra_analytics.duckdb

ATTACH '/home/tasos/T9_APFS/compustat.duckdb' AS compustat (READ_ONLY);
ATTACH '/home/tasos/T9_APFS/backtest/stock_analysis_backtest.duckdb' AS price (READ_ONLY);
CREATE SCHEMA IF NOT EXISTS analytics;

-- Optional: create a dedicated DuckDB file for analytics tables if not already present.
-- ATTACH '/home/tasos/tax_aware/barra/barra_analytics.duckdb' AS barra;

-----------------------------------------------------------------------
-- 1. Fundamentals snapshots (annual + quarterly)
-----------------------------------------------------------------------
DROP TABLE IF EXISTS analytics.fundamentals_annual;
CREATE TABLE analytics.fundamentals_annual AS
WITH latest_fundamentals AS (
    SELECT
        base.GVKEY,
        base.DATADATE::DATE AS datadate,
        base.CEQ,
        base.IB,
        base.DLTT,
        base.DLC,
        base.AT,
        base.LT,
        base.DVC,
        ROW_NUMBER() OVER (
            PARTITION BY base.GVKEY, base.DATADATE
            ORDER BY base.DATAFMT = 'STD' DESC, base.CONSOL = 'C' DESC
        ) AS rn
    FROM compustat.main.CO_AFND1 AS base
    WHERE base.DATAFMT IN ('STD', 'SUMM_STD')
      AND base.CONSOL IN ('C', 'P')
),
latest_sales AS (
    SELECT
        sales.GVKEY,
        sales.DATADATE::DATE AS datadate,
        sales.SALE,
        sales.OIADP,
        sales.OANCF,
        ROW_NUMBER() OVER (
            PARTITION BY sales.GVKEY, sales.DATADATE
            ORDER BY sales.DATAFMT = 'STD' DESC, sales.CONSOL = 'C' DESC
        ) AS rn
    FROM compustat.main.CO_AFND2 AS sales
    WHERE sales.DATAFMT IN ('STD', 'SUMM_STD')
      AND sales.CONSOL IN ('C', 'P')
)
SELECT
    dfc.GVKEY,
    dfc.RETURN_MONTH_END_DATE AS month_end_date,
    dfc.FORMATION_YEAR_T      AS formation_year,
    dfc.ME_JUNE               AS market_equity_june,
    dfc.BE_FY_T_MINUS_1       AS book_equity_t_minus_1,
    dfc.OP_FY_T_MINUS_1       AS operating_profit_t_minus_1,
    dfc.INV_FY_T_MINUS_1      AS investment_t_minus_1,
    dfc.BM_T_MINUS_1          AS book_to_market_t_minus_1,
    dfc.EXCHG,
    dfc.SIC,
    lf.CEQ,
    lf.IB,
    lf.DLTT,
    lf.DLC,
    lf.AT,
    lf.LT,
    lf.DVC,
    ls.SALE,
    ls.OIADP,
    ls.OANCF,
    lf.datadate AS fundamentals_datadate
FROM compustat.main.data_for_factor_construction dfc
LEFT JOIN latest_fundamentals lf
  ON dfc.GVKEY = lf.GVKEY
 AND dfc.FORMATION_YEAR_T = EXTRACT(YEAR FROM lf.datadate)
 AND lf.rn = 1
LEFT JOIN latest_sales ls
  ON dfc.GVKEY = ls.GVKEY
 AND dfc.FORMATION_YEAR_T = EXTRACT(YEAR FROM ls.datadate)
 AND ls.rn = 1;

COMMENT ON TABLE analytics.fundamentals_annual IS
    'Baseline annual features sourced from data_for_factor_construction. Extend this table with additional Compustat fields as required.';

-----------------------------------------------------------------------
-- 2. Monthly price aggregation & returns from daily_data
-----------------------------------------------------------------------
DROP TABLE IF EXISTS analytics.monthly_prices;
CREATE TABLE analytics.monthly_prices AS
WITH daily AS (
    SELECT
        GVKEY,
        ticker,
        date_trunc('month', datadate) AS month_start,
        datadate,
        adj_price_close,
        company_market_cap_millions
    FROM price.daily_data
    WHERE adj_price_close IS NOT NULL
      AND GVKEY IS NOT NULL
),
ranked AS (
    SELECT
        GVKEY,
        ticker,
        month_start,
        datadate,
        adj_price_close,
        company_market_cap_millions,
        ROW_NUMBER() OVER (
            PARTITION BY GVKEY, month_start
            ORDER BY datadate DESC
        ) AS rn
    FROM daily
)
SELECT
    GVKEY,
    ticker,
    month_start,
    datadate AS month_end_date,
    adj_price_close AS month_end_price,
    company_market_cap_millions AS month_end_market_cap
FROM ranked
WHERE rn = 1;

DROP TABLE IF EXISTS analytics.monthly_returns;
CREATE TABLE analytics.monthly_returns AS
SELECT
    GVKEY,
    ticker,
    month_end_date,
    month_end_price,
    month_end_market_cap,
    month_end_price / LAG(month_end_price) OVER (
        PARTITION BY GVKEY
        ORDER BY month_end_date
    ) - 1 AS monthly_return
FROM analytics.monthly_prices
WHERE month_end_price > 0;

-----------------------------------------------------------------------
-- 3. Market index proxy (SPY)
-----------------------------------------------------------------------
DROP TABLE IF EXISTS analytics.market_index_returns;
CREATE TABLE analytics.market_index_returns AS
SELECT
    month_end_date,
    SUM(monthly_return * month_end_market_cap) / NULLIF(SUM(month_end_market_cap), 0) AS market_return,
    SUM(month_end_market_cap) AS aggregate_market_cap,
    COUNT(*) AS constituent_count
FROM analytics.monthly_returns
GROUP BY month_end_date;

COMMENT ON TABLE analytics.market_index_returns IS
    'Cap-weighted market return proxy computed from monthly_returns. Replace with SPY from multiasset_new if/when that database exposes the ticker.';

-----------------------------------------------------------------------
-- 4. GVKEY↔Ticker mapping (latest)
-----------------------------------------------------------------------
DROP TABLE IF EXISTS analytics.gvkey_ticker_mapping;
CREATE TABLE analytics.gvkey_ticker_mapping AS
WITH ranked AS (
    SELECT
        GVKEY,
        ticker,
        company_name,
        gics_code,
        gics_sector,
        gics_industry,
        datadate,
        ROW_NUMBER() OVER (
            PARTITION BY GVKEY
            ORDER BY datadate DESC
        ) AS rn
    FROM price.daily_data
    WHERE GVKEY IS NOT NULL
      AND ticker IS NOT NULL
)
SELECT
    GVKEY,
    ticker,
    company_name,
    gics_code,
    gics_sector,
    gics_industry,
    datadate AS last_seen_date
FROM ranked
WHERE rn = 1;

-----------------------------------------------------------------------
-- 5. Placeholder table for style factor exposures (populated via Python)
-----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.style_factor_exposures (
    month_end_date DATE NOT NULL,
    gvkey VARCHAR NOT NULL,
    factor VARCHAR NOT NULL,
    exposure DOUBLE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    flags VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_style_factor_exposures_date
    ON analytics.style_factor_exposures (month_end_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_style_factor_exposures_unique
    ON analytics.style_factor_exposures (month_end_date, gvkey, factor);

CREATE TABLE IF NOT EXISTS analytics.industry_exposures (
    month_end_date DATE NOT NULL,
    gvkey VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    factor VARCHAR NOT NULL,
    label VARCHAR,
    exposure DOUBLE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    flags VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_industry_exposures_unique
    ON analytics.industry_exposures (month_end_date, gvkey, factor);

CREATE TABLE IF NOT EXISTS analytics.country_exposures (
    month_end_date DATE NOT NULL,
    gvkey VARCHAR NOT NULL,
    factor VARCHAR NOT NULL,
    label VARCHAR,
    exposure DOUBLE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    flags VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_country_exposures_unique
    ON analytics.country_exposures (month_end_date, gvkey, factor);

CREATE TABLE IF NOT EXISTS analytics.factor_returns (
    month_end_date DATE NOT NULL,
    factor VARCHAR NOT NULL,
    factor_return DOUBLE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (month_end_date, factor)
);

CREATE TABLE IF NOT EXISTS analytics.specific_returns (
    month_end_date DATE NOT NULL,
    gvkey VARCHAR NOT NULL,
    residual DOUBLE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (month_end_date, gvkey)
);

CREATE TABLE IF NOT EXISTS analytics.specific_risk (
    month_end_date DATE NOT NULL,
    gvkey VARCHAR NOT NULL,
    specific_var DOUBLE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (month_end_date, gvkey)
);

DROP TABLE IF EXISTS analytics.factor_covariance;
CREATE TABLE IF NOT EXISTS analytics.factor_covariance (
    month_end_date DATE NOT NULL,
    factor_i VARCHAR NOT NULL,
    factor_j VARCHAR NOT NULL,
    covariance DOUBLE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (month_end_date, factor_i, factor_j)
);

-----------------------------------------------------------------------
-- Notes:
-- * Additional Compustat tables (e.g., CO_AMDA, CO_IMDA) should be joined into
--   analytics.fundamentals_annual/quarterly to surface CEQ, IB, DLTT etc.
-- * The analytics schema acts as the canonical source for downstream factor
--   calculations; keep transformations deterministic and point-in-time safe.
