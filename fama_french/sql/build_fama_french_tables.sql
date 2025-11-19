-- File: /home/tasos/tax_aware/fama_french/sql/build_fama_french_tables.sql

USE DATABASE FAMA_FRENCH;
USE SCHEMA PROCESSED_COMPUSTAT_DATA;

-- Statement 1: Create STOCK_DATA_MONTHLY_CLEANED
CREATE OR REPLACE TABLE PROCESSED_COMPUSTAT_DATA.STOCK_DATA_MONTHLY_CLEANED AS
SELECT
    pr.GVKEY,
    pr.IID,
    pr.DATADATE,
    s.TPCI,
    s.EXCHG,
    s.SECSTAT,
    pr.CURCDM AS CURCD,
    (tr.TRT1M / 100.0) AS MONTHLY_RETURN,
    (pr.PRCCM * sh.CSHOM) AS MARKET_EQUITY,
    LAG(pr.PRCCM * sh.CSHOM, 1) OVER (PARTITION BY pr.GVKEY, pr.IID ORDER BY pr.DATADATE) AS LAG_MARKET_EQUITY
FROM spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.SEC_MTHPRC pr
JOIN spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.SECURITY s
    ON pr.GVKEY = s.GVKEY AND pr.IID = s.IID
LEFT JOIN spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.SEC_MTHTRT tr
    ON pr.GVKEY = tr.GVKEY AND pr.IID = tr.IID AND pr.DATADATE = tr.DATADATE
LEFT JOIN spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.SEC_MSHARE sh
    ON pr.GVKEY = sh.GVKEY AND pr.IID = sh.IID AND pr.DATADATE = sh.DATADATE
WHERE
    pr.CURCDM = 'USD'
    AND s.TPCI = '0'
    AND s.SECSTAT = 'A' -- VERIFY THIS CODE
    AND s.EXCHG IN ('11', '12', '13', '14', '15', '17', '18', '19', '20') -- VERIFY THESE CODES
    AND pr.DATADATE >= '1960-01-01'
    AND pr.PRCCM IS NOT NULL AND pr.PRCCM > 0
    AND tr.TRT1M IS NOT NULL
    AND sh.CSHOM IS NOT NULL AND sh.CSHOM > 0
QUALIFY MONTHLY_RETURN IS NOT NULL AND MARKET_EQUITY IS NOT NULL AND MARKET_EQUITY > 0;

-- Statement 2: Create ANNUAL_FUNDAMENTALS_PREPARED
CREATE OR REPLACE TABLE PROCESSED_COMPUSTAT_DATA.ANNUAL_FUNDAMENTALS_PREPARED AS
WITH FundaDataRaw AS (
    SELECT
        f1.GVKEY,
        f1.DATADATE AS FISCAL_YEAR_END_DATE,
        COALESCE(akey.FYEAR, YEAR(f1.DATADATE)) AS FISCAL_YEAR,
        akey.CURCD AS CURRENCY_ANNUAL,
        f1.CONSOL,
        f1.POPSRC,
        f1.INDFMT,
        co.SIC AS SIC,
        f1.AT, f1.LT, f1.CEQ, f1.COGS, f1.IB,
        f2.PSTK, f2.PSTKL, f2.PSTKRV, f2.REVT, f2.SEQ, f2.TXDB, f2.XINT, f2.XSGA,
        LAG(f1.AT, 1, 0) OVER (PARTITION BY f1.GVKEY ORDER BY COALESCE(akey.FYEAR, YEAR(f1.DATADATE))) AS PREV_YEAR_TOTAL_ASSETS
    FROM spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.CO_AFND1 f1
    JOIN spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.CO_AFND2 f2
        ON f1.GVKEY = f2.GVKEY AND f1.DATADATE = f2.DATADATE
        AND f1.INDFMT = f2.INDFMT AND f1.DATAFMT = f2.DATAFMT
        AND f1.CONSOL = f2.CONSOL AND f1.POPSRC = f2.POPSRC
    LEFT JOIN spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.CSCO_AKEY akey
        ON f1.GVKEY = akey.GVKEY AND f1.DATADATE = akey.DATADATE
    LEFT JOIN spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.COMPANY co
        ON f1.GVKEY = co.GVKEY
    WHERE
        akey.CURCD = 'USD'
        AND f1.INDFMT = 'INDL'
        AND f1.CONSOL = 'C'
        AND f1.POPSRC = 'D'
        AND f1.DATAFMT = 'STD' -- Ensure we are using standardized format from CO_AFND1
        AND akey.DATAFMT = 'STD' -- Ensure we are using standardized format from CSCO_AKEY
        AND f1.DATADATE >= '1950-01-01'
),
RankedFundaData AS ( -- To handle any remaining duplicates after filtering on STD
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY GVKEY, FISCAL_YEAR
            ORDER BY
                -- If akey.FDATE (filing date) is available and reliable, use it as primary tie-breaker
                -- For now, using FISCAL_YEAR_END_DATE as the main tie-breaker for a given FYEAR
                FISCAL_YEAR_END_DATE DESC
        ) as rn
    FROM FundaDataRaw
),
CalculatedAnnuals AS (
    SELECT
        GVKEY, FISCAL_YEAR_END_DATE, FISCAL_YEAR, CURRENCY_ANNUAL, SIC,
        LEFT(SIC, 2) AS SIC_GROUP_2D,
        (COALESCE(SEQ, CEQ + COALESCE(PSTK,0), AT - LT) - COALESCE(PSTKRV, PSTKL, PSTK, 0) + COALESCE(TXDB, 0)) AS BOOK_EQUITY,
        (COALESCE(REVT,0) - COALESCE(COGS,0) - COALESCE(XSGA,0) - COALESCE(XINT,0)) AS OP_NUMERATOR_FF,
        IB AS OP_NUMERATOR_IB,
        CASE WHEN PREV_YEAR_TOTAL_ASSETS IS NOT NULL AND PREV_YEAR_TOTAL_ASSETS != 0 THEN (AT - PREV_YEAR_TOTAL_ASSETS) / PREV_YEAR_TOTAL_ASSETS ELSE NULL END AS INVESTMENT_RATE,
        AT AS TOTAL_ASSETS
    FROM RankedFundaData
    WHERE rn = 1 -- Pick the unique, best record
      AND SIC IS NOT NULL
      AND CURRENCY_ANNUAL = 'USD'
)
SELECT *
FROM CalculatedAnnuals
WHERE (SIC_GROUP_2D NOT BETWEEN '60' AND '69')
  AND BOOK_EQUITY IS NOT NULL AND BOOK_EQUITY > 0
  AND FISCAL_YEAR IS NOT NULL;

-- Statement 3: Create DATA_FOR_FACTOR_CONSTRUCTION
CREATE OR REPLACE TABLE PROCESSED_COMPUSTAT_DATA.DATA_FOR_FACTOR_CONSTRUCTION AS
WITH
ME_December AS (
    SELECT GVKEY, IID, LAST_DAY(DATADATE, 'MONTH') AS ME_DEC_DATE, MARKET_EQUITY AS ME_DECEMBER
    FROM PROCESSED_COMPUSTAT_DATA.STOCK_DATA_MONTHLY_CLEANED
    WHERE MONTH(DATADATE) = 12
),
ME_June AS (
    SELECT GVKEY, IID, LAST_DAY(DATADATE, 'MONTH') AS ME_JUNE_DATE, MARKET_EQUITY AS ME_JUNE
    FROM PROCESSED_COMPUSTAT_DATA.STOCK_DATA_MONTHLY_CLEANED
    WHERE MONTH(DATADATE) = 6
)
SELECT
    s.GVKEY, s.IID, s.DATADATE AS RETURN_MONTH_END_DATE,
    YEAR(ADD_MONTHS(s.DATADATE, -6)) AS FORMATION_YEAR_T,
    s.MONTHLY_RETURN, s.EXCHG,
    af.SIC,
    mj.ME_JUNE,
    af.BOOK_EQUITY AS BE_FY_T_MINUS_1,
    CASE
        WHEN af.BOOK_EQUITY IS NOT NULL AND af.BOOK_EQUITY != 0
        THEN af.OP_NUMERATOR_FF / af.BOOK_EQUITY
        ELSE NULL
    END AS OP_FY_T_MINUS_1,
    af.INVESTMENT_RATE AS INV_FY_T_MINUS_1,
    CASE
        WHEN md.ME_DECEMBER IS NOT NULL AND md.ME_DECEMBER != 0
        THEN af.BOOK_EQUITY / md.ME_DECEMBER
        ELSE NULL
    END AS BM_T_MINUS_1
FROM PROCESSED_COMPUSTAT_DATA.STOCK_DATA_MONTHLY_CLEANED s
JOIN ME_June mj
    ON s.GVKEY = mj.GVKEY AND s.IID = mj.IID
    AND mj.ME_JUNE_DATE = LAST_DAY(DATEFROMPARTS(YEAR(ADD_MONTHS(s.DATADATE, -6)), 6, 1), 'MONTH')
JOIN PROCESSED_COMPUSTAT_DATA.ANNUAL_FUNDAMENTALS_PREPARED af
    ON s.GVKEY = af.GVKEY
    AND af.FISCAL_YEAR = (YEAR(ADD_MONTHS(s.DATADATE, -6)) - 1)
    AND YEAR(af.FISCAL_YEAR_END_DATE) = (YEAR(ADD_MONTHS(s.DATADATE, -6)) - 1)
JOIN ME_December md
    ON s.GVKEY = md.GVKEY AND s.IID = md.IID
    AND md.ME_DEC_DATE = LAST_DAY(DATEFROMPARTS(YEAR(ADD_MONTHS(s.DATADATE, -6)) - 1, 12, 1), 'MONTH')
WHERE
    s.DATADATE >= '1963-07-01'
    AND mj.ME_JUNE IS NOT NULL AND mj.ME_JUNE > 0
    AND md.ME_DECEMBER IS NOT NULL AND md.ME_DECEMBER > 0
    AND s.DATADATE >= mj.ME_JUNE_DATE
    AND s.DATADATE < ADD_MONTHS(mj.ME_JUNE_DATE, 12)
    AND af.GVKEY IS NOT NULL;