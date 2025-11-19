-- File: test_permissions.sql
-- This query tests if we can read from the source S&P table.

SELECT * 
FROM spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.SEC_MTHPRC
LIMIT 1;

SELECT * FROM spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.SECURITY LIMIT 1;


SELECT * FROM spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.SEC_MTHTRT LIMIT 1;

SELECT * FROM
spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.SEC_MSHARE LIMIT 1;
SELECT * FROM
spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.CO_AFND1 LIMIT 1;
SELECT * FROM
spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.CO_AFND2  LIMIT 1;
SELECT * FROM
spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.CSCO_AKEY LIMIT 1;
SELECT * FROM
spglobalxpresscloud_spglobalxpresscloud_aws_us_east_1_xf_l32partners.xpressfeed.COMPANY LIMIT 1;