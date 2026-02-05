SELECT * FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE LIMIT 10;

SELECT * FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY LIMIT 10;

SELECT * FROM US_STOCK_DAILY.DCCM.SYMBOLS LIMIT 10;

-- test symbol name exchange from SYMBOLS table and symbol, companyname, and exchange from COMPANY_profile 
-- test the symbol column frequncy and same 

SELECT COUNT(distinct symbol) FROM US_STOCK_DAILY.DCCM.SYMBOLS; -- 10913 different symbol in SYMBOLS
SELECT COUNT(distinct symbol) FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE; -- 10913 different symbol in COMPANY_PROFILE

WITH cte1 AS(SELECT distinct symbol FROM US_STOCK_DAILY.DCCM.SYMBOLS),
cte2 AS(SELECT distinct symbol from US_STOCK_DAILY.DCCM.COMPANY_PROFILE)
SELECT
count(*)
FROM cte1
JOIN cte2
USING (symbol);
-- 10913 same symbol between two table, which means they are same

-- STEP2: test the NAME & Companyname column frequncy and same 
SELECT COUNT(distinct name) FROM US_STOCK_DAILY.DCCM.SYMBOLS; -- 10607 different name in SYMBOLS
SELECT COUNT(distinct companyname) FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE; -- 10606 different companyname in COMPANY_PROFILE

WITH cte1 AS(SELECT distinct name FROM US_STOCK_DAILY.DCCM.SYMBOLS),
cte2 AS(SELECT distinct companyname from US_STOCK_DAILY.DCCM.COMPANY_PROFILE)
SELECT
count(*)
FROM cte1
JOIN cte2
ON cte1.name = cte2.companyname;
-- 10606 same symbol between two table, which means they are same

-- STEP3: test the Exchange column frequncy and same 
SELECT COUNT(distinct exchange) FROM US_STOCK_DAILY.DCCM.SYMBOLS; -- 24 different exchange in SYMBOLS
SELECT COUNT(distinct exchange) FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE; -- 23 different exchange in COMPANY_PROFILE

WITH cte1 AS(SELECT distinct exchange FROM US_STOCK_DAILY.DCCM.SYMBOLS),
cte2 AS(SELECT distinct exchange from US_STOCK_DAILY.DCCM.COMPANY_PROFILE)
SELECT
count(*)
FROM cte1
JOIN cte2
ON cte1.exchange = cte2.exchange;
-- 23 same symbol between two table, which means they are same
