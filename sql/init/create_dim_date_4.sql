CREATE OR REPLACE TABLE AIRFLOW0105.DEV.DIM_DATE_4 AS
SELECT DISTINCT
  TO_NUMBER(TO_CHAR(date,'YYYYMMDD')) AS date_sk,
  date,
  YEAR(date)    AS year,
  QUARTER(date) AS quarter,
  MONTH(date)   AS month,
  DAY(date)     AS day,
  DAYOFWEEK(date) AS weekday,
  IFF(DAYOFWEEK(date) IN (1,7), TRUE, FALSE) AS is_weekend,
  IFF(date = LAST_DAY(date), TRUE, FALSE)    AS is_month_end,
  FALSE AS is_quarter_end
FROM AIRFLOW0105.DEV.STG_STOCK_HISTORY_4;