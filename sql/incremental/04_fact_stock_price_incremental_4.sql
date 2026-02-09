INSERT INTO AIRFLOW0105.DEV.FACT_STOCK_PRICE_4
(
  stock_fact_sk, company_sk, date_sk, financial_sk,
  open, high, low, close, adjclose, volume
)
SELECT
  (SELECT COALESCE(MAX(stock_fact_sk), 0) FROM AIRFLOW0105.DEV.FACT_STOCK_PRICE_4)
  + ROW_NUMBER() OVER (ORDER BY src.symbol, src.date) AS stock_fact_sk,
  c.company_sk,
  d.date_sk,
  f.financial_sk,
  src.open, src.high, src.low, src.close, src.adjclose, src.volume
FROM (
  SELECT
    h.symbol, h.date, h.open, h.high, h.low, h.close, h.adjclose, h.volume
  FROM AIRFLOW0105.DEV.STG_STOCK_HISTORY_4 h
  CROSS JOIN (
    SELECT last_loaded_date
    FROM AIRFLOW0105.DEV.ETL_METADATA_4
    WHERE job_name = 'stock_etl_4'
    LIMIT 1
  ) wm
  WHERE wm.last_loaded_date IS NULL OR h.date > wm.last_loaded_date
) src
JOIN AIRFLOW0105.DEV.DIM_COMPANY_CORE_4 c
  ON src.symbol = c.symbol AND c.is_current = TRUE
JOIN AIRFLOW0105.DEV.DIM_DATE_4 d
  ON src.date = d.date
JOIN AIRFLOW0105.DEV.DIM_COMPANY_FINANCIAL_4 f
  ON src.symbol = f.symbol;