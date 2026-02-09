CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.FACT_STOCK_PRICE_4 (
  stock_fact_sk NUMBER,
  company_sk NUMBER,
  date_sk NUMBER,
  financial_sk NUMBER,
  open NUMBER(18,8),
  high NUMBER(18,8),
  low NUMBER(18,8),
  close NUMBER(18,8),
  adjclose NUMBER(18,8),
  volume NUMBER(38,8)
);

INSERT INTO AIRFLOW0105.DEV.FACT_STOCK_PRICE_4
SELECT
  SEQ8(),
  c.company_sk,
  d.date_sk,
  f.financial_sk,
  h.open,
  h.high,
  h.low,
  h.close,
  h.adjclose,
  h.volume
FROM AIRFLOW0105.DEV.STG_STOCK_HISTORY_4 h
JOIN AIRFLOW0105.DEV.DIM_COMPANY_CORE_4 c
  ON h.symbol = c.symbol AND c.is_current = TRUE
JOIN AIRFLOW0105.DEV.DIM_DATE_4 d
  ON h.date = d.date
JOIN AIRFLOW0105.DEV.DIM_COMPANY_FINANCIAL_4 f
  ON h.symbol = f.symbol;