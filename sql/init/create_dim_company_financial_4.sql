CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_COMPANY_FINANCIAL_4 (
  financial_sk NUMBER,
  symbol VARCHAR(16),
  range VARCHAR(64),
  price NUMBER(18,8),
  beta NUMBER(18,8),
  volavg NUMBER(38,0),
  mktcap NUMBER(38,0),
  lastdiv NUMBER(18,8),
  dcf NUMBER(18,8),
  dcfdiff NUMBER(18,8),
  changes NUMBER(18,8)
);

INSERT INTO AIRFLOW0105.DEV.DIM_COMPANY_FINANCIAL_4
SELECT
  SEQ8(),
  symbol,
  range,
  price,
  beta,
  volavg,
  mktcap,
  lastdiv,
  dcf,
  dcfdiff,
  changes
FROM AIRFLOW0105.DEV.STG_COMPANY_PROFILE_4;
