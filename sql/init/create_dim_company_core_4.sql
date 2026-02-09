CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_COMPANY_CORE_4 (
  company_sk NUMBER,
  symbol VARCHAR(16),
  company_name VARCHAR(512),
  industry VARCHAR(64),
  sector VARCHAR(64),
  exchange VARCHAR(64),
  website VARCHAR(64),
  ceo VARCHAR(64),
  description VARCHAR(2048),
  effective_start_date DATE,
  effective_end_date DATE,
  is_current BOOLEAN
);

INSERT INTO AIRFLOW0105.DEV.DIM_COMPANY_CORE_4
SELECT
  SEQ8(),
  symbol,
  companyname,
  industry,
  sector,
  exchange,
  website,
  ceo,
  description,
  CURRENT_DATE(),
  '9999-12-31'::DATE,
  TRUE
FROM AIRFLOW0105.DEV.STG_COMPANY_PROFILE_4;