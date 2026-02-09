UPDATE AIRFLOW0105.DEV.DIM_COMPANY_CORE_4 tgt
SET
  effective_end_date = CURRENT_DATE(),
  is_current = FALSE
FROM AIRFLOW0105.DEV.STG_COMPANY_PROFILE_4 src
WHERE tgt.symbol = src.symbol
  AND tgt.is_current = TRUE
  AND (
    COALESCE(tgt.company_name, '') <> COALESCE(src.companyname, '') OR
    COALESCE(tgt.industry, '')     <> COALESCE(src.industry, '') OR
    COALESCE(tgt.sector, '')       <> COALESCE(src.sector, '') OR
    COALESCE(tgt.exchange, '')     <> COALESCE(src.exchange, '') OR
    COALESCE(tgt.website, '')      <> COALESCE(src.website, '') OR
    COALESCE(tgt.ceo, '')          <> COALESCE(src.ceo, '')
  );

INSERT INTO AIRFLOW0105.DEV.DIM_COMPANY_CORE_4
(
  company_sk, symbol, company_name, industry, sector, exchange, website, ceo,
  effective_start_date, effective_end_date, is_current
)
SELECT
  (SELECT COALESCE(MAX(company_sk), 0) FROM AIRFLOW0105.DEV.DIM_COMPANY_CORE_4)
  + ROW_NUMBER() OVER (ORDER BY src.symbol) AS company_sk,
  src.symbol,
  src.companyname AS company_name,
  src.industry,
  src.sector,
  src.exchange,
  src.website,
  src.ceo,
  CURRENT_DATE() AS effective_start_date,
  '9999-12-31'::DATE AS effective_end_date,
  TRUE AS is_current
FROM AIRFLOW0105.DEV.STG_COMPANY_PROFILE_4 src
LEFT JOIN AIRFLOW0105.DEV.DIM_COMPANY_CORE_4 cur
  ON cur.symbol = src.symbol AND cur.is_current = TRUE
WHERE cur.symbol IS NULL;
