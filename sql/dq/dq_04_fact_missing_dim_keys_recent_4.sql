WITH wm AS (
  SELECT {{LAST_LOADED_DATE}}::DATE AS last_loaded_date
),
src AS (
  SELECT DISTINCT h.symbol, h.date
  FROM AIRFLOW0105.DEV.STG_STOCK_HISTORY_4 h
  CROSS JOIN wm
  WHERE wm.last_loaded_date IS NULL OR h.date > wm.last_loaded_date
)
SELECT COUNT(*) AS bad_cnt
FROM src
LEFT JOIN AIRFLOW0105.DEV.DIM_COMPANY_CORE_4 c
  ON src.symbol = c.symbol AND c.is_current = TRUE
LEFT JOIN AIRFLOW0105.DEV.DIM_DATE_4 d
  ON src.date = d.date
LEFT JOIN AIRFLOW0105.DEV.DIM_COMPANY_FINANCIAL_4 f
  ON src.symbol = f.symbol
WHERE c.company_sk IS NULL OR d.date_sk IS NULL OR f.financial_sk IS NULL;