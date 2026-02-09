INSERT INTO AIRFLOW0105.DEV.DIM_DATE_4 (date_sk, date, year, quarter, month, day, weekday, is_weekend, is_month_end, is_quarter_end)
SELECT
  TO_NUMBER(TO_CHAR(h.date, 'YYYYMMDD')) AS date_sk,
  h.date,
  YEAR(h.date),
  QUARTER(h.date),
  MONTH(h.date),
  DAY(h.date),
  DAYOFWEEK(h.date),
  IFF(DAYOFWEEK(h.date) IN (1,7), TRUE, FALSE) AS is_weekend,
  IFF(h.date = LAST_DAY(h.date), TRUE, FALSE) AS is_month_end,
  IFF(h.date = LAST_DAY(h.date, 'QUARTER'), TRUE, FALSE) AS is_quarter_end
FROM AIRFLOW0105.DEV.STG_STOCK_HISTORY_4 h
LEFT JOIN AIRFLOW0105.DEV.DIM_DATE_4 d
  ON d.date = h.date
WHERE d.date IS NULL
GROUP BY h.date;
