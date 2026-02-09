MERGE INTO AIRFLOW0105.DEV.DIM_COMPANY_FINANCIAL_4 tgt
USING (
  SELECT
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
  FROM AIRFLOW0105.DEV.STG_COMPANY_PROFILE_4
) src
ON tgt.symbol = src.symbol
WHEN MATCHED THEN UPDATE SET
  tgt.range = src.range,
  tgt.price = src.price,
  tgt.beta = src.beta,
  tgt.volavg = src.volavg,
  tgt.mktcap = src.mktcap,
  tgt.lastdiv = src.lastdiv,
  tgt.dcf = src.dcf,
  tgt.dcfdiff = src.dcfdiff,
  tgt.changes = src.changes
WHEN NOT MATCHED THEN INSERT
(
  financial_sk, symbol, range, price, beta, volavg, mktcap, lastdiv, dcf, dcfdiff, changes
)
VALUES
(
  (SELECT COALESCE(MAX(financial_sk), 0) FROM AIRFLOW0105.DEV.DIM_COMPANY_FINANCIAL_4)
  + ROW_NUMBER() OVER (ORDER BY src.symbol),
  src.symbol, src.range, src.price, src.beta, src.volavg, src.mktcap, src.lastdiv, src.dcf, src.dcfdiff, src.changes
);