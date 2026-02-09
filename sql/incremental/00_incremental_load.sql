-- Load data to dim_date table (Append)
SELECT DISTINCT date_sk, date, year, quarter, month, day, weekday, is_weekend, is_month_end, is_quarter_end
FROM STG_STOCK_HISTORY;

-- Load data to dim_company_financial (SCD 1) 
MERGE INTO dim_company_financial AS d
USING STG_COMPANY_PROFILE AS s
ON d.financial_sk = s.financial_sk
WHEN MATCHED AND (
d.symbol <> s.symbol OR
d.range <> s.range OR
d.price <> s.price OR
d.beta <> s.beta OR
d.volavg <> s.volavg OR
d.mktcap <> s.mktcap OR
d.lastdiv <> s.lastdiv OR
d.dcf <> s.dcf OR
d.dcfdiff <> s.dcfdiff OR
d.changes <> s.changes OR) THEN
    UPDATE SET
        d.symbol = s.symbol
        d.range = s.range
        d.price = s.price
        d.beta = s.beta
        d.volavg = s.volavg
        d.mktcap = s.mktcap
        d.lastdiv = s.lastdiv
        d.dcf = s.dcf
        d.dcfdiff = s.dcfdiff
        d.changes = s.changes
        d.updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
    financial_sk,
    symbol,
    range,
    price,
    beta,
    volavg,
    mktcap,
    lastdiv,
    dcf,
    dcfdiff,
    changes,
    updated_at)
    VALUES (
    s.symbol
    s.range
    s.price
    s.beta
    s.volavg
    s.mktcap
    s.lastdiv
    s.dcf
    s.dcfdiff
    s.changes,
    current_timestamp());

-- Load data to dim_company_core (SCD 2) 
-- SCD 2_Step 1: Update existing data effective_end_date
UPDATE dim_company_core d
SET effective_end_date = CURRENT_TIMESTAMP,
    is_current = FALSE
FROM STG_COMPANY_PROFILE s
WHERE d.company_sk = s.company_sk
  AND d.is_current = TRUE
  AND (
        s.symbol <> d.symbol OR
        s.company_name <> d.company_name OR
        s.industry <> d.industry OR
        s.sector <> d.sector OR
        s.exchange <> d.exchange OR
        s.website <> d.website OR
        s.ceo <> d.ceo
  );
-- SCD 2_Step 2: Insert new data
INSErt INTO dim_company_core(
company_sk,
symbol,
company_name,
industry,
sector,
exchange,
website,
ceo,
effective_start_date,
effective_end_date,
is_current
)
SELECT
    s.symbol,
    s.company_name,
    s.industry,
    s.sector,
    s.exchange,
    s.website,
    s.ceo,
    CURRENT_TIMESTAMP,
    '9999-12-31 23:59:59',
    TRUE
FROM STG_COMPANY_PROFILE s
LEFT JOIN dim_company_core d
  ON s.company_sk = d.company_sk
  AND d.is_current = TRUE
WHERE d.customer_id IS NULL OR 
    s.symbol <> d.symbol OR
    s.company_name <> d.company_name OR
    s.industry <> d.industry OR
    s.sector <> d.sector OR
    s.exchange <> d.exchange OR
    s.website <> d.website OR
    s.ceo <> d.ceo
   );

-- Load data to fact table
INSERT INTO fact_stock_price_4
SELECT stock_fact_sk, company_sk, date_sk, financial_sk, open, high, low, close, adjclose, volume
FROM STG_STOCK_HISTORY;

--Update ETL Metadata??
/*UPDATE etl_metadata
SET last_loaded_ts = (
        SELECT MAX(ingest_ts) FROM staging_dim_customer
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE dag_name = 'dim_customer_scd2';*/

