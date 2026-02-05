from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"

default_args = {
    "owner": "team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=f"project1_stock_dimensional_etl_{team1}",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",   # daily trigger
    catchup=False,
    default_args=default_args,
    tags=["project1", "snowflake", "stock", "team1",'etl','dwh'],
) as dag:
    load_dims = SnowflakeOperator(
        task_id="load_dims",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="""
    """,
    )

    load_fact = SnowflakeOperator(
        task_id="load_fact",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="""create table if not exists AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 (
    PK_STOCK_HISTORY_ID NUMBER AUTOINCREMENT,

    -- Foreign keys
    DATE_KEY        DATE NOT NULL,
    SYMBOL           VARCHAR(16) NOT NULL,
    company_key       VARCHAR(16) not null,

    -- Measures
    OPEN_PRICE           NUMBER(18,6),
    HIGH_PRICE           NUMBER(18,6),
    LOW_PRICE            NUMBER(18,6),
    CLOSE_PRICE          NUMBER(18,6),
    VOLUME               NUMBER(18,0),
    ADJCLOSE_PRICE      NUMBER(18,6),
    VOLAVG          NUMBER(18,6),
    CHANGES         NUMBER(18,6),
    MA_7            NUMBER(18,6),
    MA_30           NUMBER(18,6),
    DAILY_RETURN    NUMBER(18,6),
    DAILY_CHANGE    NUMBER(18,6)

    -- Metadata
    LOAD_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_fact_1 PRIMARY KEY (PK_STOCK_HISTORY_ID)
);

insert into AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 (
    DATE_KEY,
    SYMBOL,
    COMPANY_KEY,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    CLOSE_PRICE,
    VOLUME,
    ADJCLOSE_PRICE,
    VOLAVG,
    CHANGES,
    MA_7,
    MA_30,
    DAILY_RETURN,
    DAILY_CHANGE,
    
)
SELECT
    dd.DATE_KEY,
    ds.SYMBOL_KEY,
    dc.COMPANY_KEY,
    sh.OPEN,
    sh.HIGH,
    sh.LOW,
    sh.CLOSE,
    sh.ADJCLOSE,
    sh.VOLUME,
    sh.VOLAVG,
    sh.CHANGES,
    sh.MA_7,
    sh.MA_30,
    sh.DAILY_RETURN,
    sh.DAILY_CHANGE
FROM STG_STOCK_HISTORY sh
JOIN DIM_DATE dd
  ON sh.DATE = dd.FULL_DATE
JOIN DIM_SYMBOL ds
  ON sh.SYMBOL = ds.SYMBOL
JOIN DIM_COMPANY dc
  ON sh.SYMBOL = dc.SYMBOL



--merge fact table

MERGE INTO AIRFLOW0105.DEV.FACT_STOCK_DAILY_1 tgt
USING (
    SELECT
        dd.DATE_KEY,
        ds.SYMBOL_KEY,
        dc.COMPANY_KEY,
        sh.OPEN,
        sh.HIGH,
        sh.LOW,
        sh.CLOSE,
        sh.ADJCLOSE,
        sh.VOLUME,
        sh.VOLAVG,
        sh.CHANGES,
        sh.MA_7,
        sh.MA_30,
        sh.DAILY_RETURN,
        sh.DAILY_CHANGE
    FROM STG_STOCK_HISTORY sh
    JOIN DIM_DATE dd
      ON sh.DATE = dd.FULL_DATE
    JOIN DIM_SYMBOL ds
      ON sh.SYMBOL = ds.SYMBOL
    JOIN DIM_COMPANY dc
      ON sh.SYMBOL = dc.SYMBOL
) src
ON  tgt.DATE_KEY    = src.DATE_KEY
AND tgt.SYMBOL_KEY  = src.SYMBOL_KEY
AND tgt.COMPANY_KEY = src.COMPANY_KEY

WHEN MATCHED THEN UPDATE SET
    tgt.OPEN_PRICE   = src.OPEN,
    tgt.HIGH_PRICE   = src.HIGH,
    tgt.LOW_PRICE    = src.LOW,
    tgt.CLOSE_PRICE  = src.CLOSE,
    tgt.ADJ_CLOSE    = src.ADJCLOSE,
    tgt.VOLUME       = src.VOLUME,
    tgt.VOLAVG       = src.VOLAVG,
    tgt.CHANGES      = src.CHANGES,
    tgt.MA_7         = src.MA_7,
    tgt.MA_30        = src.MA_30,
    tgt.DAILY_RETURN = src.DAILY_RETURN,
    tgt.DAILY_CHANGE = src.DAILY_CHANGE,
    tgt.LOAD_TS      = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    DATE_KEY,
    SYMBOL_KEY,
    COMPANY_KEY,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    CLOSE_PRICE,
    ADJ_CLOSE,
    VOLUME,
    VOLAVG,
    CHANGES,
    MA_7,
    MA_30,
    DAILY_RETURN,
    DAILY_CHANGE
)
VALUES (
    src.DATE_KEY,
    src.SYMBOL_KEY,
    src.COMPANY_KEY,
    src.OPEN,
    src.HIGH,
    src.LOW,
    src.CLOSE,
    src.ADJCLOSE,
    src.VOLUME,
    src.VOLAVG,
    src.CHANGES,
    src.MA_7,
    src.MA_30,
    src.DAILY_RETURN,
    src.DAILY_CHANGE
)""",
    )

    validate = SnowflakeOperator(
        task_id="validate",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="""SELECT * FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE LIMIT 10;

SELECT * FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY LIMIT 10;

SELECT * FROM US_STOCK_DAILY.DCCM.SYMBOLS LIMIT 10;

-- test symbol name exchange from SYMBOLS table and symbol, companyname, and exchange from COMPANY_profile 
-- test the symbol column frequncy and same 

SELECT COUNT(distinct symbol) FROM US_STOCK_DAILY.DCCM.SYMBOLS; -- 10913 different symbol in SYMBOLS
SELECT COUNT(distinct symbol) FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE; -- 10913 different symbol in COMPANY_PROFILE

WITH cte1 AS(SELECT distinct symbol FROM US_STOCK_DAILY.DCCM.SYMBOLS),
cte2 AS(SELECT distinct symbol from US_STOCK_DAILY.DCCM.COMPANY_PROFILE)
SELECT
count(*)
FROM cte1
JOIN cte2
USING (symbol);
-- 10913 same symbol between two table, which means they are same

-- STEP2: test the NAME & Companyname column frequncy and same 
SELECT COUNT(distinct name) FROM US_STOCK_DAILY.DCCM.SYMBOLS; -- 10607 different symbol in SYMBOLS
SELECT COUNT(distinct companyname) FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE; -- 10606 different symbol in COMPANY_PROFILE

WITH cte1 AS(SELECT distinct name FROM US_STOCK_DAILY.DCCM.SYMBOLS),
cte2 AS(SELECT distinct companyname from US_STOCK_DAILY.DCCM.COMPANY_PROFILE)
SELECT
count(*)
FROM cte1
JOIN cte2
ON cte1.name = cte2.companyname;
-- 10606 same symbol between two table, which means they are same

-- STEP3: test the Exchange column frequncy and same 
SELECT COUNT(distinct exchange) FROM US_STOCK_DAILY.DCCM.SYMBOLS; -- 24 different symbol in SYMBOLS
SELECT COUNT(distinct exchange) FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE; -- 23 different symbol in COMPANY_PROFILE

WITH cte1 AS(SELECT distinct exchange FROM US_STOCK_DAILY.DCCM.SYMBOLS),
cte2 AS(SELECT distinct exchange from US_STOCK_DAILY.DCCM.COMPANY_PROFILE)
SELECT
count(*)
FROM cte1
JOIN cte2
ON cte1.exchange = cte2.exchange;
-- 23 same symbol between two table, which means they are same""",
    )

    load_dims >> load_fact >> validate
