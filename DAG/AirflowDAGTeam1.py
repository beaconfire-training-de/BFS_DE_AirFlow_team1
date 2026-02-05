
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

GROUP_NUM = "team1"
SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"

default_args = {
    "owner": GROUP_NUM,
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id=f"project1_stock_dimensional_etl_{GROUP_NUM}",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",   # daily trigger
    catchup=False,
    default_args=default_args,
    tags=["project1", "snowflake", "stock", "team1",'etl','dwh'],
) as dag:


    load_fact = SnowflakeOperator(
        task_id="load_fact",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="""CREATE OR REPLACE TABLE AIRFLOW0105.DEV.dim_Date_1 (
    date_key        INT             PRIMARY KEY,        -- Surrogate key (YYYYMMDD format)
    full_date       DATE            NOT NULL,           -- Full date value
    year            INT             NOT NULL,           -- Year (e.g., 2026)
    quarter         INT             NOT NULL,           -- Quarter (1-4)
    month           INT             NOT NULL,           -- Month (1-12)
    month_name      VARCHAR(20)     NOT NULL,           -- Month name (e.g., 'January')
    day_of_week     INT             NOT NULL,           -- Day of week (1=Sunday, 7=Saturday)
    day_name        VARCHAR(20)     NOT NULL,           -- Day name (e.g., 'Monday')
    is_weekend      BOOLEAN         NOT NULL,           -- TRUE if Saturday or Sunday
    is_holiday      BOOLEAN         DEFAULT FALSE       -- TRUE if holiday (manually maintained)
);


-- ============================================================
-- 3. DIMENSION TABLE: dim_Company_1
-- Source: US_STOCK_DAILY.DCCM.COMPANY_PROFILE
-- ============================================================
CREATE OR REPLACE TABLE AIRFLOW0105.DEV.dim_Company_1 (
    company_key     INT             PRIMARY KEY AUTOINCREMENT,  -- Surrogate key
    symbol          VARCHAR(20)     NOT NULL UNIQUE,            -- Stock symbol (business key)
    company_name    VARCHAR(255),                               -- Company name
    ceo             VARCHAR(255),                               -- CEO name
    sector          VARCHAR(100),                               -- Sector (e.g., 'Technology')
    industry        VARCHAR(255),                               -- Industry (e.g., 'Software')
    exchange        VARCHAR(100),                               -- Exchange name
    website         VARCHAR(500),                               -- Company website URL
    beta            DECIMAL(10, 5),                             -- Beta coefficient
    mktcap          BIGINT,                                     -- Market capitalization
    description     TEXT                                        -- Company description
);

-- ============================================================
-- 4. FACT TABLE: fact_Stock_Daily_1
-- Source: US_STOCK_DAILY.DCCM.STOCK_HISTORY + calculated fields
-- ============================================================
CREATE OR REPLACE TABLE AIRFLOW0105.DEV.fact_Stock_Daily_1 (
    stock_daily_key INT             PRIMARY KEY AUTOINCREMENT,  -- Surrogate key
    date_key        INT             NOT NULL,                   -- FK to dim_Date_1
    company_key     INT,                                        -- FK to dim_Company_1 (nullable)
    
    -- Measures from source data
    open_price      DECIMAL(18, 8),                             -- Opening price
    high_price      DECIMAL(18, 8),                             -- Highest price
    low_price       DECIMAL(18, 8),                             -- Lowest price
    close_price     DECIMAL(18, 8),                             -- Closing price
    adj_close       DECIMAL(18, 8),                             -- Adjusted closing price
    volume          BIGINT,                                     -- Trading volume
    volavg          DECIMAL(18, 8),                             -- Average volume
    changes         DECIMAL(18, 8),                             -- Price changes
    
    -- Calculated fields
    ma_7            DECIMAL(18, 8),                             -- 7-day moving average
    ma_30           DECIMAL(18, 8),                             -- 30-day moving average
    daily_return    DECIMAL(18, 8),                             -- Daily return rate
    daily_change    DECIMAL(18, 8),                             -- Daily price change
    
    -- Foreign key constraints
    CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES  AIRFLOW0105.DEV.dim_Date_1(date_key),
    CONSTRAINT fk_company FOREIGN KEY (company_key) REFERENCES  AIRFLOW0105.DEV.dim_Company_1(company_key)
);

-- ============================================================
-- 5. POPULATE dim_Date_1 (Generate date dimension data)
-- Generate dates from 1990-01-01 to 2030-12-31
-- ============================================================
INSERT INTO AIRFLOW0105.DEV.dim_Date_1 (date_key, full_date, year, quarter, month, month_name, day_of_week, day_name, is_weekend, is_holiday)
SELECT
    TO_NUMBER(TO_CHAR(date_value, 'YYYYMMDD')) AS date_key,
    date_value AS full_date,
    YEAR(date_value) AS year,
    QUARTER(date_value) AS quarter,
    MONTH(date_value) AS month,
    MONTHNAME(date_value) AS month_name,
    DAYOFWEEK(date_value) AS day_of_week,
    DAYNAME(date_value) AS day_name,
    CASE WHEN DAYOFWEEK(date_value) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday
FROM (
    SELECT DATEADD(DAY, SEQ4(), '1990-01-01'::DATE) AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 15000))  -- ~41 years of dates
) dates
WHERE date_value <= '2030-12-31';

-- ============================================================
-- 7. POPULATE dim_Company_1 (Load from source)
-- ============================================================
INSERT INTO AIRFLOW0105.DEV.dim_Company_1 (symbol, company_name, ceo, sector, industry, exchange, website, beta, mktcap, description)
SELECT DISTINCT
    u1.SYMBOL,
    u1.NAME AS company_name, 
    u2.CEO,
    u2.SECTOR,
    u2.INDUSTRY,
    u1.EXCHANGE,       -- use Symbols‘ exchange （more complete）
    u2.WEBSITE,
    u2.BETA,
    u2.MKTCAP,
    u2.DESCRIPTION
FROM US_STOCK_DAILY.DCCM.SYMBOLS u1
LEFT JOIN US_STOCK_DAILY.DCCM.COMPANY_PROFILE u2
    ON u1.SYMBOL = u2.SYMBOL;

-- ============================================================
-- 8. POPULATE fact_Stock_Daily_1 (Initial Load)
-- Note: Calculated fields (ma_7, ma_30, daily_return, daily_change)
-- will be computed using window functions
-- ============================================================
INSERT INTO AIRFLOW0105.DEV.fact_Stock_Daily_1 (
    date_key, company_key,
    open_price, high_price, low_price, close_price, adj_close, volume, volavg, changes,
    ma_7, ma_30, daily_return, daily_change
)
WITH stock_with_calculations AS (
    SELECT
        sh.SYMBOL,
        sh.DATE,
        sh.OPEN,
        sh.HIGH,
        sh.LOW,
        sh.CLOSE,
        sh.ADJCLOSE,
        sh.VOLUME,
        cp.VOLAVG,
        cp.CHANGES,
        -- 7-day moving average
        AVG(sh.CLOSE) OVER (
            PARTITION BY sh.SYMBOL 
            ORDER BY sh.DATE 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7,
        -- 30-day moving average
        AVG(sh.CLOSE) OVER (
            PARTITION BY sh.SYMBOL 
            ORDER BY sh.DATE 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS ma_30,
        -- Daily return: (close - previous_close) / previous_close
        (sh.CLOSE - LAG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE)) 
            / NULLIF(LAG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE), 0) AS daily_return,
        -- Daily change: close - previous_close
        sh.CLOSE - LAG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE) AS daily_change
    FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
    LEFT JOIN US_STOCK_DAILY.DCCM.COMPANY_PROFILE cp ON sh.SYMBOL = cp.SYMBOL
)
SELECT
    d.date_key,
    
    c.company_key,          -- Can be NULL if no company profile exists
    swc.OPEN,
    swc.HIGH,
    swc.LOW,
    swc.CLOSE,
    swc.ADJCLOSE,
    swc.VOLUME,
    swc.VOLAVG,
    swc.CHANGES,
    swc.ma_7,
    swc.ma_30,
    swc.daily_return,
    swc.daily_change
FROM stock_with_calculations swc
INNER JOIN AIRFLOW0105.DEV.dim_Date_1 d ON TO_NUMBER(TO_CHAR(swc.DATE, 'YYYYMMDD')) = d.date_key
LEFT JOIN AIRFLOW0105.DEV.dim_Company_1 c ON swc.SYMBOL = c.symbol;"""
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

    load_fact >> validate
