
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

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
        sql="""-- ============================================================
-- Stock Data Warehouse - Star Schema
-- Team 1 - Snowflake SQL DDL Scripts
-- Database: AIRFLOW0105
-- Schema: DEV
-- ============================================================

-- Set the context
USE DATABASE AIRFLOW0105;
USE SCHEMA DEV;

-- ============================================================
-- 1. DIMENSION TABLE: dim_Date_1
-- Source: Generated date dimension (not from source tables)
-- ============================================================
CREATE OR REPLACE TABLE AIRFLOW0105.DEV.dim_Date_1 (
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
LEFT JOIN AIRFLOW0105.DEV.dim_Company_1 c ON swc.SYMBOL = c.symbol;

-- Incremental data for  dim_Company_1
MERGE INTO AIRFLOW0105.DEV.dim_Company_1 AS target
USING (
    SELECT DISTINCT
        u1.SYMBOL,
        u1.NAME AS company_name,
        u2.CEO,
        u2.SECTOR,
        u2.INDUSTRY,
        u1.EXCHANGE,
        u2.WEBSITE,
        u2.BETA,
        u2.MKTCAP,
        u2.DESCRIPTION
    FROM US_STOCK_DAILY.DCCM.SYMBOLS u1
    LEFT JOIN US_STOCK_DAILY.DCCM.COMPANY_PROFILE u2
        ON u1.SYMBOL = u2.SYMBOL
) AS source
ON target.symbol = source.SYMBOL
-- if mached then update
WHEN MATCHED THEN UPDATE SET
    target.company_name = source.company_name,
    target.ceo = source.CEO,
    target.sector = source.SECTOR,
    target.industry = source.INDUSTRY,
    target.exchange = source.EXCHANGE,
    target.website = source.WEBSITE,
    target.beta = source.BETA,
    target.mktcap = source.MKTCAP,
    target.description = source.DESCRIPTION
-- if not，insert new
WHEN NOT MATCHED THEN INSERT (symbol, company_name, ceo, sector, industry, exchange, website, beta, mktcap, description)
VALUES (source.SYMBOL, source.company_name, source.CEO, source.SECTOR, source.INDUSTRY, source.EXCHANGE, source.WEBSITE, source.BETA, source.MKTCAP, source.DESCRIPTION);

-- 增量加载 fact_Stock_Daily_1
-- 方法：只加载比目标表中最新日期更新的数据

MERGE INTO AIRFLOW0105.DEV.fact_Stock_Daily_1 AS target
USING (
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
            AVG(sh.CLOSE) OVER (
                PARTITION BY sh.SYMBOL 
                ORDER BY sh.DATE 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS ma_7,
            AVG(sh.CLOSE) OVER (
                PARTITION BY sh.SYMBOL 
                ORDER BY sh.DATE 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS ma_30,
            (sh.CLOSE - LAG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE)) 
                / NULLIF(LAG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE), 0) AS daily_return,
            sh.CLOSE - LAG(sh.CLOSE) OVER (PARTITION BY sh.SYMBOL ORDER BY sh.DATE) AS daily_change
        FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
        LEFT JOIN US_STOCK_DAILY.DCCM.COMPANY_PROFILE cp ON sh.SYMBOL = cp.SYMBOL
        -- 只选择新数据（比目标表最新日期更新的）
        WHERE sh.DATE > (SELECT COALESCE(MAX(d.full_date), '1900-01-01') 
                         FROM AIRFLOW0105.DEV.fact_Stock_Daily_1 f
                         JOIN AIRFLOW0105.DEV.dim_Date_1 d ON f.date_key = d.date_key)
    )
    SELECT
        d.date_key,
        c.company_key,
        swc.*
    FROM stock_with_calculations swc
    INNER JOIN AIRFLOW0105.DEV.dim_Date_1 d ON TO_NUMBER(TO_CHAR(swc.DATE, 'YYYYMMDD')) = d.date_key
    LEFT JOIN AIRFLOW0105.DEV.dim_Company_1 c ON swc.SYMBOL = c.symbol
) AS source
ON target.date_key = source.date_key AND target.company_key = source.company_key
-- 如果匹配到（同一天同一股票），更新数据
WHEN MATCHED THEN UPDATE SET
    target.open_price = source.OPEN,
    target.high_price = source.HIGH,
    target.low_price = source.LOW,
    target.close_price = source.CLOSE,
    target.adj_close = source.ADJCLOSE,
    target.volume = source.VOLUME,
    target.volavg = source.VOLAVG,
    target.changes = source.CHANGES,
    target.ma_7 = source.ma_7,
    target.ma_30 = source.ma_30,
    target.daily_return = source.daily_return,
    target.daily_change = source.daily_change
-- 如果没匹配到，插入新数据
WHEN NOT MATCHED THEN INSERT (date_key, company_key, open_price, high_price, low_price, close_price, adj_close, volume, volavg, changes, ma_7, ma_30, daily_return, daily_change)
VALUES (source.date_key, source.company_key, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.ADJCLOSE, source.VOLUME, source.VOLAVG, source.CHANGES, source.ma_7, source.ma_30, source.daily_return, source.daily_change);"""
    )
# ============================================================
# VALIDATION FUNCTION - Top 5 Critical Checks
# ============================================================
@task
def validate_stock_data_warehouse():
    """
    Validate the stock data warehouse after loading
    Includes 5 critical data quality checks
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    validation_results = {}
    
    # ============================================================
    # 1. ROW COUNT VALIDATION
    # ============================================================
    logging.info("=" * 60)
    logging.info("Validation 1/5: Row Count Check")
    logging.info("=" * 60)
    
    row_counts = hook.get_first("""
        SELECT 
            (SELECT COUNT(*) FROM AIRFLOW0105.DEV.dim_Date_1) as date_count,
            (SELECT COUNT(*) FROM AIRFLOW0105.DEV.dim_Company_1) as company_count,
            (SELECT COUNT(*) FROM AIRFLOW0105.DEV.fact_Stock_Daily_1) as fact_count
    """)
    
    validation_results['row_counts'] = {
        'dim_Date_1': row_counts[0],
        'dim_Company_1': row_counts[1],
        'fact_Stock_Daily_1': row_counts[2]
    }
    
    logging.info(f"✓ dim_Date_1: {row_counts[0]:,} rows")
    logging.info(f"✓ dim_Company_1: {row_counts[1]:,} rows")
    logging.info(f"✓ fact_Stock_Daily_1: {row_counts[2]:,} rows")
    
    assert row_counts[0] > 0, "❌ FAILED: Date dimension is empty!"
    assert row_counts[1] > 0, "❌ FAILED: Company dimension is empty!"
    assert row_counts[2] > 0, "❌ FAILED: Fact table is empty!"
    
    # ============================================================
    # 2. DATE DIMENSION VALIDATION
    # ============================================================
    logging.info("\n" + "=" * 60)
    logging.info("Validation 2/5: Date Dimension Integrity")
    logging.info("=" * 60)
    
    date_checks = hook.get_first("""
        SELECT 
            COUNT(DISTINCT date_key) as unique_dates,
            MIN(full_date) as min_date,
            MAX(full_date) as max_date,
            COUNT(CASE WHEN date_key != TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) THEN 1 END) as date_key_mismatch
        FROM AIRFLOW0105.DEV.dim_Date_1
    """)
    
    validation_results['date_dimension'] = {
        'unique_dates': date_checks[0],
        'min_date': str(date_checks[1]),
        'max_date': str(date_checks[2]),
        'date_key_mismatch': date_checks[3]
    }
    
    logging.info(f"✓ Unique dates: {date_checks[0]:,}")
    logging.info(f"✓ Date range: {date_checks[1]} to {date_checks[2]}")
    logging.info(f"✓ Date key format mismatches: {date_checks[3]}")
    
    assert date_checks[3] == 0, f"❌ FAILED: Found {date_checks[3]} date key format mismatches!"
    
    # ============================================================
    # 3. COMPANY DIMENSION VALIDATION
    # ============================================================
    logging.info("\n" + "=" * 60)
    logging.info("Validation 3/5: Company Dimension Uniqueness")
    logging.info("=" * 60)
    
    company_checks = hook.get_first("""
        SELECT 
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(*) - COUNT(DISTINCT symbol) as duplicate_symbols,
            COUNT(*) - COUNT(company_name) as null_company_names,
            COUNT(*) - COUNT(sector) as null_sectors
        FROM AIRFLOW0105.DEV.dim_Company_1
    """)
    
    validation_results['company_dimension'] = {
        'unique_symbols': company_checks[0],
        'duplicate_symbols': company_checks[1],
        'null_company_names': company_checks[2],
        'null_sectors': company_checks[3]
    }
    
    logging.info(f"✓ Unique symbols: {company_checks[0]:,}")
    logging.info(f"✓ Duplicate symbols: {company_checks[1]}")
    logging.info(f"✓ NULL company names: {company_checks[2]}")
    logging.info(f"✓ NULL sectors: {company_checks[3]}")
    
    assert company_checks[1] == 0, f"❌ FAILED: Found {company_checks[1]} duplicate symbols!"
    
    # ============================================================
    # 4. FACT TABLE VALIDATION
    # ============================================================
    logging.info("\n" + "=" * 60)
    logging.info("Validation 4/5: Fact Table Data Quality")
    logging.info("=" * 60)
    
    fact_checks = hook.get_first("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT date_key) as unique_dates,
            COUNT(DISTINCT company_key) as unique_companies,
            COUNT(*) - COUNT(company_key) as null_company_keys,
            COUNT(CASE WHEN open_price IS NULL OR close_price IS NULL THEN 1 END) as null_prices,
            COUNT(CASE WHEN volume < 0 THEN 1 END) as negative_volumes,
            COUNT(CASE WHEN close_price < 0 THEN 1 END) as negative_prices
        FROM AIRFLOW0105.DEV.fact_Stock_Daily_1
    """)
    
    validation_results['fact_table'] = {
        'total_records': fact_checks[0],
        'unique_dates': fact_checks[1],
        'unique_companies': fact_checks[2],
        'null_company_keys': fact_checks[3],
        'null_prices': fact_checks[4],
        'negative_volumes': fact_checks[5],
        'negative_prices': fact_checks[6]
    }
    
    logging.info(f"✓ Total records: {fact_checks[0]:,}")
    logging.info(f"✓ Unique dates: {fact_checks[1]:,}")
    logging.info(f"✓ Unique companies: {fact_checks[2]:,}")
    logging.info(f"✓ NULL company keys: {fact_checks[3]:,}")
    logging.info(f"✓ NULL prices: {fact_checks[4]}")
    logging.info(f"✓ Negative volumes: {fact_checks[5]}")
    logging.info(f"✓ Negative prices: {fact_checks[6]}")
    
    assert fact_checks[4] == 0, f"❌ FAILED: Found {fact_checks[4]} records with NULL prices!"
    assert fact_checks[5] == 0, f"❌ FAILED: Found {fact_checks[5]} records with negative volumes!"
    assert fact_checks[6] == 0, f"❌ FAILED: Found {fact_checks[6]} records with negative prices!"
    
    # ============================================================
    # 5. REFERENTIAL INTEGRITY CHECK
    # ============================================================
    logging.info("\n" + "=" * 60)
    logging.info("Validation 5/5: Referential Integrity")
    logging.info("=" * 60)
    
    orphan_dates = hook.get_first("""
        SELECT COUNT(*)
        FROM AIRFLOW0105.DEV.fact_Stock_Daily_1 f
        LEFT JOIN AIRFLOW0105.DEV.dim_Date_1 d ON f.date_key = d.date_key
        WHERE d.date_key IS NULL
    """)
    
    orphan_companies = hook.get_first("""
        SELECT COUNT(*)
        FROM AIRFLOW0105.DEV.fact_Stock_Daily_1 f
        LEFT JOIN AIRFLOW0105.DEV.dim_Company_1 c ON f.company_key = c.company_key
        WHERE f.company_key IS NOT NULL AND c.company_key IS NULL
    """)
    
    validation_results['referential_integrity'] = {
        'orphan_dates': orphan_dates[0],
        'orphan_companies': orphan_companies[0]
    }
    
    logging.info(f"✓ Orphan date keys: {orphan_dates[0]}")
    logging.info(f"✓ Orphan company keys: {orphan_companies[0]}")
    
    assert orphan_dates[0] == 0, f"❌ FAILED: Found {orphan_dates[0]} orphan date keys!"
    assert orphan_companies[0] == 0, f"❌ FAILED: Found {orphan_companies[0]} orphan company keys!"
    
    # ============================================================
    # FINAL SUMMARY
    # ============================================================
    logging.info("\n" + "=" * 60)
    logging.info("✅ ALL 5 VALIDATIONS PASSED!")
    logging.info("=" * 60)
    
    return validation_results


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id=f"project1_stock_dimensional_etl_{GROUP_NUM}",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["project1", "snowflake", "stock", "team1", 'etl', 'dwh'],
) as dag:

    load_fact = SnowflakeOperator(
        task_id="load_fact",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""-- Your existing ETL SQL here
        -- (Keep your original SQL from the document)
        """
    )

    
    taskflow_validate = validate_stock_data_warehouse()

    load_fact >> taskflow_validate
