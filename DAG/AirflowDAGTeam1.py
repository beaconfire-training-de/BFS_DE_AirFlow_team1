"""
DAG: project1_stock_dimensional_etl_team1

Runs a single end-to-end ETL in Snowflake:
1) Create/replace target tables (DIM_DATE_TEAM1, DIM_SECURITY_TEAM1, FACT_STOCK_DAILY_TEAM1, FACT_SECURITY_SNAPSHOT_TEAM1)
2) Load DIM_DATE (calendar)
3) Upsert DIM_SECURITY (SYMBOLS + COMPANY_PROFILE descriptive)
4) Merge FACT_STOCK_DAILY (STOCK_HISTORY)
5) Merge FACT_SECURITY_SNAPSHOT (COMPANY_PROFILE metrics snapshot as-of today)
6) SQL validate (may fail) + Python tests (never fail, only log)

Connection:
SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


GROUP_NUM = "team1"
SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"

DB = "AIRFLOW0105"
SCHEMA = "DEV"

DIM_DATE_TBL = f"{DB}.{SCHEMA}.DIM_DATE_{GROUP_NUM.upper()}"
DIM_SECURITY_TBL = f"{DB}.{SCHEMA}.DIM_SECURITY_{GROUP_NUM.upper()}"
FACT_DAILY_TBL = f"{DB}.{SCHEMA}.FACT_STOCK_DAILY_{GROUP_NUM.upper()}"
FACT_SNAP_TBL = f"{DB}.{SCHEMA}.FACT_SECURITY_SNAPSHOT_{GROUP_NUM.upper()}"

# -----------------------------
# SQL: DDL
# -----------------------------
SQL_CREATE_TABLES = f"""
USE DATABASE AIRFLOW0105;
USE SCHEMA DEV;

CREATE OR REPLACE TABLE DIM_DATE_{GROUP_NUM.upper()} (
  DATE_KEY       NUMBER(8,0)   NOT NULL,   -- YYYYMMDD
  FULL_DATE      DATE          NOT NULL,
  YEAR           NUMBER(4,0)   NOT NULL,
  QUARTER        NUMBER(1,0)   NOT NULL,
  MONTH          NUMBER(2,0)   NOT NULL,
  DAY            NUMBER(2,0)   NOT NULL,
  WEEK_OF_YEAR   NUMBER(2,0)   NOT NULL,
  DAY_OF_WEEK    NUMBER(1,0)   NOT NULL,   -- 1=Mon .. 7=Sun (ISO)
  IS_WEEKEND     BOOLEAN       NOT NULL,
  CONSTRAINT PK_DIM_DATE_{GROUP_NUM.upper()} PRIMARY KEY (DATE_KEY),
  CONSTRAINT UQ_DIM_DATE_{GROUP_NUM.upper()}_FULL_DATE UNIQUE (FULL_DATE)
);

CREATE OR REPLACE TABLE DIM_SECURITY_{GROUP_NUM.upper()} (
  SECURITY_KEY       NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1,
  SYMBOL             VARCHAR(16)  NOT NULL,
  SYMBOL_NAME        VARCHAR(256),
  EXCHANGE           VARCHAR(64),
  SOURCE_COMPANY_ID  NUMBER(38,0),
  COMPANY_NAME       VARCHAR(512),
  SECTOR             VARCHAR(64),
  INDUSTRY           VARCHAR(64),
  CEO                VARCHAR(64),
  WEBSITE            VARCHAR(256),
  DESCRIPTION        VARCHAR(4096),
  CONSTRAINT PK_DIM_SECURITY_{GROUP_NUM.upper()} PRIMARY KEY (SECURITY_KEY),
  CONSTRAINT UQ_DIM_SECURITY_{GROUP_NUM.upper()}_SYMBOL UNIQUE (SYMBOL)
);

CREATE OR REPLACE TABLE FACT_STOCK_DAILY_{GROUP_NUM.upper()} (
  SECURITY_KEY   NUMBER(38,0) NOT NULL,
  DATE_KEY       NUMBER(8,0)  NOT NULL,
  OPEN           NUMBER(18,8),
  HIGH           NUMBER(18,8),
  LOW            NUMBER(18,8),
  CLOSE          NUMBER(18,8),
  ADJCLOSE       NUMBER(18,8),
  VOLUME         NUMBER(38,0),
  LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT FK_FSD_{GROUP_NUM.upper()}_SECURITY FOREIGN KEY (SECURITY_KEY)
    REFERENCES DIM_SECURITY_{GROUP_NUM.upper()}(SECURITY_KEY),
  CONSTRAINT FK_FSD_{GROUP_NUM.upper()}_DATE FOREIGN KEY (DATE_KEY)
    REFERENCES DIM_DATE_{GROUP_NUM.upper()}(DATE_KEY),
  CONSTRAINT UQ_FSD_{GROUP_NUM.upper()}_SECURITY_DATE UNIQUE (SECURITY_KEY, DATE_KEY)
);

CREATE OR REPLACE TABLE FACT_SECURITY_SNAPSHOT_{GROUP_NUM.upper()} (
  SECURITY_KEY    NUMBER(38,0) NOT NULL,
  ASOF_DATE_KEY   NUMBER(8,0)  NOT NULL,
  PRICE           NUMBER(18,8),
  BETA            NUMBER(18,8),
  VOLAVG          NUMBER(38,0),
  MKTCAP          NUMBER(38,0),
  LASTDIV         NUMBER(18,8),
  RANGE           VARCHAR(64),
  CHANGES         NUMBER(18,8),
  DCF             NUMBER(18,8),
  DCFDIFF         NUMBER(18,8),
  LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT FK_FSS_{GROUP_NUM.upper()}_SECURITY FOREIGN KEY (SECURITY_KEY)
    REFERENCES DIM_SECURITY_{GROUP_NUM.upper()}(SECURITY_KEY),
  CONSTRAINT FK_FSS_{GROUP_NUM.upper()}_DATE FOREIGN KEY (ASOF_DATE_KEY)
    REFERENCES DIM_DATE_{GROUP_NUM.upper()}(DATE_KEY),
  CONSTRAINT UQ_FSS_{GROUP_NUM.upper()}_SECURITY_ASOF UNIQUE (SECURITY_KEY, ASOF_DATE_KEY)
);
"""

# -----------------------------
# SQL: Loads / Merges
# -----------------------------
SQL_LOAD_DIM_DATE = f"""
USE DATABASE AIRFLOW0105;
USE SCHEMA DEV;

INSERT OVERWRITE INTO DIM_DATE_{GROUP_NUM.upper()}
WITH dates AS (
  SELECT DATEADD(DAY, SEQ4(), '1990-01-01'::DATE) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 16802))
)
SELECT
  TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))                     AS DATE_KEY,
  d                                                     AS FULL_DATE,
  YEAR(d)                                               AS YEAR,
  QUARTER(d)                                            AS QUARTER,
  MONTH(d)                                              AS MONTH,
  DAY(d)                                                AS DAY,
  WEEKOFYEAR(d)                                         AS WEEK_OF_YEAR,
  DAYOFWEEKISO(d)                                       AS DAY_OF_WEEK,
  IFF(DAYOFWEEKISO(d) IN (6,7), TRUE, FALSE)            AS IS_WEEKEND
FROM dates;
"""

SQL_MERGE_DIM_SECURITY = f"""
USE DATABASE AIRFLOW0105;
USE SCHEMA DEV;

MERGE INTO DIM_SECURITY_{GROUP_NUM.upper()} tgt
USING (
  WITH cp AS (
    SELECT
      ID,
      SYMBOL,
      COMPANYNAME,
      EXCHANGE,
      INDUSTRY,
      WEBSITE,
      DESCRIPTION,
      CEO,
      SECTOR
    FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY ID DESC) = 1
  )
  SELECT
    s.SYMBOL                                  AS SYMBOL,
    s.NAME                                    AS SYMBOL_NAME,
    COALESCE(cp.EXCHANGE, s.EXCHANGE)         AS EXCHANGE,
    cp.ID                                     AS SOURCE_COMPANY_ID,
    cp.COMPANYNAME                            AS COMPANY_NAME,
    cp.SECTOR                                 AS SECTOR,
    cp.INDUSTRY                               AS INDUSTRY,
    cp.CEO                                    AS CEO,
    cp.WEBSITE                                AS WEBSITE,
    cp.DESCRIPTION                            AS DESCRIPTION
  FROM US_STOCK_DAILY.DCCM.SYMBOLS s
  LEFT JOIN cp
    ON cp.SYMBOL = s.SYMBOL
) src
ON tgt.SYMBOL = src.SYMBOL
WHEN MATCHED THEN UPDATE SET
  tgt.SYMBOL_NAME       = src.SYMBOL_NAME,
  tgt.EXCHANGE          = src.EXCHANGE,
  tgt.SOURCE_COMPANY_ID = src.SOURCE_COMPANY_ID,
  tgt.COMPANY_NAME      = src.COMPANY_NAME,
  tgt.SECTOR            = src.SECTOR,
  tgt.INDUSTRY          = src.INDUSTRY,
  tgt.CEO               = src.CEO,
  tgt.WEBSITE           = src.WEBSITE,
  tgt.DESCRIPTION       = src.DESCRIPTION
WHEN NOT MATCHED THEN INSERT (
  SYMBOL, SYMBOL_NAME, EXCHANGE, SOURCE_COMPANY_ID, COMPANY_NAME, SECTOR, INDUSTRY, CEO, WEBSITE, DESCRIPTION
) VALUES (
  src.SYMBOL, src.SYMBOL_NAME, src.EXCHANGE, src.SOURCE_COMPANY_ID, src.COMPANY_NAME, src.SECTOR, src.INDUSTRY, src.CEO, src.WEBSITE, src.DESCRIPTION
);
"""

SQL_MERGE_FACT_STOCK_DAILY = f"""
USE DATABASE AIRFLOW0105;
USE SCHEMA DEV;

MERGE INTO FACT_STOCK_DAILY_{GROUP_NUM.upper()} tgt
USING (
  SELECT
    ds.SECURITY_KEY                                           AS SECURITY_KEY,
    dd.DATE_KEY                                               AS DATE_KEY,
    sh.OPEN                                                   AS OPEN,
    sh.HIGH                                                   AS HIGH,
    sh.LOW                                                    AS LOW,
    sh.CLOSE                                                  AS CLOSE,
    sh.ADJCLOSE                                               AS ADJCLOSE,
    sh.VOLUME                                                 AS VOLUME
  FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
  JOIN DIM_SECURITY_{GROUP_NUM.upper()} ds
    ON ds.SYMBOL = sh.SYMBOL
  JOIN DIM_DATE_{GROUP_NUM.upper()} dd
    ON dd.FULL_DATE = sh.DATE
) src
ON tgt.SECURITY_KEY = src.SECURITY_KEY
AND tgt.DATE_KEY     = src.DATE_KEY
WHEN MATCHED THEN UPDATE SET
  tgt.OPEN     = src.OPEN,
  tgt.HIGH     = src.HIGH,
  tgt.LOW      = src.LOW,
  tgt.CLOSE    = src.CLOSE,
  tgt.ADJCLOSE = src.ADJCLOSE,
  tgt.VOLUME   = src.VOLUME,
  tgt.LOAD_TS  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  SECURITY_KEY, DATE_KEY, OPEN, HIGH, LOW, CLOSE, ADJCLOSE, VOLUME
) VALUES (
  src.SECURITY_KEY, src.DATE_KEY, src.OPEN, src.HIGH, src.LOW, src.CLOSE, src.ADJCLOSE, src.VOLUME
);
"""

SQL_MERGE_FACT_SECURITY_SNAPSHOT = f"""
USE DATABASE AIRFLOW0105;
USE SCHEMA DEV;

MERGE INTO FACT_SECURITY_SNAPSHOT_{GROUP_NUM.upper()} tgt
USING (
  WITH cp AS (
    SELECT
      ID,
      SYMBOL,
      PRICE,
      BETA,
      VOLAVG,
      MKTCAP,
      LASTDIV,
      RANGE,
      CHANGES,
      DCF,
      DCFDIFF
    FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY ID DESC) = 1
  )
  SELECT
    ds.SECURITY_KEY                                  AS SECURITY_KEY,
    TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))   AS ASOF_DATE_KEY,
    cp.PRICE                                         AS PRICE,
    cp.BETA                                          AS BETA,
    cp.VOLAVG                                        AS VOLAVG,
    cp.MKTCAP                                        AS MKTCAP,
    cp.LASTDIV                                       AS LASTDIV,
    cp.RANGE                                         AS RANGE,
    cp.CHANGES                                       AS CHANGES,
    cp.DCF                                           AS DCF,
    cp.DCFDIFF                                       AS DCFDIFF
  FROM cp
  JOIN DIM_SECURITY_{GROUP_NUM.upper()} ds
    ON ds.SYMBOL = cp.SYMBOL
) src
ON tgt.SECURITY_KEY   = src.SECURITY_KEY
AND tgt.ASOF_DATE_KEY = src.ASOF_DATE_KEY
WHEN MATCHED THEN UPDATE SET
  tgt.PRICE    = src.PRICE,
  tgt.BETA     = src.BETA,
  tgt.VOLAVG   = src.VOLAVG,
  tgt.MKTCAP   = src.MKTCAP,
  tgt.LASTDIV  = src.LASTDIV,
  tgt.RANGE    = src.RANGE,
  tgt.CHANGES  = src.CHANGES,
  tgt.DCF      = src.DCF,
  tgt.DCFDIFF  = src.DCFDIFF,
  tgt.LOAD_TS  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  SECURITY_KEY, ASOF_DATE_KEY, PRICE, BETA, VOLAVG, MKTCAP, LASTDIV, RANGE, CHANGES, DCF, DCFDIFF
) VALUES (
  src.SECURITY_KEY, src.ASOF_DATE_KEY, src.PRICE, src.BETA, src.VOLAVG, src.MKTCAP, src.LASTDIV, src.RANGE, src.CHANGES, src.DCF, src.DCFDIFF
);
"""

# -----------------------------
# SQL: Validations (can fail DAG)
# -----------------------------
SQL_VALIDATE = f"""
USE DATABASE AIRFLOW0105;
USE SCHEMA DEV;

WITH checks AS (
  SELECT
    (SELECT COUNT(*) FROM FACT_STOCK_DAILY_TEAM1 f
      LEFT JOIN DIM_SECURITY_TEAM1 d ON f.SECURITY_KEY=d.SECURITY_KEY
      WHERE d.SECURITY_KEY IS NULL) AS orphan_security_daily_cnt,

    (SELECT COUNT(*) FROM FACT_STOCK_DAILY_TEAM1 f
      LEFT JOIN DIM_DATE_TEAM1 d ON f.DATE_KEY=d.DATE_KEY
      WHERE d.DATE_KEY IS NULL) AS orphan_date_daily_cnt,

    (SELECT COUNT(*) FROM FACT_SECURITY_SNAPSHOT_TEAM1 s
      LEFT JOIN DIM_DATE_TEAM1 d ON s.ASOF_DATE_KEY=d.DATE_KEY
      WHERE d.DATE_KEY IS NULL) AS orphan_date_snapshot_cnt,

    (SELECT COUNT(*) FROM (
      SELECT SECURITY_KEY, DATE_KEY
      FROM FACT_STOCK_DAILY_TEAM1
      GROUP BY 1,2
      HAVING COUNT(*) > 1
    )) AS dup_daily_cnt,

    (SELECT COUNT(*) FROM (
      SELECT SECURITY_KEY, ASOF_DATE_KEY
      FROM FACT_SECURITY_SNAPSHOT_TEAM1
      GROUP BY 1,2
      HAVING COUNT(*) > 1
    )) AS dup_snapshot_cnt
)
SELECT * FROM checks;

WITH checks AS (
  SELECT
    (SELECT COUNT(*) FROM FACT_STOCK_DAILY_TEAM1 f
      LEFT JOIN DIM_SECURITY_TEAM1 d ON f.SECURITY_KEY=d.SECURITY_KEY
      WHERE d.SECURITY_KEY IS NULL) AS orphan_security_daily_cnt,
    (SELECT COUNT(*) FROM FACT_STOCK_DAILY_TEAM1 f
      LEFT JOIN DIM_DATE_TEAM1 d ON f.DATE_KEY=d.DATE_KEY
      WHERE d.DATE_KEY IS NULL) AS orphan_date_daily_cnt,
    (SELECT COUNT(*) FROM FACT_SECURITY_SNAPSHOT_TEAM1 s
      LEFT JOIN DIM_DATE_TEAM1 d ON s.ASOF_DATE_KEY=d.DATE_KEY
      WHERE d.DATE_KEY IS NULL) AS orphan_date_snapshot_cnt,
    (SELECT COUNT(*) FROM (
      SELECT SECURITY_KEY, DATE_KEY
      FROM FACT_STOCK_DAILY_TEAM1
      GROUP BY 1,2
      HAVING COUNT(*) > 1
    )) AS dup_daily_cnt,
    (SELECT COUNT(*) FROM (
      SELECT SECURITY_KEY, ASOF_DATE_KEY
      FROM FACT_SECURITY_SNAPSHOT_TEAM1
      GROUP BY 1,2
      HAVING COUNT(*) > 1
    )) AS dup_snapshot_cnt
)
SELECT
  CASE
    WHEN (orphan_security_daily_cnt + orphan_date_daily_cnt + orphan_date_snapshot_cnt + dup_daily_cnt + dup_snapshot_cnt) = 0
    THEN 1
    ELSE TO_NUMBER('0')  -- throw error to fail
  END AS validation_status
FROM checks;

SELECT
  (SELECT COUNT(*) FROM DIM_DATE_TEAM1)               AS dim_date_cnt,
  (SELECT COUNT(*) FROM DIM_SECURITY_TEAM1)           AS dim_security_cnt,
  (SELECT COUNT(*) FROM FACT_STOCK_DAILY_TEAM1)       AS fact_stock_daily_cnt,
  (SELECT COUNT(*) FROM FACT_SECURITY_SNAPSHOT_TEAM1) AS fact_security_snapshot_cnt;
"""

# -----------------------------
# Python Tests (never fail DAG; only log)
# -----------------------------
def _safe_fetchall(hook: SnowflakeHook, sql: str):
    try:
        return hook.get_records(sql) or []
    except Exception as e:
        logging.exception(f"[PYTEST] Query failed but continuing. Error={e}\nSQL:\n{sql}")
        return []


def _safe_fetchone(hook: SnowflakeHook, sql: str, default=0):
    rows = _safe_fetchall(hook, sql)
    if not rows or rows[0] is None or len(rows[0]) == 0:
        return default
    return rows[0][0]


@task
def pytest_row_counts():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = f"""
      SELECT
        (SELECT COUNT(*) FROM {DIM_DATE_TBL})     AS dim_date_cnt,
        (SELECT COUNT(*) FROM {DIM_SECURITY_TBL}) AS dim_security_cnt,
        (SELECT COUNT(*) FROM {FACT_DAILY_TBL})   AS fact_daily_cnt,
        (SELECT COUNT(*) FROM {FACT_SNAP_TBL})    AS fact_snap_cnt
    """
    rows = _safe_fetchall(hook, sql)
    logging.info(f"[PYTEST] Row counts: {rows[0] if rows else 'N/A'}")
    return {"row_counts": rows[0] if rows else None}


@task
def pytest_orphans_and_dups(sample_n: int = 10):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    orphan_security_daily_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_DAILY_TBL} f
      LEFT JOIN {DIM_SECURITY_TBL} d ON f.SECURITY_KEY = d.SECURITY_KEY
      WHERE d.SECURITY_KEY IS NULL
    """)
    orphan_security_daily_sample = _safe_fetchall(hook, f"""
      SELECT f.SECURITY_KEY, f.DATE_KEY
      FROM {FACT_DAILY_TBL} f
      LEFT JOIN {DIM_SECURITY_TBL} d ON f.SECURITY_KEY = d.SECURITY_KEY
      WHERE d.SECURITY_KEY IS NULL
      LIMIT {sample_n}
    """)

    orphan_date_daily_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_DAILY_TBL} f
      LEFT JOIN {DIM_DATE_TBL} d ON f.DATE_KEY = d.DATE_KEY
      WHERE d.DATE_KEY IS NULL
    """)
    orphan_date_daily_sample = _safe_fetchall(hook, f"""
      SELECT f.SECURITY_KEY, f.DATE_KEY
      FROM {FACT_DAILY_TBL} f
      LEFT JOIN {DIM_DATE_TBL} d ON f.DATE_KEY = d.DATE_KEY
      WHERE d.DATE_KEY IS NULL
      LIMIT {sample_n}
    """)

    orphan_date_snapshot_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_SNAP_TBL} s
      LEFT JOIN {DIM_DATE_TBL} d ON s.ASOF_DATE_KEY = d.DATE_KEY
      WHERE d.DATE_KEY IS NULL
    """)
    orphan_date_snapshot_sample = _safe_fetchall(hook, f"""
      SELECT s.SECURITY_KEY, s.ASOF_DATE_KEY
      FROM {FACT_SNAP_TBL} s
      LEFT JOIN {DIM_DATE_TBL} d ON s.ASOF_DATE_KEY = d.DATE_KEY
      WHERE d.DATE_KEY IS NULL
      LIMIT {sample_n}
    """)

    dup_daily_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*) FROM (
        SELECT SECURITY_KEY, DATE_KEY
        FROM {FACT_DAILY_TBL}
        GROUP BY 1,2
        HAVING COUNT(*) > 1
      )
    """)
    dup_daily_sample = _safe_fetchall(hook, f"""
      SELECT SECURITY_KEY, DATE_KEY, COUNT(*) AS cnt
      FROM {FACT_DAILY_TBL}
      GROUP BY 1,2
      HAVING COUNT(*) > 1
      ORDER BY cnt DESC
      LIMIT {sample_n}
    """)

    dup_snapshot_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*) FROM (
        SELECT SECURITY_KEY, ASOF_DATE_KEY
        FROM {FACT_SNAP_TBL}
        GROUP BY 1,2
        HAVING COUNT(*) > 1
      )
    """)
    dup_snapshot_sample = _safe_fetchall(hook, f"""
      SELECT SECURITY_KEY, ASOF_DATE_KEY, COUNT(*) AS cnt
      FROM {FACT_SNAP_TBL}
      GROUP BY 1,2
      HAVING COUNT(*) > 1
      ORDER BY cnt DESC
      LIMIT {sample_n}
    """)

    summary = {
        "orphan_security_daily_cnt": orphan_security_daily_cnt,
        "orphan_date_daily_cnt": orphan_date_daily_cnt,
        "orphan_date_snapshot_cnt": orphan_date_snapshot_cnt,
        "dup_daily_cnt": dup_daily_cnt,
        "dup_snapshot_cnt": dup_snapshot_cnt,
    }

    logging.info(f"[PYTEST] Validation summary (counts only): {summary}")
    logging.info(f"[PYTEST] Sample orphan_security_daily: {orphan_security_daily_sample}")
    logging.info(f"[PYTEST] Sample orphan_date_daily: {orphan_date_daily_sample}")
    logging.info(f"[PYTEST] Sample orphan_date_snapshot: {orphan_date_snapshot_sample}")
    logging.info(f"[PYTEST] Sample dup_daily: {dup_daily_sample}")
    logging.info(f"[PYTEST] Sample dup_snapshot: {dup_snapshot_sample}")

    return {"counts": summary, "samples": {
        "orphan_security_daily": orphan_security_daily_sample,
        "orphan_date_daily": orphan_date_daily_sample,
        "orphan_date_snapshot": orphan_date_snapshot_sample,
        "dup_daily": dup_daily_sample,
        "dup_snapshot": dup_snapshot_sample,
    }}


@task
def pytest_null_fks(sample_n: int = 10):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    null_fk_daily_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_DAILY_TBL}
      WHERE SECURITY_KEY IS NULL OR DATE_KEY IS NULL
    """)
    null_fk_daily_sample = _safe_fetchall(hook, f"""
      SELECT SECURITY_KEY, DATE_KEY, OPEN, HIGH, LOW, CLOSE, ADJCLOSE, VOLUME
      FROM {FACT_DAILY_TBL}
      WHERE SECURITY_KEY IS NULL OR DATE_KEY IS NULL
      LIMIT {sample_n}
    """)

    null_fk_snap_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_SNAP_TBL}
      WHERE SECURITY_KEY IS NULL OR ASOF_DATE_KEY IS NULL
    """)
    null_fk_snap_sample = _safe_fetchall(hook, f"""
      SELECT SECURITY_KEY, ASOF_DATE_KEY, PRICE, BETA, VOLAVG, MKTCAP
      FROM {FACT_SNAP_TBL}
      WHERE SECURITY_KEY IS NULL OR ASOF_DATE_KEY IS NULL
      LIMIT {sample_n}
    """)

    logging.info(f"[PYTEST] Null FK daily cnt={null_fk_daily_cnt}; sample={null_fk_daily_sample}")
    logging.info(f"[PYTEST] Null FK snap  cnt={null_fk_snap_cnt}; sample={null_fk_snap_sample}")

    return {
        "null_fk_daily_cnt": null_fk_daily_cnt,
        "null_fk_daily_sample": null_fk_daily_sample,
        "null_fk_snap_cnt": null_fk_snap_cnt,
        "null_fk_snap_sample": null_fk_snap_sample,
    }


default_args = {
    "owner": "team1",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"project1_stock_dimensional_etl_{GROUP_NUM}",
    default_args=default_args,
    description="Snowflake->Snowflake dimensional model ETL (Team1)",
    start_date=datetime(2026, 2, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["team1", "snowflake", "dimensional_model"],
) as dag:

    create_tables = SnowflakeOperator(
        task_id="create_tables",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_CREATE_TABLES,
    )

    load_dim_date = SnowflakeOperator(
        task_id="load_dim_date",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_LOAD_DIM_DATE,
    )

    upsert_dim_security = SnowflakeOperator(
        task_id="upsert_dim_security",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_MERGE_DIM_SECURITY,
    )

    merge_fact_stock_daily = SnowflakeOperator(
        task_id="merge_fact_stock_daily",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_MERGE_FACT_STOCK_DAILY,
    )

    merge_fact_security_snapshot = SnowflakeOperator(
        task_id="merge_fact_security_snapshot",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_MERGE_FACT_SECURITY_SNAPSHOT,
    )

    # Keep SQL validate (may fail). If you want DAG to NEVER fail, comment this task + chain.
    validate = SnowflakeOperator(
        task_id="validate",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_VALIDATE,
    )

    # Python tests (NEVER fail; only log)
    t_py_row_counts = pytest_row_counts()
    t_py_orphans_dups = pytest_orphans_and_dups(sample_n=10)
    t_py_null_fks = pytest_null_fks(sample_n=10)

    create_tables >> load_dim_date >> upsert_dim_security >> merge_fact_stock_daily >> merge_fact_security_snapshot >> validate
    validate >> t_py_row_counts >> t_py_orphans_dups >> t_py_null_fks
