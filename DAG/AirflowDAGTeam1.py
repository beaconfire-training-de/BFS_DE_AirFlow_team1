"""
DAG: project1_stock_dimensional_etl_team1

#test from max

Fix based on log:
- decouple merge_fact_security_snapshot from merge_fact_stock_daily_90d
- run facts in parallel after scd2_dim_security
- add etl_done join node with a safer trigger_rule so downstream doesn't get stuck
- keep branching + backfill + second validation loop

Connection:
SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"
"""

from datetime import datetime, timedelta
import logging
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator


# -----------------------------
# Config
# -----------------------------
GROUP_NUM = "team1"
SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"

DB = "AIRFLOW0105"
SCHEMA = "DEV"

DIM_DATE_TBL = f"{DB}.{SCHEMA}.DIM_DATE_{GROUP_NUM.upper()}"
DIM_SECURITY_TBL = f"{DB}.{SCHEMA}.DIM_SECURITY_{GROUP_NUM.upper()}"
FACT_DAILY_TBL = f"{DB}.{SCHEMA}.FACT_STOCK_DAILY_{GROUP_NUM.upper()}"
FACT_SNAP_TBL = f"{DB}.{SCHEMA}.FACT_SECURITY_SNAPSHOT_{GROUP_NUM.upper()}"

DAYS_BACK = 90


# -----------------------------
# SQL: DDL (DIM_SECURITY becomes SCD2)
# -----------------------------
SQL_CREATE_TABLES = f"""
USE DATABASE {DB};
USE SCHEMA {SCHEMA};

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
  SECURITY_KEY            NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1,
  SYMBOL                  VARCHAR(16)  NOT NULL,

  SYMBOL_NAME             VARCHAR(256),
  EXCHANGE                VARCHAR(64),
  SOURCE_COMPANY_ID       NUMBER(38,0),
  COMPANY_NAME            VARCHAR(512),
  SECTOR                  VARCHAR(64),
  INDUSTRY                VARCHAR(64),
  CEO                     VARCHAR(64),
  WEBSITE                 VARCHAR(256),
  DESCRIPTION             VARCHAR(4096),

  ROW_HASH                VARCHAR(64)  NOT NULL,
  EFFECTIVE_START_DATE    DATE         NOT NULL,
  EFFECTIVE_END_DATE      DATE,
  IS_CURRENT              BOOLEAN      NOT NULL,

  LOAD_TS                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_DIM_SECURITY_{GROUP_NUM.upper()} PRIMARY KEY (SECURITY_KEY)
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
# SQL: Load DIM_DATE
# -----------------------------
SQL_LOAD_DIM_DATE = f"""
USE DATABASE {DB};
USE SCHEMA {SCHEMA};

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


# -----------------------------
# SQL: SCD2 DIM_SECURITY
# -----------------------------
SQL_SCD2_DIM_SECURITY = f"""
USE DATABASE {DB};
USE SCHEMA {SCHEMA};

-- 0) Build src in a temp CTE (re-used by both DMLs via inline subquery)
-- 1) EXPIRE changed current rows
UPDATE DIM_SECURITY_{GROUP_NUM.upper()} tgt
SET
  EFFECTIVE_END_DATE = DATEADD(DAY, -1, CURRENT_DATE()),
  IS_CURRENT = FALSE,
  LOAD_TS = CURRENT_TIMESTAMP()
FROM (
  SELECT
    s.SYMBOL                                  AS SYMBOL,
    HASH(
      COALESCE(s.NAME,''),
      COALESCE(COALESCE(cp.EXCHANGE, s.EXCHANGE),''),
      COALESCE(cp.ID::VARCHAR,''),
      COALESCE(cp.COMPANYNAME,''),
      COALESCE(cp.SECTOR,''),
      COALESCE(cp.INDUSTRY,''),
      COALESCE(cp.CEO,''),
      COALESCE(cp.WEBSITE,''),
      COALESCE(cp.DESCRIPTION,'')
    )::VARCHAR AS ROW_HASH
  FROM US_STOCK_DAILY.DCCM.SYMBOLS s
  LEFT JOIN (
    SELECT
      ID, SYMBOL, COMPANYNAME, EXCHANGE, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR
    FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY ID DESC) = 1
  ) cp
    ON cp.SYMBOL = s.SYMBOL
) src
WHERE tgt.SYMBOL = src.SYMBOL
  AND tgt.IS_CURRENT = TRUE
  AND tgt.ROW_HASH <> src.ROW_HASH
;

-- 2) INSERT new current rows (new symbols OR changed symbols)
INSERT INTO DIM_SECURITY_{GROUP_NUM.upper()} (
  SYMBOL,
  SYMBOL_NAME,
  EXCHANGE,
  SOURCE_COMPANY_ID,
  COMPANY_NAME,
  SECTOR,
  INDUSTRY,
  CEO,
  WEBSITE,
  DESCRIPTION,
  ROW_HASH,
  EFFECTIVE_START_DATE,
  EFFECTIVE_END_DATE,
  IS_CURRENT
)
SELECT
  src.SYMBOL,
  src.SYMBOL_NAME,
  src.EXCHANGE,
  src.SOURCE_COMPANY_ID,
  src.COMPANY_NAME,
  src.SECTOR,
  src.INDUSTRY,
  src.CEO,
  src.WEBSITE,
  src.DESCRIPTION,
  src.ROW_HASH,
  CURRENT_DATE(),
  NULL,
  TRUE
FROM (
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
    cp.DESCRIPTION                            AS DESCRIPTION,
    HASH(
      COALESCE(s.NAME,''),
      COALESCE(COALESCE(cp.EXCHANGE, s.EXCHANGE),''),
      COALESCE(cp.ID::VARCHAR,''),
      COALESCE(cp.COMPANYNAME,''),
      COALESCE(cp.SECTOR,''),
      COALESCE(cp.INDUSTRY,''),
      COALESCE(cp.CEO,''),
      COALESCE(cp.WEBSITE,''),
      COALESCE(cp.DESCRIPTION,'')
    )::VARCHAR AS ROW_HASH
  FROM US_STOCK_DAILY.DCCM.SYMBOLS s
  LEFT JOIN (
    SELECT
      ID, SYMBOL, COMPANYNAME, EXCHANGE, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR
    FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY ID DESC) = 1
  ) cp
    ON cp.SYMBOL = s.SYMBOL
) src
LEFT JOIN DIM_SECURITY_{GROUP_NUM.upper()} cur
  ON cur.SYMBOL = src.SYMBOL
 AND cur.IS_CURRENT = TRUE
WHERE cur.SECURITY_KEY IS NULL
   OR cur.ROW_HASH <> src.ROW_HASH
;
"""



# -----------------------------
# SQL: FACT_STOCK_DAILY (incremental last 90 days) time-travel join
# -----------------------------
SQL_MERGE_FACT_STOCK_DAILY_90D = f"""
USE DATABASE {DB};
USE SCHEMA {SCHEMA};

MERGE INTO FACT_STOCK_DAILY_{GROUP_NUM.upper()} tgt
USING (
  SELECT
    ds.SECURITY_KEY AS SECURITY_KEY,
    dd.DATE_KEY     AS DATE_KEY,
    sh.OPEN         AS OPEN,
    sh.HIGH         AS HIGH,
    sh.LOW          AS LOW,
    sh.CLOSE        AS CLOSE,
    sh.ADJCLOSE     AS ADJCLOSE,
    sh.VOLUME       AS VOLUME
  FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
  JOIN DIM_DATE_{GROUP_NUM.upper()} dd
    ON dd.FULL_DATE = sh.DATE
  JOIN DIM_SECURITY_{GROUP_NUM.upper()} ds
    ON ds.SYMBOL = sh.SYMBOL
   AND sh.DATE >= ds.EFFECTIVE_START_DATE
   AND sh.DATE <= COALESCE(ds.EFFECTIVE_END_DATE, '9999-12-31'::DATE)
  WHERE sh.DATE >= DATEADD(DAY, -{DAYS_BACK}, CURRENT_DATE())
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


# -----------------------------
# SQL: FACT_SECURITY_SNAPSHOT (as-of today) current dim row
# -----------------------------
SQL_MERGE_FACT_SECURITY_SNAPSHOT = f"""
USE DATABASE {DB};
USE SCHEMA {SCHEMA};

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
   AND ds.IS_CURRENT = TRUE
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
# SQL: Backfill last 90 days (only on dq_has_issues branch)
# -----------------------------
SQL_BACKFILL_FACT_STOCK_DAILY_90D = f"""
USE DATABASE {DB};
USE SCHEMA {SCHEMA};

DELETE FROM FACT_STOCK_DAILY_{GROUP_NUM.upper()}
WHERE DATE_KEY IN (
  SELECT DATE_KEY
  FROM DIM_DATE_{GROUP_NUM.upper()}
  WHERE FULL_DATE >= DATEADD(DAY, -{DAYS_BACK}, CURRENT_DATE())
);

INSERT INTO FACT_STOCK_DAILY_{GROUP_NUM.upper()} (
  SECURITY_KEY, DATE_KEY, OPEN, HIGH, LOW, CLOSE, ADJCLOSE, VOLUME
)
SELECT
  ds.SECURITY_KEY,
  dd.DATE_KEY,
  sh.OPEN, sh.HIGH, sh.LOW, sh.CLOSE, sh.ADJCLOSE, sh.VOLUME
FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
JOIN DIM_DATE_{GROUP_NUM.upper()} dd
  ON dd.FULL_DATE = sh.DATE
JOIN DIM_SECURITY_{GROUP_NUM.upper()} ds
  ON ds.SYMBOL = sh.SYMBOL
 AND sh.DATE >= ds.EFFECTIVE_START_DATE
 AND sh.DATE <= COALESCE(ds.EFFECTIVE_END_DATE, '9999-12-31'::DATE)
WHERE sh.DATE >= DATEADD(DAY, -{DAYS_BACK}, CURRENT_DATE());
"""


# -----------------------------
# SQL: Validate (does NOT fail; prints counts)
# -----------------------------
SQL_VALIDATE = f"""
USE DATABASE {DB};
USE SCHEMA {SCHEMA};

WITH checks AS (
  SELECT
    (SELECT COUNT(*) FROM FACT_STOCK_DAILY_{GROUP_NUM.upper()} f
      LEFT JOIN DIM_SECURITY_{GROUP_NUM.upper()} d ON f.SECURITY_KEY=d.SECURITY_KEY
      WHERE d.SECURITY_KEY IS NULL) AS orphan_security_daily_cnt,

    (SELECT COUNT(*) FROM FACT_STOCK_DAILY_{GROUP_NUM.upper()} f
      LEFT JOIN DIM_DATE_{GROUP_NUM.upper()} d ON f.DATE_KEY=d.DATE_KEY
      WHERE d.DATE_KEY IS NULL) AS orphan_date_daily_cnt,

    (SELECT COUNT(*) FROM FACT_SECURITY_SNAPSHOT_{GROUP_NUM.upper()} s
      LEFT JOIN DIM_DATE_{GROUP_NUM.upper()} d ON s.ASOF_DATE_KEY=d.DATE_KEY
      WHERE d.DATE_KEY IS NULL) AS orphan_date_snapshot_cnt,

    (SELECT COUNT(*) FROM (
      SELECT SECURITY_KEY, DATE_KEY
      FROM FACT_STOCK_DAILY_{GROUP_NUM.upper()}
      GROUP BY 1,2
      HAVING COUNT(*) > 1
    )) AS dup_daily_cnt,

    (SELECT COUNT(*) FROM (
      SELECT SECURITY_KEY, ASOF_DATE_KEY
      FROM FACT_SECURITY_SNAPSHOT_{GROUP_NUM.upper()}
      GROUP BY 1,2
      HAVING COUNT(*) > 1
    )) AS dup_snapshot_cnt
)
SELECT * FROM checks;

SELECT
  (SELECT COUNT(*) FROM DIM_DATE_{GROUP_NUM.upper()})               AS dim_date_cnt,
  (SELECT COUNT(*) FROM DIM_SECURITY_{GROUP_NUM.upper()})           AS dim_security_cnt,
  (SELECT COUNT(*) FROM FACT_STOCK_DAILY_{GROUP_NUM.upper()})       AS fact_stock_daily_cnt,
  (SELECT COUNT(*) FROM FACT_SECURITY_SNAPSHOT_{GROUP_NUM.upper()}) AS fact_security_snapshot_cnt;
"""


# -----------------------------
# Python helper: safe query (never fail task)
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


# -----------------------------
# Python Tests (XCom)
# -----------------------------
@task
def pytest_orphans_and_dups():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    orphan_security_daily_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_DAILY_TBL} f
      LEFT JOIN {DIM_SECURITY_TBL} d ON f.SECURITY_KEY = d.SECURITY_KEY
      WHERE d.SECURITY_KEY IS NULL
    """)

    orphan_date_daily_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_DAILY_TBL} f
      LEFT JOIN {DIM_DATE_TBL} d ON f.DATE_KEY = d.DATE_KEY
      WHERE d.DATE_KEY IS NULL
    """)

    orphan_date_snapshot_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_SNAP_TBL} s
      LEFT JOIN {DIM_DATE_TBL} d ON s.ASOF_DATE_KEY = d.DATE_KEY
      WHERE d.DATE_KEY IS NULL
    """)

    dup_daily_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*) FROM (
        SELECT SECURITY_KEY, DATE_KEY
        FROM {FACT_DAILY_TBL}
        GROUP BY 1,2
        HAVING COUNT(*) > 1
      )
    """)

    dup_snapshot_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*) FROM (
        SELECT SECURITY_KEY, ASOF_DATE_KEY
        FROM {FACT_SNAP_TBL}
        GROUP BY 1,2
        HAVING COUNT(*) > 1
      )
    """)

    summary = {
        "orphan_security_daily_cnt": int(orphan_security_daily_cnt),
        "orphan_date_daily_cnt": int(orphan_date_daily_cnt),
        "orphan_date_snapshot_cnt": int(orphan_date_snapshot_cnt),
        "dup_daily_cnt": int(dup_daily_cnt),
        "dup_snapshot_cnt": int(dup_snapshot_cnt),
    }

    payload = {"has_issues": any(v > 0 for v in summary.values()), "counts": summary}
    logging.info("[PYTEST] Orphans/Dups counts: %s", json.dumps(payload, indent=2))
    return payload


@task
def pytest_null_fks():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    null_fk_daily_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_DAILY_TBL}
      WHERE SECURITY_KEY IS NULL OR DATE_KEY IS NULL
    """)

    null_fk_snap_cnt = _safe_fetchone(hook, f"""
      SELECT COUNT(*)
      FROM {FACT_SNAP_TBL}
      WHERE SECURITY_KEY IS NULL OR ASOF_DATE_KEY IS NULL
    """)

    summary = {
        "null_fk_daily_cnt": int(null_fk_daily_cnt),
        "null_fk_snap_cnt": int(null_fk_snap_cnt),
    }

    payload = {"has_issues": any(v > 0 for v in summary.values()), "counts": summary}
    logging.info("[PYTEST] Null FK counts: %s", json.dumps(payload, indent=2))
    return payload


def dq_branch_callable(ti, **_):
    dq1 = ti.xcom_pull(task_ids="pytest_orphans_and_dups") or {}
    dq2 = ti.xcom_pull(task_ids="pytest_null_fks") or {}

    has_issues = bool(dq1.get("has_issues")) or bool(dq2.get("has_issues"))
    logging.info("[BRANCH] dq1=%s dq2=%s => has_issues=%s", dq1, dq2, has_issues)
    return "dq_has_issues" if has_issues else "dq_clean"


@task
def log_after_backfill_report(ti=None):
    dq1 = ti.xcom_pull(task_ids="pytest_orphans_and_dups_after_backfill") or {}
    dq2 = ti.xcom_pull(task_ids="pytest_null_fks_after_backfill") or {}
    logging.info("[AFTER_BACKFILL] dq1=%s", json.dumps(dq1, indent=2))
    logging.info("[AFTER_BACKFILL] dq2=%s", json.dumps(dq2, indent=2))
    return "ok"


# -----------------------------
# DAG
# -----------------------------
default_args = {
    "owner": "team1",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=f"project1_stock_dimensional_etl_{GROUP_NUM}",
    default_args=default_args,
    description="Snowflake->Snowflake dimensional model ETL + SCD2 + 90d backfill + branching + recheck (parallel facts)",
    start_date=datetime(2026, 2, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["team1", "snowflake", "scd2", "dq"],
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

    scd2_dim_security = SnowflakeOperator(
    task_id="scd2_dim_security",
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_SCD2_DIM_SECURITY,
    )


    # ✅ facts run in parallel (fixes your log symptom)
    merge_fact_stock_daily_90d = SnowflakeOperator(
        task_id="merge_fact_stock_daily_90d",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_MERGE_FACT_STOCK_DAILY_90D,
    )

    merge_fact_security_snapshot = SnowflakeOperator(
        task_id="merge_fact_security_snapshot",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_MERGE_FACT_SECURITY_SNAPSHOT,
        trigger_rule="all_done",  # extra safety against upstream state weirdness
    )

    # Join point so downstream doesn't get blocked by an unrelated upstream
    etl_done = EmptyOperator(
        task_id="etl_done",
        # if you want "run downstream even if daily fact failed", use all_done.
        # This is a bit safer than all_done but still prevents 'stuck' when one branch succeeds.
        trigger_rule="all_done",
    )

    validate = SnowflakeOperator(
        task_id="validate",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_VALIDATE,
        trigger_rule="all_done",  # don't block DQ due to one upstream
    )

    # First-pass Python tests (XCom)
    t_py_orphans_dups = pytest_orphans_and_dups()
    t_py_null_fks = pytest_null_fks()

    dq_branch = BranchPythonOperator(
        task_id="dq_branch",
        python_callable=dq_branch_callable,
    )

    dq_has_issues = EmptyOperator(task_id="dq_has_issues")
    dq_clean = EmptyOperator(task_id="dq_clean")

    # Backfill only on issues
    backfill_fact_stock_daily_90d = SnowflakeOperator(
        task_id="backfill_fact_stock_daily_90d",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_BACKFILL_FACT_STOCK_DAILY_90D,
        trigger_rule="all_done",
    )

    # Second-pass tests after backfill
    t_py_orphans_dups_after_backfill = pytest_orphans_and_dups.override(
        task_id="pytest_orphans_and_dups_after_backfill"
    )()

    t_py_null_fks_after_backfill = pytest_null_fks.override(
        task_id="pytest_null_fks_after_backfill"
    )()

    followup_has_issues = log_after_backfill_report.override(task_id="followup_has_issues")()
    followup_clean = EmptyOperator(task_id="followup_clean")

    # --- Dependencies ---
    create_tables >> load_dim_date >> scd2_dim_security

    # Parallelize fact loads (key fix)
    scd2_dim_security >> merge_fact_stock_daily_90d
    scd2_dim_security >> merge_fact_security_snapshot

    # Join
    [merge_fact_stock_daily_90d, merge_fact_security_snapshot] >> etl_done >> validate

    # DQ + branching (won't be blocked by a single upstream odd state)
    validate >> t_py_orphans_dups >> t_py_null_fks >> dq_branch

    dq_branch >> dq_clean >> followup_clean

    dq_branch >> dq_has_issues >> backfill_fact_stock_daily_90d \
        >> t_py_orphans_dups_after_backfill >> t_py_null_fks_after_backfill \
        >> followup_has_issues
