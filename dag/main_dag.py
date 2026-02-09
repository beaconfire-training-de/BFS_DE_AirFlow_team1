import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


# ---------- Path helpers: read SQL file content ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(BASE_DIR, "..", "sql")

def load_sql(*paths) -> str:
    file_path = os.path.join(SQL_DIR, *paths)
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"
JOB_NAME = "stock_etl_4"


default_args = {"owner": "team4"}

with DAG(
    dag_id="main_stock_etl_4",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["team4", "main"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ---------- 1) Read metadata -> XCom ----------
    def get_mode_and_watermark(ti):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        sql = f"""
        SELECT
            COALESCE(is_initialized, FALSE) AS is_initialized,
            last_loaded_date
        FROM AIRFLOW0105.DEV.ETL_METADATA_4
        WHERE job_name = '{JOB_NAME}'
        LIMIT 1;
        """
        rows = hook.get_records(sql)
        if not rows:
            is_initialized, last_loaded_date = False, None
        else:
            is_initialized, last_loaded_date = rows[0][0], rows[0][1]

        mode = "incremental" if is_initialized else "full"
        ti.xcom_push(key="mode", value=mode)
        ti.xcom_push(key="is_initialized", value=is_initialized)
        ti.xcom_push(key="last_loaded_date", value=str(last_loaded_date) if last_loaded_date else None)

    get_state = PythonOperator(
        task_id="get_mode_and_watermark",
        python_callable=get_mode_and_watermark,
    )

    # ---------- 1) Branch ----------
    def choose_branch(ti):
        mode = ti.xcom_pull(task_ids="get_mode_and_watermark", key="mode")
        return "run_full_path" if mode == "full" else "run_incremental_path"

    branch = BranchPythonOperator(
        task_id="branch_full_or_incremental",
        python_callable=choose_branch,
    )

    # ---------- Common: staging refresh (full) ----------
    refresh_staging_full = SnowflakeOperator(
        task_id="refresh_staging_full",
        sql=load_sql("staging", "refresh_staging.sql"),
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # ---------- Full path tasks ----------
    run_full_path = EmptyOperator(task_id="run_full_path")

    full_load_dims = SnowflakeOperator(
        task_id="full_load_dims",
        sql="\n".join([
            load_sql("init", "create_dim_date_4.sql"),
            load_sql("init", "create_dim_company_core_4.sql"),
            load_sql("init", "create_dim_company_financial_4.sql")
        ]),
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    full_load_fact = SnowflakeOperator(
        task_id="full_load_fact",
        sql=load_sql("init", "create_fact_stock_price_4.sql"),
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # ---------- Incremental path tasks ----------
    run_incremental_path = EmptyOperator(task_id="run_incremental_path")

    dim_incremental = SnowflakeOperator(
        task_id="incremental_load_dims",
        sql="\n".join([
            load_sql("incremental", "01_dim_date_incremental_4.sql"),
            load_sql("incremental", "02_dim_company_core_scd2_4.sql"),
            load_sql("incremental", "03_dim_company_financial_upsert_4.sql"),
        ]),
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    fact_stock_price_incremental = SnowflakeOperator(
        task_id="fact_stock_price_incremental",
        sql=load_sql("incremental", "04_fact_stock_price_incremental_4.sql"),
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # ---------- 3) Join branch back ----------
    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---------- Update watermark (after load) ----------
    update_watermark = SnowflakeOperator(
        task_id="update_watermark",
        sql=load_sql("metadata", "update_watermark_4.sql"),
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # ---------- 4) DQ using STG_SYMBOLS + XCom watermark ----------
    def run_dq_checks(ti):
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        last_loaded = ti.xcom_pull(task_ids="get_mode_and_watermark", key="last_loaded_date")
        checks = [
            ("dq_01_symbols_null_or_dup", load_sql("dq", "dq_01_symbols_null_or_dup_4.sql")),
            ("dq_02_symbols_missing_company_profile", load_sql("dq", "dq_02_symbols_missing_company_profile_4.sql")),
            ("dq_03_stock_history_symbol_not_in_symbols", load_sql("dq", "dq_03_stock_history_symbol_not_in_symbols_4.sql")),
            (
                "dq_04_fact_missing_dim_keys_recent", 
                load_sql("dq", "dq_04_fact_missing_dim_keys_recent_4.sql")
                    .replace("{{LAST_LOADED_DATE}}", f"'{last_loaded}'" if last_loaded else "NULL")
            ),
        ]

        failed = []
        results = {}
        for name, sql in checks:
            cnt = hook.get_first(sql)[0]
            results[name] = int(cnt)
            if int(cnt) > 0:
                failed.append((name, int(cnt)))

        ti.xcom_push(key="dq_results", value=results)

        if failed:
            msg = "DQ failed: " + ", ".join([f"{n}={c}" for n, c in failed])
            raise ValueError(msg)

    dq = PythonOperator(
        task_id="dq_checks",
        python_callable=run_dq_checks,
    )

    end = EmptyOperator(task_id="end")

    # ---------- Dependencies ----------
    start >> get_state >> branch

    # Full path
    branch >> run_full_path >> refresh_staging_full >> full_load_dims >> full_load_fact >> join

    # Incremental path
    branch >> run_incremental_path >> dim_incremental >> fact_stock_price_incremental >> join

    join >> update_watermark >> dq >> end
