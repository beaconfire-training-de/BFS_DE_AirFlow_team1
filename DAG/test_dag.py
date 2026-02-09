# dags/test_snowflake_conn.py
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="team4_tong_test_snowflake_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger only
    catchup=False,
):
    test = SnowflakeOperator(
        task_id="team4_tong_test_connection",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()",
    )