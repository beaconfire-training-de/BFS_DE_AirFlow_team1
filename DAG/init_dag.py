import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(BASE_DIR, "..", "sql")

def load_sql(*paths):
    file_path = os.path.join(SQL_DIR, *paths)
    with open(file_path, "r") as f:
        return f.read()

default_args = {"owner": "team4"}

with DAG(
    dag_id="init_stock_infra_4",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["team4", "init"],
) as dag:

    create_schemas = SnowflakeOperator(
        task_id="create_schemas",
        sql=load_sql("init", "00_create_schemas.sql"),
        snowflake_conn_id="jan_airflow_snowflake",
    )

    create_staging_tables = SnowflakeOperator(
        task_id="create_staging_tables",
        sql=load_sql("init", "01_create_staging_tables.sql"),
        snowflake_conn_id="jan_airflow_snowflake",
    )

    create_metadata_table = SnowflakeOperator(
        task_id="create_metadata_table",
        sql=load_sql("metadata", "create_metadata_table_4.sql"),
        snowflake_conn_id="jan_airflow_snowflake",
    )

    init_metadata_record = SnowflakeOperator(
        task_id="init_metadata_record",
        sql=load_sql("metadata", "init_metadata_record_4.sql"),
        snowflake_conn_id="jan_airflow_snowflake",
    )

    create_schemas >> create_staging_tables >> create_metadata_table >> init_metadata_record
