from datetime import datetime
from airflow import DAG
from subcomponents.init_schema import initialise_snowflake_schema

with DAG(
    dag_id="init_snowflake_schema_dag",
    description="One-time utility to initialise Snowflake Control, Bronze, Silver, and Gold tables",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["admin", "snowflake", "setup"],
) as dag:

    init_schema_task = initialise_snowflake_schema()
