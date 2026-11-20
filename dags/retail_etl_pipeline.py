from datetime import datetime, timedelta

from airflow import DAG

from subcomponents.sensors import build_wait_sensors
from subcomponents.bronze_ingest import list_daily_files, load_csv_to_bronze
from subcomponents.silver_clean import build_silver_layer
from subcomponents.gold_dims import build_gold_dimensions
from subcomponents.gold_facts import build_gold_facts
from subcomponents.marts import build_marts
from subcomponents.reconciliation import reconcile_pipeline

DEFAULT_ARGS = {
    "owner": "kshitiz",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="retail_medallion_etl",
    default_args=DEFAULT_ARGS,
    description="Module 2.3: Retail Store Medallion ETL from Raw Drops to Gold Kimball Model",
    schedule="@daily",
    start_date=datetime(2026, 3, 1),
    end_date=datetime(2026, 3, 5),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "medallion", "snowflake", "module2"],
) as dag:

    wait_stage = build_wait_sensors(
        group_id="wait_for_files",
        base_dir="{{ var.value.get('RETAIL_DROP_DIR', '/opt/airflow/data') }}",
    )

    file_list = list_daily_files()
    bronze_stage = load_csv_to_bronze.expand(file_info=file_list)

    silver_stage = build_silver_layer()

    gold_dims_stage = build_gold_dimensions()

    gold_facts_stage = build_gold_facts()

    agg_stage = build_marts()

    reconcile_stage = reconcile_pipeline()

    (
        wait_stage
        >> file_list
        >> bronze_stage
        >> silver_stage
        >> gold_dims_stage
        >> gold_facts_stage
        >> agg_stage
        >> reconcile_stage
    )
