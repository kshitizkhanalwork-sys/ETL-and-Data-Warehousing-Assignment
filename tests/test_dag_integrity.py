import os
import sys
import time
import pytest
from pathlib import Path

DAGS_FOLDER = str(Path(__file__).parent.parent / "dags")
if DAGS_FOLDER not in sys.path:
    sys.path.insert(0, DAGS_FOLDER)

@pytest.fixture(scope="session")
def dag_bag():
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("Airflow is not installed in the local Python environment.")

    start_time = time.time()
    bag = DagBag(dag_folder=DAGS_FOLDER)
    load_duration = time.time() - start_time

    return {"bag": bag, "load_duration": load_duration}

def test_dag_import_no_errors(dag_bag):
    bag = dag_bag["bag"]
    assert len(bag.import_errors) == 0, (
        f"DAG import errors detected: {bag.import_errors}"
    )

def test_dag_load_performance(dag_bag):
    load_duration = dag_bag["load_duration"]
    assert load_duration < 1.0, (
        f"DagBag took {load_duration:.3f}s to parse, exceeding the 1.0s limit! "
        "Check for top-level code, filesystem scans, or network requests."
    )

def test_dag_has_no_cycles(dag_bag):
    bag = dag_bag["bag"]
    dag = bag.get_dag("retail_medallion_etl")
    assert dag is not None, "retail_medallion_etl DAG not found in DagBag"

    dag.check_cycle()

def test_retail_pipeline_topology(dag_bag):
    bag = dag_bag["bag"]
    dag = bag.get_dag("retail_medallion_etl")

    task_ids = set(dag.task_ids)

    expected_tasks = [
        "wait_for_files.wait_for_pos",
        "wait_for_files.wait_for_inventory",
        "wait_for_files.wait_for_item",
        "wait_for_files.wait_for_store",
        "list_daily_files",
        "load_csv_to_bronze",
        "build_silver_layer",
        "build_gold_dimensions",
        "build_gold_facts",
        "build_marts",
        "reconcile_pipeline",
    ]

    for expected in expected_tasks:
        assert expected in task_ids, f"Expected task '{expected}' missing from DAG!"

    list_files_task = dag.get_task("list_daily_files")
    load_bronze_task = dag.get_task("load_csv_to_bronze")
    silver_task = dag.get_task("build_silver_layer")
    gold_dims_task = dag.get_task("build_gold_dimensions")
    gold_facts_task = dag.get_task("build_gold_facts")
    marts_task = dag.get_task("build_marts")
    reconcile_task = dag.get_task("reconcile_pipeline")

    assert load_bronze_task in list_files_task.downstream_list
    assert silver_task in load_bronze_task.downstream_list
    assert gold_dims_task in silver_task.downstream_list
    assert gold_facts_task in gold_dims_task.downstream_list
    assert marts_task in gold_facts_task.downstream_list
    assert reconcile_task in marts_task.downstream_list

def test_default_args_and_hardening(dag_bag):
    bag = dag_bag["bag"]
    dag = bag.get_dag("retail_medallion_etl")

    assert dag.default_args.get("retries", 0) >= 2, (
        "DAG retries must be >= 2 for production resilience."
    )
    assert dag.max_active_runs == 1, (
        "max_active_runs must be 1 to guarantee sequential backfill processing."
    )
    assert dag.catchup is False, (
        "catchup should be False by default for manual backfills."
    )
