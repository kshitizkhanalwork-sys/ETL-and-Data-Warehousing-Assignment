import csv
import json
import logging
import os
from datetime import date, datetime
from typing import Dict, List, Optional

from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from subcomponents.snowflake_session import get_snowflake_cursor, ensure_snowflake_context

SNOWFLAKE_CONN_ID = "retail_snowflake_conn"

FEED_DEFINITIONS: List[Dict[str, str]] = [
    {
        "feed":         "sales",
        "file_pattern": "pos_sales_{date_nodash}.csv",
        "target":       "STG_POS_SALES",
    },
    {
        "feed":         "inventory",
        "file_pattern": "inventory_snapshot_{date_nodash}.csv",
        "target":       "STG_INVENTORY_SNAPSHOT",
    },
    {
        "feed":         "item",
        "file_pattern": "item_master_{date_nodash}.csv",
        "target":       "STG_ITEM_MASTER",
    },
    {
        "feed":         "store",
        "file_pattern": "store_master_{date_nodash}.csv",
        "target":       "STG_STORE_MASTER",
    },
]

BRONZE_COLUMN_MAPS: Dict[str, List[str]] = {
    "STG_POS_SALES": [
        "TXN_ID", "LINE_ID", "TXN_TS", "TXN_DT",
        "STORE_ID", "ITEM_ID", "SLS_QTY", "SLS_AMT",
        "RTN_QTY", "RTN_AMT",
    ],
    "STG_INVENTORY_SNAPSHOT": [
        "STORE_ID", "ITEM_ID", "SNAP_DT",
        "STOCK_ON_HAND", "STOCK_IN_TRANSIT", "STOCK_RESERVED",
    ],
    "STG_ITEM_MASTER": [
        "ITEM_ID", "ITEM_DESC",
        "SUBCLASS_ID", "SUBCLASS_NM",
        "CLASS_ID", "CLASS_NM",
        "DEPT_ID", "DEPT_NM",
        "UNIT_COST", "UNIT_RETAIL",
    ],
    "STG_STORE_MASTER": [
        "STORE_ID", "STORE_NM",
        "DISTRICT_ID", "DISTRICT_NM",
        "REGION_ID", "REGION_NM",
        "OPEN_DT", "CLOSE_DT", "STORE_TYPE",
    ],
}

ALLOWED_BRONZE_TABLES = set(BRONZE_COLUMN_MAPS.keys())

@task
def list_daily_files() -> List[Dict[str, str]]:
    context = get_current_context()

    logical_date = context["logical_date"]

    date_nodash = logical_date.strftime("%Y%m%d")

    base_dir = Variable.get("RETAIL_DROP_DIR", default_var="/opt/airflow/data")

    resolved_files = []
    for feed_def in FEED_DEFINITIONS:

        filename = feed_def["file_pattern"].format(date_nodash=date_nodash)
        full_path = os.path.join(base_dir, filename)

        resolved_files.append({
            "feed":   feed_def["feed"],
            "file":   full_path,
            "target": feed_def["target"],
        })

    logging.info(
        "Prepared %d file configs for logical_date=%s",
        len(resolved_files),
        logical_date.date(),
    )
    for fc in resolved_files:
        logging.info("  feed=%-12s  file=%s", fc["feed"], fc["file"])

    return resolved_files

@task
def load_csv_to_bronze(file_info: Dict[str, str]) -> Dict[str, int]:

    context = get_current_context()
    logical_date: date = context["logical_date"].date()
    batch_id: str = context["run_id"]
    dag_id: str = context["dag"].dag_id

    feed: str = file_info["feed"]
    file_path: str = file_info["file"]
    target_table: str = file_info["target"]

    if target_table not in ALLOWED_BRONZE_TABLES:
        raise ValueError(
            f"[BRONZE] Refused to write to unknown table: '{target_table}'. "
            f"Allowed: {ALLOWED_BRONZE_TABLES}"
        )

    if not os.path.isfile(file_path):
        raise FileNotFoundError(
            f"[BRONZE] CSV file not found: {file_path}\n"
            f"Check that the sensor succeeded and the mount is correct."
        )

    source_file_name: str = os.path.basename(file_path)

    src_columns: List[str] = BRONZE_COLUMN_MAPS[target_table]

    logging.info(
        "[BRONZE] Starting load: feed=%s  file=%s  target=%s  date=%s",
        feed, source_file_name, target_table, logical_date,
    )

    rows_to_insert: List[tuple] = []

    with open(file_path, mode="r", encoding="utf-8-sig", newline="") as csv_file:

        reader = csv.DictReader(csv_file)

        if not reader.fieldnames:
            raise ValueError(f"[BRONZE] CSV has no header row: {file_path}")

        normalised_headers = {h.upper(): h for h in reader.fieldnames}

        for expected_col in src_columns:
            if expected_col not in normalised_headers:
                raise ValueError(
                    f"[BRONZE] Expected column '{expected_col}' not found in "
                    f"{source_file_name}. Available: {list(reader.fieldnames)}"
                )

        for row_number, raw_row in enumerate(reader, start=2):

            raw_payload: str = json.dumps(raw_row, ensure_ascii=False)

            src_values: List[Optional[str]] = [
                raw_row.get(normalised_headers[col]) or None
                for col in src_columns
            ]

            rows_to_insert.append(
                tuple(src_values) + (
                    raw_payload,
                    source_file_name,
                    row_number,
                    batch_id,
                    logical_date,
                )
            )

    rows_read = len(rows_to_insert)
    logging.info("[BRONZE] Read %d rows from %s", rows_read, source_file_name)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    rows_written = 0

    with get_snowflake_cursor(hook) as (conn, cursor):
        try:

            cursor.execute("BEGIN")

            delete_sql = f"DELETE FROM {target_table} WHERE INGEST_DT = %s"
            cursor.execute(delete_sql, (logical_date,))
            deleted_count = cursor.rowcount
            logging.info(
                "[BRONZE] Deleted %d existing rows from %s for %s",
                deleted_count, target_table, logical_date
            )

            if rows_to_insert:
                insert_sql = _build_insert_sql(target_table, src_columns)

                cursor.executemany(insert_sql, rows_to_insert)
                rows_written = len(rows_to_insert)

            cursor.execute("COMMIT")
            logging.info(
                "[BRONZE] ✓ Committed %d rows into %s for %s",
                rows_written, target_table, logical_date
            )

        except Exception as exc:

            cursor.execute("ROLLBACK")
            logging.error("[BRONZE] ROLLBACK triggered: %s", exc)
            raise

    _write_batch_log(
        batch_id=batch_id,
        dag_id=dag_id,
        logical_date=logical_date,
        stage_name=f"BRONZE_{target_table}",
        status="SUCCESS",
        rows_read=rows_read,
        rows_written=rows_written,
    )

    return {"rows_read": rows_read, "rows_written": rows_written}

def build_bronze_ingest():
    file_configs = list_daily_files()
    bronze_tasks = load_csv_to_bronze.expand(file_info=file_configs)
    return bronze_tasks

def _build_insert_sql(table_name: str, src_columns: List[str]) -> str:

    all_param_columns = src_columns + [
        "RAW_PAYLOAD",
        "SRC_FILE_NM",
        "SRC_ROW_NUM",
        "BATCH_ID",
        "INGEST_DT",
    ]

    param_placeholders = ", ".join(["%s"] * len(all_param_columns))

    col_list = ", ".join(all_param_columns) + ", INGEST_TS"
    val_list = param_placeholders + ", CURRENT_TIMESTAMP()"

    return f"INSERT INTO {table_name} ({col_list}) VALUES ({val_list})"

def _write_batch_log(
    batch_id: str,
    dag_id: str,
    logical_date: date,
    stage_name: str,
    status: str,
    rows_read: int = 0,
    rows_written: int = 0,
    error_msg: Optional[str] = None,
) -> None:
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    try:
        with get_snowflake_cursor(hook) as (conn, cursor):
            cursor.execute(
                """
                INSERT INTO C_BATCH_LOG (
                    BATCH_ID, DAG_ID, LOGICAL_DATE, STAGE_NAME,
                    STATUS, ROWS_READ, ROWS_WRITTEN, ERROR_MSG,
                    START_TS, END_TS
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                )
                """,
                (
                    batch_id, dag_id, logical_date, stage_name,
                    status, rows_read, rows_written, error_msg,
                ),
            )
            cursor.execute("COMMIT")
    except Exception as log_err:

        logging.warning("[BRONZE] C_BATCH_LOG write failed (non-fatal): %s", log_err)
