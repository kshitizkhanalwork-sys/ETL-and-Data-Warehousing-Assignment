import logging
import os
from datetime import date
from pathlib import Path
from typing import Dict, Optional

from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from subcomponents.snowflake_session import get_snowflake_cursor, split_sql_statements

SNOWFLAKE_CONN_ID = "retail_snowflake_conn"

SILVER_SQL_FILES: Dict[str, str] = {
    "item_master": "include/sql/silver/clean_item_master.sql",
    "store_master": "include/sql/silver/clean_store_master.sql",
    "pos_sales":    "include/sql/silver/clean_pos_sales.sql",
    "inventory":    "include/sql/silver/clean_inventory.sql",
}

SRC_FILE_PATTERNS: Dict[str, str] = {
    "pos_sales":    "pos_sales_{date_nodash}.csv",
    "inventory":    "inventory_snapshot_{date_nodash}.csv",
    "item_master":  "item_master_{date_nodash}.csv",
    "store_master": "store_master_{date_nodash}.csv",
}

@task
def build_silver_layer() -> Dict[str, int]:
    context = get_current_context()
    logical_date: date = context["logical_date"].date()
    batch_id: str      = context["run_id"]
    dag_id: str        = context["dag"].dag_id
    date_nodash: str   = logical_date.strftime("%Y%m%d")

    logging.info(
        "[SILVER] Starting Silver cleaning for logical_date=%s  batch_id=%s",
        logical_date, batch_id,
    )

    execution_order = ["item_master", "store_master", "pos_sales", "inventory"]

    results = {}

    for feed_name in execution_order:
        src_file_nm = SRC_FILE_PATTERNS[feed_name].format(date_nodash=date_nodash)

        logging.info(
            "[SILVER] Processing feed: %s  src_file=%s",
            feed_name, src_file_nm,
        )

        sql_params = {
            "logical_date": str(logical_date),
            "batch_id":     batch_id,
            "src_file_nm":  src_file_nm,
        }

        rows_written = _execute_silver_sql(
            feed_name=feed_name,
            sql_params=sql_params,
        )

        results[feed_name] = rows_written

        _write_batch_log(
            batch_id=batch_id,
            dag_id=dag_id,
            logical_date=logical_date,
            stage_name=f"SILVER_{feed_name.upper()}",
            status="SUCCESS",
            rows_written=rows_written,
        )

        logging.info(
            "[SILVER] ✓ Completed: feed=%s  rows_written=%d",
            feed_name, rows_written,
        )

    quarantine_count = _get_quarantine_count(logical_date, batch_id)
    logging.info(
        "[SILVER] Summary for %s: quarantined=%d  results=%s",
        logical_date, quarantine_count, results,
    )

    return results

def _execute_silver_sql(feed_name: str, sql_params: Dict[str, str]) -> int:

    sql_file = _resolve_path(SILVER_SQL_FILES[feed_name])
    if not sql_file.exists():
        raise FileNotFoundError(
            f"[SILVER] SQL file not found: {sql_file}\n"
            f"Check that the include/ directory is mounted in Docker."
        )

    raw_sql = sql_file.read_text(encoding="utf-8")
    rendered_sql = raw_sql.format(**sql_params)

    statements = split_sql_statements(rendered_sql)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    rows_written = 0

    with get_snowflake_cursor(hook) as (conn, cursor):
        try:
            cursor.execute("BEGIN")

            for stmt in statements:
                logging.debug("[SILVER] Executing:\n%s", stmt[:200])
                cursor.execute(stmt)

                stmt_upper = stmt.strip().upper()
                if stmt_upper.startswith("INSERT"):
                    rows_written += cursor.rowcount
                    logging.info(
                        "[SILVER] INSERT affected %d rows", cursor.rowcount
                    )
                elif stmt_upper.startswith("DELETE"):
                    logging.info(
                        "[SILVER] DELETE removed %d rows", cursor.rowcount
                    )
                elif stmt_upper.startswith("TRUNCATE"):
                    logging.info("[SILVER] TRUNCATE completed")

            cursor.execute("COMMIT")

        except Exception as exc:
            cursor.execute("ROLLBACK")
            logging.error(
                "[SILVER] ROLLBACK triggered for feed=%s: %s", feed_name, exc
            )

            raise RuntimeError(
                f"[SILVER] Silver cleaning failed for feed '{feed_name}': {exc}"
            ) from exc

    return rows_written

def _get_quarantine_count(logical_date: date, batch_id: str) -> int:
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    with get_snowflake_cursor(hook) as (conn, cursor):
        cursor.execute(
            """
            SELECT COUNT(*) FROM C_REJ_TABLE
            WHERE LOGICAL_DATE = %s AND BATCH_ID = %s
            """,
            (logical_date, batch_id),
        )
        return cursor.fetchone()[0]

def _write_batch_log(
    batch_id: str,
    dag_id: str,
    logical_date: date,
    stage_name: str,
    status: str,
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
                    STATUS, ROWS_WRITTEN, ERROR_MSG,
                    START_TS, END_TS
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                )
                """,
                (
                    batch_id, dag_id, logical_date, stage_name,
                    status, rows_written, error_msg,
                ),
            )
            cursor.execute("COMMIT")
    except Exception as log_err:
        logging.warning("[SILVER] C_BATCH_LOG write failed (non-fatal): %s", log_err)

def _resolve_path(relative_path: str) -> Path:
    docker_base = Path("/opt/airflow")
    candidate = docker_base / relative_path
    if candidate.exists():
        return candidate

    local_base = Path(__file__).parent.parent.parent
    return local_base / relative_path
