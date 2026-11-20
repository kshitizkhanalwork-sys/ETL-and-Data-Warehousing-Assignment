import logging
from datetime import date
from pathlib import Path
from typing import Dict, Optional

from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from subcomponents.snowflake_session import get_snowflake_cursor, split_sql_statements

SNOWFLAKE_CONN_ID = "retail_snowflake_conn"

GOLD_DIM_SQL_FILES: Dict[str, str] = {
    "dim_subclass": "include/sql/gold/load_dim_subclass.sql",
    "dim_district": "include/sql/gold/load_dim_district.sql",
    "dim_item":     "include/sql/gold/load_dim_item_scd2.sql",
    "dim_locn":     "include/sql/gold/load_dim_locn_scd2.sql",
}

@task
def build_gold_dimensions() -> Dict[str, int]:
    context = get_current_context()
    logical_date: date = context["logical_date"].date()
    batch_id: str      = context["run_id"]
    dag_id: str        = context["dag"].dag_id

    logging.info(
        "[GOLD_DIMS] Starting Gold dimension loading for logical_date=%s  batch_id=%s",
        logical_date, batch_id,
    )

    execution_order = ["dim_subclass", "dim_district", "dim_item", "dim_locn"]
    results = {}

    for dim_name in execution_order:
        logging.info("[GOLD_DIMS] Processing dimension: %s", dim_name)

        sql_params = {
            "logical_date": str(logical_date),
            "batch_id":     batch_id,
        }

        rows_affected = _execute_gold_dim_sql(
            dim_name=dim_name,
            sql_params=sql_params,
        )

        results[dim_name] = rows_affected

        _write_batch_log(
            batch_id=batch_id,
            dag_id=dag_id,
            logical_date=logical_date,
            stage_name=f"GOLD_{dim_name.upper()}",
            status="SUCCESS",
            rows_written=rows_affected,
        )

        logging.info(
            "[GOLD_DIMS] Completed: dim=%s  rows_affected=%d",
            dim_name, rows_affected,
        )

    logging.info("[GOLD_DIMS] All dimensions completed successfully: %s", results)
    return results

def _execute_gold_dim_sql(dim_name: str, sql_params: Dict[str, str]) -> int:
    sql_file = _resolve_path(GOLD_DIM_SQL_FILES[dim_name])
    if not sql_file.exists():
        raise FileNotFoundError(
            f"[GOLD_DIMS] SQL file not found: {sql_file}\n"
            f"Check that the include/ directory is properly mounted."
        )

    raw_sql = sql_file.read_text(encoding="utf-8")
    rendered_sql = raw_sql.format(**sql_params)

    statements = split_sql_statements(rendered_sql)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    total_rows_affected = 0

    with get_snowflake_cursor(hook) as (conn, cursor):
        try:
            cursor.execute("BEGIN")

            for stmt in statements:
                logging.debug("[GOLD_DIMS] Executing:\n%s", stmt[:200])
                cursor.execute(stmt)

                stmt_upper = stmt.strip().upper()
                if stmt_upper.startswith(("INSERT", "UPDATE", "MERGE")):
                    cnt = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else 0
                    total_rows_affected += cnt
                    logging.info("[GOLD_DIMS] %s affected %d rows", stmt_upper.split()[0], cnt)

            cursor.execute("COMMIT")

        except Exception as exc:
            cursor.execute("ROLLBACK")
            logging.error(
                "[GOLD_DIMS] ROLLBACK triggered for dimension=%s: %s", dim_name, exc
            )
            raise RuntimeError(
                f"[GOLD_DIMS] Dimension load failed for '{dim_name}': {exc}"
            ) from exc

    return total_rows_affected

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
        logging.warning("[GOLD_DIMS] C_BATCH_LOG write failed (non-fatal): %s", log_err)

def _resolve_path(relative_path: str) -> Path:
    docker_base = Path("/opt/airflow")
    candidate = docker_base / relative_path
    if candidate.exists():
        return candidate
    local_base = Path(__file__).parent.parent.parent
    return local_base / relative_path
