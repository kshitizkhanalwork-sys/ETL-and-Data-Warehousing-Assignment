import logging
from datetime import date
from pathlib import Path
from typing import Dict, Optional

from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from subcomponents.snowflake_session import get_snowflake_cursor, split_sql_statements

SNOWFLAKE_CONN_ID = "retail_snowflake_conn"

MART_SQL_FILES: Dict[str, str] = {
    "agg_sales": "include/sql/gold/load_agg_sales.sql",
}

@task
def build_marts() -> Dict[str, int]:
    context = get_current_context()
    logical_date: date = context["logical_date"].date()
    batch_id: str      = context["run_id"]
    dag_id: str        = context["dag"].dag_id

    logging.info(
        "[MARTS] Starting aggregate mart build for logical_date=%s  batch_id=%s",
        logical_date, batch_id,
    )

    results = {}

    for mart_name, sql_rel_path in MART_SQL_FILES.items():
        logging.info("[MARTS] Rebuilding mart: %s", mart_name)

        sql_params = {
            "logical_date": str(logical_date),
            "batch_id":     batch_id,
        }

        rows_written = _execute_mart_sql(sql_rel_path, sql_params)
        results[mart_name] = rows_written

        _write_batch_log(
            batch_id=batch_id,
            dag_id=dag_id,
            logical_date=logical_date,
            stage_name=f"MART_{mart_name.upper()}",
            status="SUCCESS",
            rows_written=rows_written,
        )

        logging.info("[MARTS] Completed %s: wrote %d rows", mart_name, rows_written)

    return results

def _execute_mart_sql(sql_rel_path: str, sql_params: Dict[str, str]) -> int:
    sql_file = _resolve_path(sql_rel_path)
    if not sql_file.exists():
        raise FileNotFoundError(f"[MARTS] SQL file not found: {sql_file}")

    raw_sql = sql_file.read_text(encoding="utf-8")
    rendered_sql = raw_sql.format(**sql_params)

    statements = split_sql_statements(rendered_sql)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    inserted_rows = 0

    with get_snowflake_cursor(hook) as (conn, cursor):
        try:
            cursor.execute("BEGIN")

            for stmt in statements:
                cursor.execute(stmt)

                stmt_upper = stmt.strip().upper()
                if stmt_upper.startswith("INSERT"):
                    cnt = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else 0
                    inserted_rows += cnt

            cursor.execute("COMMIT")

        except Exception as exc:
            cursor.execute("ROLLBACK")
            logging.error("[MARTS] ROLLBACK triggered: %s", exc)
            raise RuntimeError(f"[MARTS] Mart build failed: {exc}") from exc

    return inserted_rows

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
        logging.warning("[MARTS] C_BATCH_LOG write failed (non-fatal): %s", log_err)

def _resolve_path(relative_path: str) -> Path:
    docker_base = Path("/opt/airflow")
    candidate = docker_base / relative_path
    if candidate.exists():
        return candidate
    local_base = Path(__file__).parent.parent.parent
    return local_base / relative_path
