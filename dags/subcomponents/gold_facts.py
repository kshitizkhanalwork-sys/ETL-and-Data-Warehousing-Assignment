import logging
from datetime import date
from pathlib import Path
from typing import Dict, Optional

from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from subcomponents.snowflake_session import get_snowflake_cursor, split_sql_statements

SNOWFLAKE_CONN_ID = "retail_snowflake_conn"

GOLD_FACT_SQL_FILES: Dict[str, str] = {
    "fct_sales":     "include/sql/gold/load_fct_sales.sql",
    "fct_inventory": "include/sql/gold/load_fct_inventory.sql",
}

@task
def build_gold_facts() -> Dict[str, int]:
    context = get_current_context()
    logical_date: date = context["logical_date"].date()
    batch_id: str      = context["run_id"]
    dag_id: str        = context["dag"].dag_id

    logging.info(
        "[GOLD_FACTS] Starting Gold fact build for logical_date=%s  batch_id=%s",
        logical_date, batch_id,
    )

    execution_order = ["fct_sales", "fct_inventory"]
    results = {}

    for fact_name in execution_order:
        logging.info("[GOLD_FACTS] Processing fact: %s", fact_name)

        sql_params = {
            "logical_date": str(logical_date),
            "batch_id":     batch_id,
        }

        rows_affected = _execute_gold_fact_sql(
            fact_name=fact_name,
            sql_params=sql_params,
        )

        results[fact_name] = rows_affected

        _write_batch_log(
            batch_id=batch_id,
            dag_id=dag_id,
            logical_date=logical_date,
            stage_name=f"GOLD_{fact_name.upper()}",
            status="SUCCESS",
            rows_written=rows_affected,
        )

        logging.info(
            "[GOLD_FACTS] Completed: fact=%s  rows_affected=%d",
            fact_name, rows_affected,
        )

    logging.info("[GOLD_FACTS] All fact tables completed successfully: %s", results)
    return results

def _execute_gold_fact_sql(fact_name: str, sql_params: Dict[str, str]) -> int:
    sql_file = _resolve_path(GOLD_FACT_SQL_FILES[fact_name])
    if not sql_file.exists():
        raise FileNotFoundError(
            f"[GOLD_FACTS] SQL file not found: {sql_file}\n"
            f"Check that the include/ directory is properly mounted."
        )

    raw_sql = sql_file.read_text(encoding="utf-8")
    rendered_sql = raw_sql.format(**sql_params)

    statements = split_sql_statements(rendered_sql)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    inserted_rows = 0

    with get_snowflake_cursor(hook) as (conn, cursor):
        try:
            cursor.execute("BEGIN")

            for stmt in statements:
                logging.debug("[GOLD_FACTS] Executing:\n%s", stmt[:200])
                cursor.execute(stmt)

                stmt_upper = stmt.strip().upper()
                if stmt_upper.startswith("INSERT"):
                    cnt = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else 0
                    inserted_rows += cnt
                    logging.info("[GOLD_FACTS] INSERT wrote %d rows into %s", cnt, fact_name)
                elif stmt_upper.startswith("DELETE"):
                    logging.info("[GOLD_FACTS] DELETE cleared %d existing rows", cursor.rowcount)
                elif stmt_upper.startswith("UPDATE"):
                    logging.info("[GOLD_FACTS] UPDATE adjusted %d interval bounds", cursor.rowcount)

            cursor.execute("COMMIT")

        except Exception as exc:
            cursor.execute("ROLLBACK")
            logging.error(
                "[GOLD_FACTS] ROLLBACK triggered for fact=%s: %s", fact_name, exc
            )
            raise RuntimeError(
                f"[GOLD_FACTS] Fact load failed for '{fact_name}': {exc}"
            ) from exc

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
        logging.warning("[GOLD_FACTS] C_BATCH_LOG write failed (non-fatal): %s", log_err)

def _resolve_path(relative_path: str) -> Path:
    docker_base = Path("/opt/airflow")
    candidate = docker_base / relative_path
    if candidate.exists():
        return candidate
    local_base = Path(__file__).parent.parent.parent
    return local_base / relative_path
