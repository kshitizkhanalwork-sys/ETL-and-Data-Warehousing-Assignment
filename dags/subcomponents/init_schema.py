import logging
import os
from pathlib import Path

from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from subcomponents.snowflake_session import get_snowflake_cursor, split_sql_statements

SNOWFLAKE_CONN_ID = "retail_snowflake_conn"

DDL_FILES_IN_ORDER = [
    "include/sql/ddl/init_control_tables.sql",
    "include/sql/ddl/init_bronze_tables.sql",
    "include/sql/ddl/init_silver_tables.sql",
    "include/sql/ddl/init_gold_tables.sql",
]

def _resolve_ddl_path(relative_path: str) -> Path:

    docker_base = Path("/opt/airflow")
    candidate = docker_base / relative_path
    if candidate.exists():
        return candidate

    local_base = Path(__file__).parent.parent.parent
    return local_base / relative_path

def _execute_sql_file(hook: SnowflakeHook, file_path: Path) -> int:
    sql_text = file_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    executed = 0

    with get_snowflake_cursor(hook) as (conn, cursor):
        for stmt in statements:
            logging.info("Executing statement from %s:\n%s", file_path.name, stmt[:120])
            cursor.execute(stmt)
            executed += 1

    return executed

@task
def initialise_snowflake_schema():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    total_statements = 0

    for relative_path in DDL_FILES_IN_ORDER:
        ddl_file = _resolve_ddl_path(relative_path)

        if not ddl_file.exists():
            raise FileNotFoundError(
                f"DDL file not found: {ddl_file}\n"
                f"Check that the file exists and that the Docker volume is mounted."
            )

        logging.info("=" * 60)
        logging.info("Running DDL file: %s", ddl_file.name)
        logging.info("=" * 60)

        count = _execute_sql_file(hook, ddl_file)
        total_statements += count
        logging.info("Completed %s — %d statements executed.", ddl_file.name, count)

    logging.info(
        "Schema initialisation complete. Total statements executed: %d",
        total_statements,
    )
    return total_statements
