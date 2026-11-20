import logging
from contextlib import contextmanager
from typing import Generator, Tuple

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def ensure_snowflake_context(cursor, hook: SnowflakeHook) -> Tuple[str, str]:
    conn_obj = hook.get_connection(hook.snowflake_conn_id)
    extra = conn_obj.extra_dejson or {}

    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    row = cursor.fetchone()
    curr_db = row[0] if row else None
    curr_schema = row[1] if row else None

    target_db = (
        extra.get("database")
        or getattr(hook, "database", None)
        or curr_db
    )

    target_schema = (
        conn_obj.schema
        or extra.get("schema")
        or curr_schema
        or "RETAIL_DWH"
    )

    if not curr_db:
        if not target_db:

            cursor.execute("SHOW DATABASES")
            user_dbs = [
                r[1] for r in cursor.fetchall()
                if r[1] not in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA")
            ]
            if user_dbs:
                target_db = user_dbs[0]
                logging.info("[SNOWFLAKE] Auto-selecting user database: %s", target_db)
            else:
                target_db = "RETAIL_DWH_DB"

        logging.info("[SNOWFLAKE] Ensuring database exists: %s", target_db)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        cursor.execute(f"USE DATABASE {target_db}")
        curr_db = target_db

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {curr_db}.{target_schema}")
    cursor.execute(f"USE SCHEMA {curr_db}.{target_schema}")

    logging.info("[SNOWFLAKE] Active Session Context: Database=%s, Schema=%s", curr_db, target_schema)
    return curr_db, target_schema

@contextmanager
def get_snowflake_cursor(
    hook: SnowflakeHook,
    autocommit: bool = False,
) -> Generator:
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        ensure_snowflake_context(cursor, hook)
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()

def split_sql_statements(sql_text: str) -> list[str]:
    clean_lines = []
    for line in sql_text.splitlines():
        trimmed = line.strip()
        if trimmed.startswith("--"):
            continue
        if "--" in line:
            idx = line.find("--")
            before = line[:idx]
            if before.count("'") % 2 == 0:
                line = before
        if line.strip():
            clean_lines.append(line)

    clean_text = "\n".join(clean_lines)
    return [stmt.strip() for stmt in clean_text.split(";") if stmt.strip()]
