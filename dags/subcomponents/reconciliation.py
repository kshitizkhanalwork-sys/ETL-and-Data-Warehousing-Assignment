import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from subcomponents.snowflake_session import get_snowflake_cursor

SNOWFLAKE_CONN_ID = "retail_snowflake_conn"

@task
def reconcile_pipeline() -> Dict[str, Any]:
    context = get_current_context()
    logical_date: date = context["logical_date"].date()
    batch_id: str      = context["run_id"]
    dag_id: str        = context["dag"].dag_id

    logging.info(
        "======================================================================"
    )
    logging.info(
        "[RECONCILIATION] Starting Data Integrity Gate for logical_date=%s",
        logical_date,
    )
    logging.info(
        "======================================================================"
    )

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    with get_snowflake_cursor(hook) as (conn, cursor):
        try:

            cursor.execute(
                "SELECT COUNT(*) FROM STG_POS_SALES WHERE INGEST_DT = %s",
                (logical_date,),
            )
            bronze_pos_count = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM TMP_POS_SALES WHERE TXN_DT = %s",
                (logical_date,),
            )
            silver_pos_count = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT COUNT(*) FROM C_REJ_TABLE
                WHERE LOGICAL_DATE = %s
                  AND SRC_FILE_NM LIKE 'pos_sales%%'
                """,
                (logical_date,),
            )
            quarantine_pos_count = cursor.fetchone()[0] or 0

            expected_pos_total = silver_pos_count + quarantine_pos_count
            logging.info(
                "[CHECK 1 - POS Volume] Bronze: %d | Silver Valid: %d | Quarantined: %d | Sum: %d",
                bronze_pos_count, silver_pos_count, quarantine_pos_count, expected_pos_total,
            )

            if bronze_pos_count != expected_pos_total:
                err_msg = (
                    f"RECONCILIATION VIOLATION [POS Volume]: Mismatch detected for {logical_date}! "
                    f"Bronze count ({bronze_pos_count}) != Silver valid ({silver_pos_count}) + "
                    f"Quarantined ({quarantine_pos_count}) [Sum: {expected_pos_total}]. "
                    f"Difference: {bronze_pos_count - expected_pos_total} un accounted rows."
                )
                logging.error(err_msg)
                _record_failure_log(cursor, batch_id, dag_id, logical_date, err_msg)
                conn.commit()
                raise ValueError(err_msg)

            cursor.execute(
                """
                SELECT COALESCE(SUM(SLS_AMT - RTN_AMT), 0)
                FROM TMP_POS_SALES
                WHERE TXN_DT = %s
                """,
                (logical_date,),
            )
            silver_net_sales = Decimal(str(cursor.fetchone()[0] or 0))

            cursor.execute(
                """
                SELECT COALESCE(SUM(NET_SLS_AMT), 0)
                FROM FCT_SALES
                WHERE TXN_DT = %s
                """,
                (logical_date,),
            )
            gold_fct_net_sales = Decimal(str(cursor.fetchone()[0] or 0))

            cursor.execute(
                """
                SELECT COALESCE(SUM(NET_SLS_AMT), 0)
                FROM AGG_SLS_SUBCLS_LOCN_DAY
                WHERE DAY_DT = %s
                """,
                (logical_date,),
            )
            mart_net_sales = Decimal(str(cursor.fetchone()[0] or 0))

            logging.info(
                "[CHECK 2 - Financials] Silver Net: $%.2f | Gold FCT Net: $%.2f | Mart Net: $%.2f",
                silver_net_sales, gold_fct_net_sales, mart_net_sales,
            )

            if abs(silver_net_sales - gold_fct_net_sales) > Decimal("0.01"):
                err_msg = (
                    f"RECONCILIATION VIOLATION [Financial Tie]: Silver Net Sales (${silver_net_sales:.2f}) "
                    f"does not match Gold FCT_SALES Net Sales (${gold_fct_net_sales:.2f}) for {logical_date}!"
                )
                logging.error(err_msg)
                _record_failure_log(cursor, batch_id, dag_id, logical_date, err_msg)
                conn.commit()
                raise ValueError(err_msg)

            if abs(gold_fct_net_sales - mart_net_sales) > Decimal("0.01"):
                err_msg = (
                    f"RECONCILIATION VIOLATION [Mart Financial Tie]: Gold FCT Net Sales (${gold_fct_net_sales:.2f}) "
                    f"does not match AGG Mart Net Sales (${mart_net_sales:.2f}) for {logical_date}!"
                )
                logging.error(err_msg)
                _record_failure_log(cursor, batch_id, dag_id, logical_date, err_msg)
                conn.commit()
                raise ValueError(err_msg)

            cursor.execute(
                "SELECT COUNT(*) FROM STG_INVENTORY_SNAPSHOT WHERE INGEST_DT = %s",
                (logical_date,),
            )
            bronze_inv_count = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM TMP_INVENTORY WHERE SNAP_DT = %s",
                (logical_date,),
            )
            silver_inv_count = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT COUNT(*) FROM C_REJ_TABLE
                WHERE LOGICAL_DATE = %s
                  AND SRC_FILE_NM LIKE 'inventory_snapshot%%'
                """,
                (logical_date,),
            )
            quarantine_inv_count = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM FCT_INVENTORY WHERE SNAP_DT = %s",
                (logical_date,),
            )
            fct_inv_count = cursor.fetchone()[0] or 0

            expected_inv_total = silver_inv_count + quarantine_inv_count
            logging.info(
                "[CHECK 3 - Inventory] Bronze: %d | Silver: %d | Quarantined: %d | Gold FCT: %d",
                bronze_inv_count, silver_inv_count, quarantine_inv_count, fct_inv_count,
            )

            if bronze_inv_count != expected_inv_total:
                err_msg = (
                    f"RECONCILIATION VIOLATION [Inventory Volume]: Bronze inventory ({bronze_inv_count}) "
                    f"!= Silver ({silver_inv_count}) + Quarantined ({quarantine_inv_count}) for {logical_date}!"
                )
                logging.error(err_msg)
                _record_failure_log(cursor, batch_id, dag_id, logical_date, err_msg)
                conn.commit()
                raise ValueError(err_msg)

            if silver_inv_count != fct_inv_count:
                err_msg = (
                    f"RECONCILIATION VIOLATION [Inventory Fact Completeness]: Silver inventory rows "
                    f"({silver_inv_count}) != Gold FCT_INVENTORY rows ({fct_inv_count}) for {logical_date}!"
                )
                logging.error(err_msg)
                _record_failure_log(cursor, batch_id, dag_id, logical_date, err_msg)
                conn.commit()
                raise ValueError(err_msg)

            cursor.execute(
                """
                UPDATE C_LOAD_DATES
                SET LAST_SUCCESS_DT = GREATEST(COALESCE(LAST_SUCCESS_DT, '1900-01-01'::DATE), %s::DATE),
                    LAST_SUCCESS_DT_COMMITTED_TS = CURRENT_TIMESTAMP(),
                    UPDATED_TS = CURRENT_TIMESTAMP()
                WHERE TARGET_TABLE_NM IN (
                    'STG_POS_SALES', 'STG_INVENTORY_SNAPSHOT', 'STG_ITEM_MASTER', 'STG_STORE_MASTER',
                    'TMP_POS_SALES', 'TMP_INVENTORY', 'DIM_ITEM', 'DIM_LOCN',
                    'FCT_SALES', 'FCT_INVENTORY', 'AGG_SLS_SUBCLS_LOCN_DAY'
                )
                """,
                (logical_date,),
            )
            watermark_updates = cursor.rowcount
            logging.info(
                "[WATERMARK] Advanced high watermarks to %s for %d target tables.",
                logical_date, watermark_updates,
            )

            cursor.execute(
                """
                INSERT INTO C_BATCH_LOG (
                    BATCH_ID, DAG_ID, LOGICAL_DATE, STAGE_NAME,
                    STATUS, ROWS_READ, ROWS_WRITTEN, ERROR_MSG,
                    START_TS, END_TS
                ) VALUES (
                    %s, %s, %s, 'RECONCILIATION',
                    'SUCCESS', %s, %s, NULL,
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                )
                """,
                (batch_id, dag_id, logical_date, bronze_pos_count, silver_pos_count),
            )

            conn.commit()

        except Exception:
            conn.rollback()
            raise

    audit_summary = {
        "status": "PASSED",
        "logical_date": str(logical_date),
        "pos_volume": {
            "bronze": bronze_pos_count,
            "silver_valid": silver_pos_count,
            "quarantined": quarantine_pos_count,
            "balanced": True,
        },
        "financial_reconciliation": {
            "silver_net_sales": float(silver_net_sales),
            "gold_fct_net_sales": float(gold_fct_net_sales),
            "mart_net_sales": float(mart_net_sales),
            "balanced": True,
        },
        "inventory_volume": {
            "bronze": bronze_inv_count,
            "silver": silver_inv_count,
            "quarantined": quarantine_inv_count,
            "gold_fct": fct_inv_count,
            "balanced": True,
        },
    }

    logging.info(
        "======================================================================"
    )
    logging.info(
        "[RECONCILIATION] PASSED 100%% CERTIFIED INTEGRITY FOR DATE %s",
        logical_date,
    )
    logging.info("  ✓ POS Sales Volume Conservation: MATCH")
    logging.info("  ✓ Financial Net Sales 3-Way Tie: MATCH ($%.2f)", silver_net_sales)
    logging.info("  ✓ Inventory Snapshot Volume & Fact Completeness: MATCH")
    logging.info("  ✓ Watermarks Committed in C_LOAD_DATES")
    logging.info(
        "======================================================================"
    )

    return audit_summary

def _record_failure_log(
    cursor: Any,
    batch_id: str,
    dag_id: str,
    logical_date: date,
    error_msg: str,
) -> None:
    try:
        cursor.execute(
            """
            INSERT INTO C_BATCH_LOG (
                BATCH_ID, DAG_ID, LOGICAL_DATE, STAGE_NAME,
                STATUS, ROWS_READ, ROWS_WRITTEN, ERROR_MSG,
                START_TS, END_TS
            ) VALUES (
                %s, %s, %s, 'RECONCILIATION',
                'FAILED', 0, 0, %s,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """,
            (batch_id, dag_id, logical_date, error_msg[:4000]),
        )
    except Exception as log_err:
        logging.warning("Could not record failure in C_BATCH_LOG: %s", log_err)
