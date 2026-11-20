-- =============================================================================
-- POST-BACKFILL VERIFICATION SCRIPT  [Snowflake SQL]
-- File: evidence/verification_queries.sql
-- =============================================================================

USE SCHEMA RETAIL_DWH;


-- ---------------------------------------------------------------------------
-- 1. OVERALL TABLE ROW COUNTS ACROSS ALL LAYERS
-- ---------------------------------------------------------------------------
SELECT 'CONTROL' AS LAYER, 'C_PARAM' AS TABLE_NAME, COUNT(*) AS ROW_CNT FROM C_PARAM UNION ALL
SELECT 'CONTROL', 'C_BATCH_LOG', COUNT(*) FROM C_BATCH_LOG UNION ALL
SELECT 'CONTROL', 'C_LOAD_DATES', COUNT(*) FROM C_LOAD_DATES UNION ALL
SELECT 'CONTROL', 'C_REJ_TABLE', COUNT(*) FROM C_REJ_TABLE UNION ALL
SELECT 'BRONZE', 'STG_POS_SALES', COUNT(*) FROM STG_POS_SALES UNION ALL
SELECT 'BRONZE', 'STG_INVENTORY_SNAPSHOT', COUNT(*) FROM STG_INVENTORY_SNAPSHOT UNION ALL
SELECT 'BRONZE', 'STG_ITEM_MASTER', COUNT(*) FROM STG_ITEM_MASTER UNION ALL
SELECT 'BRONZE', 'STG_STORE_MASTER', COUNT(*) FROM STG_STORE_MASTER UNION ALL
SELECT 'SILVER', 'TMP_POS_SALES', COUNT(*) FROM TMP_POS_SALES UNION ALL
SELECT 'SILVER', 'TMP_INVENTORY', COUNT(*) FROM TMP_INVENTORY UNION ALL
SELECT 'GOLD_DIM', 'DIM_DAY', COUNT(*) FROM DIM_DAY UNION ALL
SELECT 'GOLD_DIM', 'DIM_SUBCLASS', COUNT(*) FROM DIM_SUBCLASS UNION ALL
SELECT 'GOLD_DIM', 'DIM_DISTRICT', COUNT(*) FROM DIM_DISTRICT UNION ALL
SELECT 'GOLD_DIM', 'DIM_ITEM', COUNT(*) FROM DIM_ITEM UNION ALL
SELECT 'GOLD_DIM', 'DIM_LOCN', COUNT(*) FROM DIM_LOCN UNION ALL
SELECT 'GOLD_FCT', 'FCT_SALES', COUNT(*) FROM FCT_SALES UNION ALL
SELECT 'GOLD_FCT', 'FCT_INVENTORY', COUNT(*) FROM FCT_INVENTORY UNION ALL
SELECT 'MART', 'AGG_SLS_SUBCLS_LOCN_DAY', COUNT(*) FROM AGG_SLS_SUBCLS_LOCN_DAY
ORDER BY LAYER, TABLE_NAME;


-- ---------------------------------------------------------------------------
-- 2. FIVE-DAY VOLUME CONSERVATION AUDIT
-- Rule: Bronze Count = Silver Valid Count + Quarantine Count for each day
-- ---------------------------------------------------------------------------
WITH bronze AS (
    SELECT INGEST_DT, COUNT(*) AS bronze_cnt
    FROM STG_POS_SALES
    GROUP BY INGEST_DT
),
silver AS (
    SELECT TXN_DT, COUNT(*) AS silver_cnt
    FROM TMP_POS_SALES
    GROUP BY TXN_DT
),
quarantine AS (
    SELECT LOGICAL_DATE, COUNT(*) AS rej_cnt
    FROM C_REJ_TABLE
    WHERE SRC_FILE_NM LIKE 'pos_sales%'
    GROUP BY LOGICAL_DATE
)
SELECT 
    b.INGEST_DT                                     AS CALENDAR_DATE,
    COALESCE(b.bronze_cnt, 0)                       AS BRONZE_ROWS,
    COALESCE(s.silver_cnt, 0)                       AS SILVER_VALID_ROWS,
    COALESCE(q.rej_cnt, 0)                          AS QUARANTINED_ROWS,
    COALESCE(s.silver_cnt, 0) + COALESCE(q.rej_cnt, 0) AS CONSERVED_TOTAL,
    IFF(b.bronze_cnt = COALESCE(s.silver_cnt, 0) + COALESCE(q.rej_cnt, 0), 'BALANCED', 'MISMATCH') AS STATUS
FROM bronze b
LEFT JOIN silver s ON b.INGEST_DT = s.TXN_DT
LEFT JOIN quarantine q ON b.INGEST_DT = q.LOGICAL_DATE
ORDER BY b.INGEST_DT;


-- ---------------------------------------------------------------------------
-- 3. THREE-WAY FINANCIAL NET SALES RECONCILIATION
-- Rule: Silver Valid Net Sales == Gold Fact Net Sales == Mart Net Sales
-- ---------------------------------------------------------------------------
WITH silver_fin AS (
    SELECT TXN_DT, SUM(SLS_AMT - RTN_AMT) AS silver_net
    FROM TMP_POS_SALES
    GROUP BY TXN_DT
),
gold_fct_fin AS (
    SELECT TXN_DT, SUM(NET_SLS_AMT) AS gold_fct_net
    FROM FCT_SALES
    GROUP BY TXN_DT
),
mart_fin AS (
    SELECT DAY_DT, SUM(NET_SLS_AMT) AS mart_net
    FROM AGG_SLS_SUBCLS_LOCN_DAY
    GROUP BY DAY_DT
)
SELECT 
    s.TXN_DT                                        AS RUN_DATE,
    s.silver_net                                    AS SILVER_NET_SALES,
    g.gold_fct_net                                  AS GOLD_FCT_NET_SALES,
    m.mart_net                                      AS MART_NET_SALES,
    ROUND(s.silver_net - g.gold_fct_net, 4)         AS SILVER_VS_GOLD_DIFF,
    ROUND(g.gold_fct_net - m.mart_net, 4)           AS GOLD_VS_MART_DIFF,
    IFF(
        ABS(s.silver_net - g.gold_fct_net) <= 0.01 
        AND ABS(g.gold_fct_net - m.mart_net) <= 0.01,
        '100% TIED', 'DRIFT DETECTED'
    ) AS AUDIT_VERDICT
FROM silver_fin s
JOIN gold_fct_fin g ON s.TXN_DT = g.TXN_DT
JOIN mart_fin m ON s.TXN_DT = m.DAY_DT
ORDER BY s.TXN_DT;


-- ---------------------------------------------------------------------------
-- 4. DAY 3 QUARANTINE BREAKDOWN (5 DEFECT RECORDS)
-- ---------------------------------------------------------------------------
SELECT 
    REJ_ID,
    RULE_ID,
    RULE_DESC,
    BUSINESS_KEY,
    SRC_ROW_NUM,
    RAW_RECORD_PAYLOAD
FROM C_REJ_TABLE
WHERE LOGICAL_DATE = '2026-03-03'
ORDER BY REJ_ID;


-- ---------------------------------------------------------------------------
-- 5. SCD TYPE 2 VERSION HISTORY VERIFICATION (STORE02 RECLASSIFICATION)
-- Expected: Exactly 2 rows for STORE02 (Version 1 closed, Version 2 active)
-- ---------------------------------------------------------------------------
SELECT 
    LOCN_KY,
    LOCN_ID,
    LOCN_NM,
    DISTRICT_ID,
    DISTRICT_KY,
    CURR_FLG,
    EFF_FROM_DT,
    EFF_TO_DT,
    REC_INS_TS
FROM DIM_LOCN
WHERE LOCN_ID = 'STORE02'
ORDER BY EFF_FROM_DT;


-- ---------------------------------------------------------------------------
-- 6. HIGH WATERMARKS IN C_LOAD_DATES
-- Expected: LAST_SUCCESS_DT = '2026-03-05' across target tables
-- ---------------------------------------------------------------------------
SELECT 
    TARGET_TABLE_NM,
    LAST_SUCCESS_DT,
    LAST_SUCCESS_DT_COMMITTED_TS,
    UPDATED_TS
FROM C_LOAD_DATES
ORDER BY TARGET_TABLE_NM;


-- ---------------------------------------------------------------------------
-- 7. AUDIT TELEMETRY TRAIL IN C_BATCH_LOG
-- ---------------------------------------------------------------------------
SELECT 
    RUN_ID,
    LOGICAL_DATE,
    STAGE_NAME,
    STATUS,
    ROWS_READ,
    ROWS_WRITTEN,
    ERROR_MSG,
    START_TS,
    END_TS
FROM C_BATCH_LOG
ORDER BY RUN_ID DESC
LIMIT 50;
