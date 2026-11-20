DELETE FROM C_REJ_TABLE
WHERE LOGICAL_DATE = '{logical_date}'::DATE
  AND BATCH_ID     = '{batch_id}'
  AND SRC_FILE_NM  = '{src_file_nm}';

INSERT INTO C_REJ_TABLE (
    BATCH_ID, LOGICAL_DATE, SRC_FILE_NM, SRC_ROW_NUM,
    RULE_ID, RULE_DESC, BUSINESS_KEY, RAW_RECORD_PAYLOAD, REJECT_DT
)
SELECT
    '{batch_id}',
    '{logical_date}'::DATE,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    'DUP_TXN_LINE',
    'Duplicate transaction line: same TXN_ID + LINE_ID + TXN_DT',
    COALESCE(TXN_ID, 'NULL') || '-' || COALESCE(LINE_ID, 'NULL'),
    RAW_PAYLOAD,
    '{logical_date}'::DATE
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY TXN_ID, LINE_ID, TXN_DT
               ORDER BY SRC_ROW_NUM ASC
           ) AS RN
    FROM STG_POS_SALES
    WHERE INGEST_DT = '{logical_date}'::DATE
) ranked_rows
WHERE RN > 1;

INSERT INTO C_REJ_TABLE (
    BATCH_ID, LOGICAL_DATE, SRC_FILE_NM, SRC_ROW_NUM,
    RULE_ID, RULE_DESC, BUSINESS_KEY, RAW_RECORD_PAYLOAD, REJECT_DT
)
SELECT
    '{batch_id}',
    '{logical_date}'::DATE,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    'NULL_ITEM_ID',
    'Item ID is NULL or empty — cannot link to item master',
    'NO_ITEM_ID',
    RAW_PAYLOAD,
    '{logical_date}'::DATE
FROM STG_POS_SALES s
WHERE s.INGEST_DT = '{logical_date}'::DATE

  AND NULLIF(TRIM(s.ITEM_ID), '') IS NULL

  AND s.SRC_ROW_NUM NOT IN (
      SELECT SRC_ROW_NUM FROM C_REJ_TABLE
      WHERE LOGICAL_DATE = '{logical_date}'::DATE
        AND BATCH_ID     = '{batch_id}'
        AND SRC_FILE_NM  = '{src_file_nm}'
  );

INSERT INTO C_REJ_TABLE (
    BATCH_ID, LOGICAL_DATE, SRC_FILE_NM, SRC_ROW_NUM,
    RULE_ID, RULE_DESC, BUSINESS_KEY, RAW_RECORD_PAYLOAD, REJECT_DT
)
SELECT
    '{batch_id}',
    '{logical_date}'::DATE,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    'NEG_SLS_QTY',
    'Sales quantity is negative — only return quantity may be non-zero',
    COALESCE(TXN_ID, 'NULL') || '-' || COALESCE(LINE_ID, 'NULL'),
    RAW_PAYLOAD,
    '{logical_date}'::DATE
FROM STG_POS_SALES s
WHERE s.INGEST_DT = '{logical_date}'::DATE

  AND TRY_TO_NUMBER(TRIM(s.SLS_QTY), 18, 4) < 0

  AND s.SRC_ROW_NUM NOT IN (
      SELECT SRC_ROW_NUM FROM C_REJ_TABLE
      WHERE LOGICAL_DATE = '{logical_date}'::DATE
        AND BATCH_ID     = '{batch_id}'
        AND SRC_FILE_NM  = '{src_file_nm}'
  );

INSERT INTO C_REJ_TABLE (
    BATCH_ID, LOGICAL_DATE, SRC_FILE_NM, SRC_ROW_NUM,
    RULE_ID, RULE_DESC, BUSINESS_KEY, RAW_RECORD_PAYLOAD, REJECT_DT
)
SELECT
    '{batch_id}',
    '{logical_date}'::DATE,
    s.SRC_FILE_NM,
    s.SRC_ROW_NUM,
    'ORPHAN_ITEM_FK',
    'ITEM_ID not found in item master (TMP_ITEM) — referential integrity failure',
    COALESCE(s.TXN_ID, 'NULL') || '-' || COALESCE(s.ITEM_ID, 'NULL'),
    s.RAW_PAYLOAD,
    '{logical_date}'::DATE
FROM STG_POS_SALES s

LEFT JOIN TMP_ITEM i ON TRIM(s.ITEM_ID) = i.ITEM_ID
WHERE s.INGEST_DT = '{logical_date}'::DATE

  AND i.ITEM_ID IS NULL

  AND NULLIF(TRIM(s.ITEM_ID), '') IS NOT NULL

  AND s.SRC_ROW_NUM NOT IN (
      SELECT SRC_ROW_NUM FROM C_REJ_TABLE
      WHERE LOGICAL_DATE = '{logical_date}'::DATE
        AND BATCH_ID     = '{batch_id}'
        AND SRC_FILE_NM  = '{src_file_nm}'
  );

DELETE FROM TMP_POS_SALES
WHERE TXN_DT = '{logical_date}'::DATE;

INSERT INTO TMP_POS_SALES (
    TXN_ID,
    LINE_ID,
    TXN_TS,
    TXN_DT,
    STORE_ID,
    ITEM_ID,
    SLS_QTY,
    SLS_AMT,
    RTN_QTY,
    RTN_AMT,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    BATCH_ID
)
SELECT
    TRIM(TXN_ID)                                                AS TXN_ID,
    TRIM(LINE_ID)                                               AS LINE_ID,

    TRY_TO_TIMESTAMP_NTZ(TRIM(TXN_TS), 'YYYY-MM-DD HH24:MI:SS') AS TXN_TS,

    TRY_TO_DATE(TRIM(TXN_DT), 'YYYY-MM-DD')                    AS TXN_DT,
    TRIM(STORE_ID)                                              AS STORE_ID,
    TRIM(ITEM_ID)                                               AS ITEM_ID,

    TRY_TO_NUMBER(TRIM(SLS_QTY), 18, 4)                        AS SLS_QTY,
    TRY_TO_NUMBER(TRIM(SLS_AMT), 18, 4)                        AS SLS_AMT,
    TRY_TO_NUMBER(TRIM(RTN_QTY), 18, 4)                        AS RTN_QTY,
    TRY_TO_NUMBER(TRIM(RTN_AMT), 18, 4)                        AS RTN_AMT,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    '{batch_id}'                                                AS BATCH_ID
FROM (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY TXN_ID, LINE_ID, TXN_DT
               ORDER BY SRC_ROW_NUM ASC
           ) AS RN
    FROM STG_POS_SALES
    WHERE INGEST_DT = '{logical_date}'::DATE

      AND SRC_ROW_NUM NOT IN (
          SELECT SRC_ROW_NUM FROM C_REJ_TABLE
          WHERE LOGICAL_DATE = '{logical_date}'::DATE
            AND BATCH_ID     = '{batch_id}'
            AND SRC_FILE_NM  = '{src_file_nm}'
      )
) clean_candidates
WHERE RN = 1;
