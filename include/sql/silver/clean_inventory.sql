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
    'DUP_SNAP_ROW',
    'Duplicate inventory snapshot: same STORE_ID + ITEM_ID + SNAP_DT',
    COALESCE(STORE_ID, 'NULL') || '-' || COALESCE(ITEM_ID, 'NULL'),
    RAW_PAYLOAD,
    '{logical_date}'::DATE
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY STORE_ID, ITEM_ID, SNAP_DT
               ORDER BY SRC_ROW_NUM ASC
           ) AS RN
    FROM STG_INVENTORY_SNAPSHOT
    WHERE INGEST_DT = '{logical_date}'::DATE
) r
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
    'NEG_STOCK',
    'Stock on hand is negative — physically impossible',
    COALESCE(STORE_ID, 'NULL') || '-' || COALESCE(ITEM_ID, 'NULL'),
    RAW_PAYLOAD,
    '{logical_date}'::DATE
FROM STG_INVENTORY_SNAPSHOT s
WHERE s.INGEST_DT = '{logical_date}'::DATE
  AND TRY_TO_NUMBER(TRIM(s.STOCK_ON_HAND), 18, 4) < 0
  AND s.SRC_ROW_NUM NOT IN (
      SELECT SRC_ROW_NUM FROM C_REJ_TABLE
      WHERE LOGICAL_DATE = '{logical_date}'::DATE
        AND BATCH_ID     = '{batch_id}'
        AND SRC_FILE_NM  = '{src_file_nm}'
  );

DELETE FROM TMP_INVENTORY
WHERE SNAP_DT = '{logical_date}'::DATE;

INSERT INTO TMP_INVENTORY (
    STORE_ID,
    ITEM_ID,
    SNAP_DT,
    STOCK_ON_HAND,
    STOCK_IN_TRANSIT,
    STOCK_RESERVED,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    BATCH_ID
)
SELECT
    TRIM(STORE_ID)                                          AS STORE_ID,
    TRIM(ITEM_ID)                                           AS ITEM_ID,
    TRY_TO_DATE(TRIM(SNAP_DT), 'YYYY-MM-DD')               AS SNAP_DT,
    COALESCE(TRY_TO_NUMBER(TRIM(STOCK_ON_HAND),    18, 4), 0) AS STOCK_ON_HAND,
    COALESCE(TRY_TO_NUMBER(TRIM(STOCK_IN_TRANSIT), 18, 4), 0) AS STOCK_IN_TRANSIT,
    COALESCE(TRY_TO_NUMBER(TRIM(STOCK_RESERVED),   18, 4), 0) AS STOCK_RESERVED,
    SRC_FILE_NM,
    SRC_ROW_NUM,
    '{batch_id}'                                            AS BATCH_ID
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY STORE_ID, ITEM_ID, SNAP_DT
               ORDER BY SRC_ROW_NUM ASC
           ) AS RN
    FROM STG_INVENTORY_SNAPSHOT
    WHERE INGEST_DT = '{logical_date}'::DATE
      AND SRC_ROW_NUM NOT IN (
          SELECT SRC_ROW_NUM FROM C_REJ_TABLE
          WHERE LOGICAL_DATE = '{logical_date}'::DATE
            AND BATCH_ID     = '{batch_id}'
            AND SRC_FILE_NM  = '{src_file_nm}'
      )
) clean_candidates
WHERE RN = 1

  AND NULLIF(TRIM(STORE_ID), '') IS NOT NULL
  AND NULLIF(TRIM(ITEM_ID),  '') IS NOT NULL;
