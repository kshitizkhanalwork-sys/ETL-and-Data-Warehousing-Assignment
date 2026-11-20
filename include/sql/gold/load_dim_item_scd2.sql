UPDATE DIM_ITEM
SET CURR_FLG = 'Y',
    EFF_TO_DT = '2100-12-31'::DATE,
    REC_UPDT_TS = CURRENT_TIMESTAMP()
WHERE EFF_TO_DT = DATEADD(day, -1, '{logical_date}'::DATE)
  AND ITEM_ID IN (
      SELECT ITEM_ID FROM DIM_ITEM
      WHERE EFF_FROM_DT = '{logical_date}'::DATE
  );

DELETE FROM DIM_ITEM
WHERE EFF_FROM_DT = '{logical_date}'::DATE;

UPDATE DIM_ITEM AS target
SET target.CURR_FLG    = 'N',
    target.EFF_TO_DT   = DATEADD(day, -1, '{logical_date}'::DATE),
    target.REC_UPDT_TS = CURRENT_TIMESTAMP()
FROM TMP_ITEM AS source
WHERE target.ITEM_ID = source.ITEM_ID
  AND target.CURR_FLG = 'Y'
  AND (
      NOT EQUAL_NULL(target.ITEM_DESC,   source.ITEM_DESC)
      OR NOT EQUAL_NULL(target.SUBCLASS_ID, source.SUBCLASS_ID)
      OR NOT EQUAL_NULL(target.UNIT_COST,   source.UNIT_COST)
      OR NOT EQUAL_NULL(target.UNIT_RETAIL, source.UNIT_RETAIL)
  );

INSERT INTO DIM_ITEM (
    ITEM_KY,
    ITEM_ID,
    ITEM_DESC,
    SUBCLASS_ID,
    SUBCLASS_KY,
    UNIT_COST,
    UNIT_RETAIL,
    CURR_FLG,
    EFF_FROM_DT,
    EFF_TO_DT,
    REC_INS_TS,
    REC_UPDT_TS
)
SELECT
    item_ky_seq.NEXTVAL,
    s.ITEM_ID,
    s.ITEM_DESC,
    s.SUBCLASS_ID,
    COALESCE(sub.SUBCLASS_KY, -1)   AS SUBCLASS_KY,
    s.UNIT_COST,
    s.UNIT_RETAIL,
    'Y'                             AS CURR_FLG,
    '{logical_date}'::DATE          AS EFF_FROM_DT,
    '2100-12-31'::DATE              AS EFF_TO_DT,
    CURRENT_TIMESTAMP()             AS REC_INS_TS,
    CURRENT_TIMESTAMP()             AS REC_UPDT_TS
FROM TMP_ITEM s
JOIN DIM_ITEM t
  ON s.ITEM_ID = t.ITEM_ID
 AND t.EFF_TO_DT = DATEADD(day, -1, '{logical_date}'::DATE)
 AND t.CURR_FLG = 'N'
LEFT JOIN DIM_SUBCLASS sub
  ON s.SUBCLASS_ID = sub.SUBCLASS_ID;

INSERT INTO DIM_ITEM (
    ITEM_KY,
    ITEM_ID,
    ITEM_DESC,
    SUBCLASS_ID,
    SUBCLASS_KY,
    UNIT_COST,
    UNIT_RETAIL,
    CURR_FLG,
    EFF_FROM_DT,
    EFF_TO_DT,
    REC_INS_TS,
    REC_UPDT_TS
)
SELECT
    item_ky_seq.NEXTVAL,
    s.ITEM_ID,
    s.ITEM_DESC,
    s.SUBCLASS_ID,
    COALESCE(sub.SUBCLASS_KY, -1)   AS SUBCLASS_KY,
    s.UNIT_COST,
    s.UNIT_RETAIL,
    'Y'                             AS CURR_FLG,
    '{logical_date}'::DATE          AS EFF_FROM_DT,
    '2100-12-31'::DATE              AS EFF_TO_DT,
    CURRENT_TIMESTAMP()             AS REC_INS_TS,
    CURRENT_TIMESTAMP()             AS REC_UPDT_TS
FROM TMP_ITEM s
LEFT JOIN DIM_SUBCLASS sub
  ON s.SUBCLASS_ID = sub.SUBCLASS_ID
WHERE NOT EXISTS (
    SELECT 1 FROM DIM_ITEM t
    WHERE t.ITEM_ID = s.ITEM_ID
);
