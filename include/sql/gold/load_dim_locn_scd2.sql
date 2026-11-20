UPDATE DIM_LOCN
SET CURR_FLG = 'Y',
    EFF_TO_DT = '2100-12-31'::DATE,
    REC_UPDT_TS = CURRENT_TIMESTAMP()
WHERE EFF_TO_DT = DATEADD(day, -1, '{logical_date}'::DATE)
  AND LOCN_ID IN (
      SELECT LOCN_ID FROM DIM_LOCN
      WHERE EFF_FROM_DT = '{logical_date}'::DATE
  );

DELETE FROM DIM_LOCN
WHERE EFF_FROM_DT = '{logical_date}'::DATE;

UPDATE DIM_LOCN AS target
SET target.CURR_FLG    = 'N',
    target.EFF_TO_DT   = DATEADD(day, -1, '{logical_date}'::DATE),
    target.REC_UPDT_TS = CURRENT_TIMESTAMP()
FROM TMP_LOCN AS source
WHERE target.LOCN_ID = source.LOCN_ID
  AND target.CURR_FLG = 'Y'
  AND (
      NOT EQUAL_NULL(target.LOCN_NM,     source.LOCN_NM)
      OR NOT EQUAL_NULL(target.DISTRICT_ID, source.DISTRICT_ID)
      OR NOT EQUAL_NULL(target.REGION_ID,   source.REGION_ID)
      OR NOT EQUAL_NULL(target.STORE_TYPE,  source.STORE_TYPE)
      OR NOT EQUAL_NULL(target.OPEN_DT,     source.OPEN_DT)
      OR NOT EQUAL_NULL(target.CLOSE_DT,    source.CLOSE_DT)
  );

INSERT INTO DIM_LOCN (
    LOCN_KY,
    LOCN_ID,
    LOCN_NM,
    DISTRICT_ID,
    DISTRICT_KY,
    REGION_ID,
    STORE_TYPE,
    OPEN_DT,
    CLOSE_DT,
    CURR_FLG,
    EFF_FROM_DT,
    EFF_TO_DT,
    REC_INS_TS,
    REC_UPDT_TS
)
SELECT
    locn_ky_seq.NEXTVAL,
    s.LOCN_ID,
    s.LOCN_NM,
    s.DISTRICT_ID,
    COALESCE(d.DISTRICT_KY, -1)     AS DISTRICT_KY,
    s.REGION_ID,
    s.STORE_TYPE,
    s.OPEN_DT,
    s.CLOSE_DT,
    'Y'                             AS CURR_FLG,
    '{logical_date}'::DATE          AS EFF_FROM_DT,
    '2100-12-31'::DATE              AS EFF_TO_DT,
    CURRENT_TIMESTAMP()             AS REC_INS_TS,
    CURRENT_TIMESTAMP()             AS REC_UPDT_TS
FROM TMP_LOCN s
JOIN DIM_LOCN t
  ON s.LOCN_ID = t.LOCN_ID
 AND t.EFF_TO_DT = DATEADD(day, -1, '{logical_date}'::DATE)
 AND t.CURR_FLG = 'N'
LEFT JOIN DIM_DISTRICT d
  ON s.DISTRICT_ID = d.DISTRICT_ID;

INSERT INTO DIM_LOCN (
    LOCN_KY,
    LOCN_ID,
    LOCN_NM,
    DISTRICT_ID,
    DISTRICT_KY,
    REGION_ID,
    STORE_TYPE,
    OPEN_DT,
    CLOSE_DT,
    CURR_FLG,
    EFF_FROM_DT,
    EFF_TO_DT,
    REC_INS_TS,
    REC_UPDT_TS
)
SELECT
    locn_ky_seq.NEXTVAL,
    s.LOCN_ID,
    s.LOCN_NM,
    s.DISTRICT_ID,
    COALESCE(d.DISTRICT_KY, -1)     AS DISTRICT_KY,
    s.REGION_ID,
    s.STORE_TYPE,
    s.OPEN_DT,
    s.CLOSE_DT,
    'Y'                             AS CURR_FLG,
    '{logical_date}'::DATE          AS EFF_FROM_DT,
    '2100-12-31'::DATE              AS EFF_TO_DT,
    CURRENT_TIMESTAMP()             AS REC_INS_TS,
    CURRENT_TIMESTAMP()             AS REC_UPDT_TS
FROM TMP_LOCN s
LEFT JOIN DIM_DISTRICT d
  ON s.DISTRICT_ID = d.DISTRICT_ID
WHERE NOT EXISTS (
    SELECT 1 FROM DIM_LOCN t
    WHERE t.LOCN_ID = s.LOCN_ID
);
