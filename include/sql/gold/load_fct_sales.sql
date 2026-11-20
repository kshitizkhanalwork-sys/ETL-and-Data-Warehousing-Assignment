DELETE FROM FCT_SALES
WHERE TXN_DT = '{logical_date}'::DATE;

INSERT INTO FCT_SALES (
    DAY_KY,
    ITEM_KY,
    LOCN_KY,
    TXN_ID,
    LINE_ID,
    TXN_TS,
    TXN_DT,
    SLS_QTY,
    SLS_AMT,
    RTN_QTY,
    RTN_AMT,
    NET_SLS_QTY,
    NET_SLS_AMT,
    BATCH_ID,
    REC_INS_TS
)
SELECT
    d.DAY_KY,
    COALESCE(i.ITEM_KY, -1)                 AS ITEM_KY,
    COALESCE(l.LOCN_KY, -1)                 AS LOCN_KY,
    s.TXN_ID,
    s.LINE_ID,
    s.TXN_TS,
    s.TXN_DT,
    s.SLS_QTY,
    s.SLS_AMT,
    s.RTN_QTY,
    s.RTN_AMT,
    (s.SLS_QTY - s.RTN_QTY)                 AS NET_SLS_QTY,
    (s.SLS_AMT - s.RTN_AMT)                 AS NET_SLS_AMT,
    '{batch_id}'                            AS BATCH_ID,
    CURRENT_TIMESTAMP()                     AS REC_INS_TS
FROM TMP_POS_SALES s

JOIN DIM_DAY d
  ON s.TXN_DT = d.CALENDAR_DT

LEFT JOIN DIM_ITEM i
  ON s.ITEM_ID = i.ITEM_ID
 AND s.TXN_DT BETWEEN i.EFF_FROM_DT AND i.EFF_TO_DT

LEFT JOIN DIM_LOCN l
  ON s.STORE_ID = l.LOCN_ID
 AND s.TXN_DT BETWEEN l.EFF_FROM_DT AND l.EFF_TO_DT
WHERE s.TXN_DT = '{logical_date}'::DATE;
