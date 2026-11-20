DELETE FROM AGG_SLS_SUBCLS_LOCN_DAY
WHERE DAY_DT = '{logical_date}'::DATE;

INSERT INTO AGG_SLS_SUBCLS_LOCN_DAY (
    DAY_KY,
    SUBCLASS_KY,
    LOCN_KY,
    DAY_DT,
    TOTAL_SLS_QTY,
    TOTAL_SLS_AMT,
    TOTAL_RTN_QTY,
    TOTAL_RTN_AMT,
    NET_SLS_QTY,
    NET_SLS_AMT,
    TXN_LINE_CNT,
    BATCH_ID,
    REC_INS_TS
)
SELECT
    f.DAY_KY,
    COALESCE(i.SUBCLASS_KY, -1)             AS SUBCLASS_KY,
    f.LOCN_KY,
    f.TXN_DT                                AS DAY_DT,

    SUM(f.SLS_QTY)                          AS TOTAL_SLS_QTY,
    SUM(f.SLS_AMT)                          AS TOTAL_SLS_AMT,
    SUM(f.RTN_QTY)                          AS TOTAL_RTN_QTY,
    SUM(f.RTN_AMT)                          AS TOTAL_RTN_AMT,
    SUM(f.NET_SLS_QTY)                      AS NET_SLS_QTY,
    SUM(f.NET_SLS_AMT)                      AS NET_SLS_AMT,
    COUNT(*)                                AS TXN_LINE_CNT,
    '{batch_id}'                            AS BATCH_ID,
    CURRENT_TIMESTAMP()                     AS REC_INS_TS
FROM FCT_SALES f
JOIN DIM_ITEM i
  ON f.ITEM_KY = i.ITEM_KY
WHERE f.TXN_DT = '{logical_date}'::DATE
GROUP BY
    f.DAY_KY,
    COALESCE(i.SUBCLASS_KY, -1),
    f.LOCN_KY,
    f.TXN_DT;
