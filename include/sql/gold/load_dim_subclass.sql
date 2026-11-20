MERGE INTO DIM_SUBCLASS AS target
USING (

    SELECT
        SUBCLASS_ID,
        SUBCLASS_NM,
        CLASS_ID,
        CLASS_NM,
        DEPT_ID,
        DEPT_NM
    FROM TMP_SUBCLASS
    WHERE SUBCLASS_ID IS NOT NULL
) AS source
ON target.SUBCLASS_ID = source.SUBCLASS_ID

WHEN MATCHED
    AND (
        NOT EQUAL_NULL(target.SUBCLASS_NM, source.SUBCLASS_NM)
        OR NOT EQUAL_NULL(target.CLASS_ID, source.CLASS_ID)
        OR NOT EQUAL_NULL(target.CLASS_NM, source.CLASS_NM)
        OR NOT EQUAL_NULL(target.DEPT_ID, source.DEPT_ID)
        OR NOT EQUAL_NULL(target.DEPT_NM, source.DEPT_NM)
    )
THEN UPDATE SET
    target.SUBCLASS_NM  = source.SUBCLASS_NM,
    target.CLASS_ID     = source.CLASS_ID,
    target.CLASS_NM     = source.CLASS_NM,
    target.DEPT_ID      = source.DEPT_ID,
    target.DEPT_NM      = source.DEPT_NM,
    target.REC_UPDT_TS  = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    SUBCLASS_KY,
    SUBCLASS_ID,
    SUBCLASS_NM,
    CLASS_ID,
    CLASS_NM,
    DEPT_ID,
    DEPT_NM,
    REC_INS_TS,
    REC_UPDT_TS
) VALUES (
    subclass_ky_seq.NEXTVAL,
    source.SUBCLASS_ID,
    source.SUBCLASS_NM,
    source.CLASS_ID,
    source.CLASS_NM,
    source.DEPT_ID,
    source.DEPT_NM,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
