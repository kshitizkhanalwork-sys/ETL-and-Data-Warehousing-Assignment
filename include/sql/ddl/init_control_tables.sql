CREATE TABLE IF NOT EXISTS C_PARAM (
    PARAM_KEY       VARCHAR(100)    NOT NULL,
    PARAM_VAL       VARCHAR(500)    NOT NULL,
    ENV             VARCHAR(20)     DEFAULT 'DEV',
    REC_INS_TS      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_c_param PRIMARY KEY (PARAM_KEY)
);

MERGE INTO C_PARAM AS target
USING (
    SELECT 'RETAIL_DROP_DIR'  AS PARAM_KEY, '/opt/airflow/data' AS PARAM_VAL, 'DEV' AS ENV UNION ALL
    SELECT 'SCHEMA_NAME',      'RETAIL_DWH',                                  'DEV'          UNION ALL
    SELECT 'ENV',              'DEV',                                          'DEV'          UNION ALL
    SELECT 'DELTA_FLG',        'Y',                                            'DEV'
) AS src ON target.PARAM_KEY = src.PARAM_KEY
WHEN NOT MATCHED THEN
    INSERT (PARAM_KEY, PARAM_VAL, ENV)
    VALUES (src.PARAM_KEY, src.PARAM_VAL, src.ENV);

CREATE TABLE IF NOT EXISTS C_BATCH_SCRIPTS (
    SCRIPT_NM       VARCHAR(200)    NOT NULL,
    SCRIPT_DESC     VARCHAR(500),
    ACTIVE_FLG      CHAR(1)         DEFAULT 'Y',
    LAST_RUN_DT     DATE,
    REC_INS_TS      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    REC_UPDT_TS     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_c_batch_scripts PRIMARY KEY (SCRIPT_NM)
);

CREATE TABLE IF NOT EXISTS C_BATCH_LOG (
    RUN_ID          NUMBER          AUTOINCREMENT PRIMARY KEY,
    BATCH_ID        VARCHAR(200)    NOT NULL,
    DAG_ID          VARCHAR(100)    NOT NULL,
    LOGICAL_DATE    DATE            NOT NULL,
    STAGE_NAME      VARCHAR(100)    NOT NULL,
    STATUS          VARCHAR(20)     NOT NULL,
    ROWS_READ       NUMBER(18,0)    DEFAULT 0,
    ROWS_WRITTEN    NUMBER(18,0)    DEFAULT 0,
    ERROR_MSG       VARCHAR(4000),
    START_TS        TIMESTAMP_NTZ,
    END_TS          TIMESTAMP_NTZ

);

CREATE TABLE IF NOT EXISTS C_LOAD_DATES (
    TARGET_TABLE_NM             VARCHAR(100)    NOT NULL,
    LAST_SUCCESS_DT             DATE            NOT NULL,
    COMPACT_FLG                 CHAR(1)         DEFAULT 'N',
    NM1                         VARCHAR(50),
    NM2                         NUMBER(18,0)    DEFAULT 1,
    LAST_SUCCESS_DT_COMMITTED_TS TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_c_load_dates PRIMARY KEY (TARGET_TABLE_NM)
);

MERGE INTO C_LOAD_DATES AS target
USING (
    SELECT 'STG_POS_SALES'           AS T, '2000-01-01'::DATE AS D UNION ALL
    SELECT 'STG_INVENTORY_SNAPSHOT',          '2000-01-01'           UNION ALL
    SELECT 'STG_ITEM_MASTER',                 '2000-01-01'           UNION ALL
    SELECT 'STG_STORE_MASTER',                '2000-01-01'           UNION ALL
    SELECT 'TMP_POS_SALES',                   '2000-01-01'           UNION ALL
    SELECT 'TMP_INVENTORY',                   '2000-01-01'           UNION ALL
    SELECT 'DIM_DAY',                         '2000-01-01'           UNION ALL
    SELECT 'DIM_ITEM',                        '2000-01-01'           UNION ALL
    SELECT 'DIM_LOCN',                        '2000-01-01'           UNION ALL
    SELECT 'DIM_SUBCLASS',                    '2000-01-01'           UNION ALL
    SELECT 'DIM_DISTRICT',                    '2000-01-01'           UNION ALL
    SELECT 'FCT_SALES',                       '2000-01-01'           UNION ALL
    SELECT 'FCT_INVENTORY',                   '2000-01-01'           UNION ALL
    SELECT 'AGG_SLS_SUBCLS_LOCN_DAY',         '2000-01-01'
) AS src (T, D) ON target.TARGET_TABLE_NM = src.T
WHEN NOT MATCHED THEN
    INSERT (TARGET_TABLE_NM, LAST_SUCCESS_DT)
    VALUES (src.T, src.D);

CREATE TABLE IF NOT EXISTS C_REJ_TABLE (
    REJ_ID              NUMBER          AUTOINCREMENT PRIMARY KEY,
    BATCH_ID            VARCHAR(200)    NOT NULL,
    LOGICAL_DATE        DATE            NOT NULL,
    SRC_FILE_NM         VARCHAR(255)    NOT NULL,
    SRC_ROW_NUM         NUMBER(18,0)    NOT NULL,
    RULE_ID             VARCHAR(50)     NOT NULL,
    RULE_DESC           VARCHAR(500)    NOT NULL,
    BUSINESS_KEY        VARCHAR(200),
    RAW_RECORD_PAYLOAD  VARCHAR(8000)   NOT NULL,
    REJECT_DT           DATE            NOT NULL,
    RESOLVED_FLG        CHAR(1)         DEFAULT 'N',
    REC_INS_TS          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()

);
