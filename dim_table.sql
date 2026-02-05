--dim_date
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_DATE (
    DATE_KEY        NUMBER NOT NULL,

    FULL_DATE       DATE NOT NULL,
    YEAR            INT,
    QUARTER         INT,
    MONTH           INT,
    MONTH_NAME      VARCHAR,
    DAY_OF_WEEK     INT,
    DAY_NAME        VARCHAR,
    IS_WEEKEND      BOOLEAN,
    IS_HOLIDAY      BOOLEAN,

    CONSTRAINT PK_DIM_DATE
        PRIMARY KEY (DATE_KEY)
);

INSERT INTO AIRFLOW0105.DEV.DIM_DATE (
    DATE_KEY,
    FULL_DATE,
    YEAR,
    QUARTER,
    MONTH,
    MONTH_NAME,
    DAY_OF_WEEK,
    DAY_NAME,
    IS_WEEKEND,
    IS_HOLIDAY
)
SELECT DISTINCT
    TO_NUMBER(TO_CHAR(DATE, 'YYYYMMDD')) AS DATE_KEY,
    DATE                                 AS FULL_DATE,
    YEAR(DATE)                           AS YEAR,
    QUARTER(DATE)                        AS QUARTER,
    MONTH(DATE)                          AS MONTH,
    TO_CHAR(DATE, 'MMMM')                AS MONTH_NAME,
    DAYOFWEEK(DATE)                      AS DAY_OF_WEEK,
    TO_CHAR(DATE, 'DY')                  AS DAY_NAME,
    CASE 
        WHEN DAYOFWEEK(DATE) IN (6, 7) 
        THEN TRUE 
        ELSE FALSE 
    END                                  AS IS_WEEKEND,
    FALSE                                AS IS_HOLIDAY
FROM STG_STOCK_HISTORY;

--merge new data
MERGE INTO AIRFLOW0105.DEV.DIM_DATE tgt
USING (
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(DATE, 'YYYYMMDD')) AS DATE_KEY,
        DATE                                 AS FULL_DATE,
        YEAR(DATE)                           AS YEAR,
        QUARTER(DATE)                        AS QUARTER,
        MONTH(DATE)                          AS MONTH,
        TO_CHAR(DATE, 'MMMM')                AS MONTH_NAME,
        DAYOFWEEK(DATE)                      AS DAY_OF_WEEK,
        TO_CHAR(DATE, 'DY')                  AS DAY_NAME,
        CASE 
            WHEN DAYOFWEEK(DATE) IN (6, 7) 
            THEN TRUE 
            ELSE FALSE 
        END                                  AS IS_WEEKEND,
        FALSE                                AS IS_HOLIDAY
    FROM STG_STOCK_HISTORY
) src
ON tgt.DATE_KEY = src.DATE_KEY

WHEN NOT MATCHED THEN INSERT (
    DATE_KEY,
    FULL_DATE,
    YEAR,
    QUARTER,
    MONTH,
    MONTH_NAME,
    DAY_OF_WEEK,
    DAY_NAME,
    IS_WEEKEND,
    IS_HOLIDAY
)
VALUES (
    src.DATE_KEY,
    src.FULL_DATE,
    src.YEAR,
    src.QUARTER,
    src.MONTH,
    src.MONTH_NAME,
    src.DAY_OF_WEEK,
    src.DAY_NAME,
    src.IS_WEEKEND,
    src.IS_HOLIDAY
);


--dim_company
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_COMPANY (
    COMPANY_KEY     NUMBER AUTOINCREMENT,

    SYMBOL          VARCHAR(16) NOT NULL,
    COMPANY_NAME    VARCHAR,
    CEO             VARCHAR,
    SECTOR          VARCHAR,
    INDUSTRY        VARCHAR,
    EXCHANGE        VARCHAR,
    WEBSITE         VARCHAR,
    BETA            DECIMAL,
    MKTCAP          BIGINT,
    DESCRIPTION     TEXT,

    LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_DIM_COMPANY
        PRIMARY KEY (COMPANY_KEY)
);
INSERT INTO AIRFLOW0105.DEV.DIM_COMPANY (
    SYMBOL,
    COMPANY_NAME,
    CEO,
    SECTOR,
    INDUSTRY,
    EXCHANGE,
    WEBSITE,
    BETA,
    MKTCAP,
    DESCRIPTION
)
SELECT DISTINCT
    s.SYMBOL,
    c.COMPANY_NAME,
    c.CEO,
    c.SECTOR,
    c.INDUSTRY,
    s.EXCHANGE,
    c.WEBSITE,
    c.BETA,
    c.MKTCAP,
    c.DESCRIPTION
FROM STG_SYMBOLS s
LEFT JOIN STG_COMPANY_PROFILE c
  ON s.SYMBOL = c.SYMBOL;


  --merge new data
MERGE INTO AIRFLOW0105.DEV.DIM_COMPANY tgt
USING (
    SELECT DISTINCT
        s.SYMBOL,
        c.COMPANY_NAME,
        c.CEO,
        c.SECTOR,
        c.INDUSTRY,
        s.EXCHANGE,
        c.WEBSITE,
        c.BETA,
        c.MKTCAP,
        c.DESCRIPTION
    FROM STG_SYMBOLS s
    LEFT JOIN STG_COMPANY_PROFILE c
      ON s.SYMBOL = c.SYMBOL
) src
ON tgt.SYMBOL = src.SYMBOL

WHEN MATCHED THEN UPDATE SET
    tgt.COMPANY_NAME = src.COMPANY_NAME,
    tgt.CEO          = src.CEO,
    tgt.SECTOR       = src.SECTOR,
    tgt.INDUSTRY     = src.INDUSTRY,
    tgt.EXCHANGE     = src.EXCHANGE,
    tgt.WEBSITE      = src.WEBSITE,
    tgt.BETA         = src.BETA,
    tgt.MKTCAP       = src.MKTCAP,
    tgt.DESCRIPTION  = src.DESCRIPTION,
    tgt.LOAD_TS      = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    SYMBOL,
    COMPANY_NAME,
    CEO,
    SECTOR,
    INDUSTRY,
    EXCHANGE,
    WEBSITE,
    BETA,
    MKTCAP,
    DESCRIPTION
)
VALUES (
    src.SYMBOL,
    src.COMPANY_NAME,
    src.CEO,
    src.SECTOR,
    src.INDUSTRY,
    src.EXCHANGE,
    src.WEBSITE,
    src.BETA,
    src.MKTCAP,
    src.DESCRIPTION
);
