--dim_date
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_DATE (
    DATE_KEY        NUMBER NOT NULL,
    FULL_DATE       DATE NOT NULL,

    DAY             NUMBER,
    MONTH           NUMBER,
    YEAR            NUMBER,
    DAY_OF_WEEK     NUMBER,
    WEEK_OF_YEAR    NUMBER,
    MONTH_NAME      VARCHAR(20),
    DAY_NAME        VARCHAR(20),

    LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_KEY)
);

INSERT INTO AIRFLOW0105.DEV.DIM_DATE (
    DATE_KEY,
    FULL_DATE,
    DAY,
    MONTH,
    YEAR,
    DAY_OF_WEEK,
    WEEK_OF_YEAR,
    MONTH_NAME,
    DAY_NAME
)
SELECT DISTINCT
    TO_NUMBER(TO_CHAR(DATE, 'YYYYMMDD'))        AS DATE_KEY,
    DATE                                        AS FULL_DATE,
    DAY(DATE)                                   AS DAY,
    MONTH(DATE)                                 AS MONTH,
    YEAR(DATE)                                  AS YEAR,
    DAYOFWEEK(DATE)                             AS DAY_OF_WEEK,
    WEEKOFYEAR(DATE)                            AS WEEK_OF_YEAR,
    TO_CHAR(DATE, 'MMMM')                       AS MONTH_NAME,
    TO_CHAR(DATE, 'DY')                         AS DAY_NAME
FROM STG_STOCK_HISTORY;

--merge new data
MERGE INTO AIRFLOW0105.DEV.DIM_DATE tgt
USING (
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(DATE, 'YYYYMMDD')) AS DATE_KEY,
        DATE                                  AS FULL_DATE,
        DAY(DATE)                             AS DAY,
        MONTH(DATE)                           AS MONTH,
        YEAR(DATE)                            AS YEAR,
        DAYOFWEEK(DATE)                       AS DAY_OF_WEEK,
        WEEKOFYEAR(DATE)                      AS WEEK_OF_YEAR,
        TO_CHAR(DATE, 'MMMM')                 AS MONTH_NAME,
        TO_CHAR(DATE, 'DY')                   AS DAY_NAME
    FROM STG_STOCK_HISTORY
) src
ON tgt.DATE_KEY = src.DATE_KEY

WHEN NOT MATCHED THEN INSERT (
    DATE_KEY,
    FULL_DATE,
    DAY,
    MONTH,
    YEAR,
    DAY_OF_WEEK,
    WEEK_OF_YEAR,
    MONTH_NAME,
    DAY_NAME
)
VALUES (
    src.DATE_KEY,
    src.FULL_DATE,
    src.DAY,
    src.MONTH,
    src.YEAR,
    src.DAY_OF_WEEK,
    src.WEEK_OF_YEAR,
    src.MONTH_NAME,
    src.DAY_NAME
);

--dim_company
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.DIM_COMPANY (
    COMPANY_KEY     NUMBER AUTOINCREMENT,

    SYMBOL          VARCHAR(16) NOT NULL,
    COMPANY_NAME    VARCHAR(255),
    EXCHANGE        VARCHAR(50),
    SECTOR          VARCHAR(100),
    INDUSTRY        VARCHAR(100),
    COUNTRY         VARCHAR(50),

    LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_DIM_COMPANY PRIMARY KEY (COMPANY_KEY)
);

INSERT INTO AIRFLOW0105.DEV.DIM_COMPANY (
    SYMBOL,
    COMPANY_NAME,
    EXCHANGE,
    SECTOR,
    INDUSTRY,
    COUNTRY
)
SELECT DISTINCT
    s.SYMBOL,
    c.COMPANY_NAME,
    s.EXCHANGE,
    c.SECTOR,
    c.INDUSTRY,
    c.COUNTRY
FROM STG_SYMBOLS s
LEFT JOIN STG_COMPANY_PROFILE c
  ON s.SYMBOL = c.SYMBOL;

  --merge new data
  MERGE INTO AIRFLOW0105.DEV.DIM_COMPANY tgt
USING (
    SELECT DISTINCT
        s.SYMBOL,
        c.COMPANY_NAME,
        s.EXCHANGE,
        c.SECTOR,
        c.INDUSTRY,
        c.COUNTRY
    FROM STG_SYMBOLS s
    LEFT JOIN STG_COMPANY_PROFILE c
      ON s.SYMBOL = c.SYMBOL
) src
ON tgt.SYMBOL = src.SYMBOL

WHEN MATCHED THEN UPDATE SET
    tgt.COMPANY_NAME = src.COMPANY_NAME,
    tgt.EXCHANGE     = src.EXCHANGE,
    tgt.SECTOR       = src.SECTOR,
    tgt.INDUSTRY     = src.INDUSTRY,
    tgt.COUNTRY      = src.COUNTRY,
    tgt.LOAD_TS      = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    SYMBOL,
    COMPANY_NAME,
    EXCHANGE,
    SECTOR,
    INDUSTRY,
    COUNTRY
)
VALUES (
    src.SYMBOL,
    src.COMPANY_NAME,
    src.EXCHANGE,
    src.SECTOR,
    src.INDUSTRY,
    src.COUNTRY
);