
--create fact table
create table if not exists AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 (
    PK_STOCK_HISTORY_ID NUMBER AUTOINCREMENT,

    -- Foreign keys
    DATE_KEY        DATE NOT NULL,
    SYMBOL           VARCHAR(16) NOT NULL,
    COMPANY_KEY       VARCHAR(16) not null,

    -- Measures
    OPEN_PRICE           NUMBER(18,6),
    HIGH_PRICE           NUMBER(18,6),
    LOW_PRICE            NUMBER(18,6),
    CLOSE_PRICE          NUMBER(18,6),
    VOLUME               NUMBER(18,0),
    ADJCLOSE_PRICE      NUMBER(18,6),
    VOLAVG          NUMBER(18,6),
    CHANGES         NUMBER(18,6),
    MA_7            NUMBER(18,6),
    MA_30           NUMBER(18,6),
    DAILY_RETURN    NUMBER(18,6),
    DAILY_CHANGE    NUMBER(18,6),

    -- Metadata
    LOAD_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_fact_1 PRIMARY KEY (PK_STOCK_HISTORY_ID)
);

insert into AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 (
    DATE_KEY,
    SYMBOL,
    COMPANY_KEY,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    CLOSE_PRICE,
    VOLUME,
    ADJCLOSE_PRICE,
    VOLAVG,
    CHANGES,
    MA_7,
    MA_30,
    DAILY_RETURN,
    DAILY_CHANGE,
    
)
SELECT
    dd.DATE_KEY,
    ds.SYMBOL_KEY,
    dc.COMPANY_KEY,
    sh.OPEN,
    sh.HIGH,
    sh.LOW,
    sh.CLOSE,
    sh.VOLUME,
    sh.ADJCLOSE,
    sh.VOLAVG,
    sh.CHANGES,
    sh.MA_7,
    sh.MA_30,
    sh.DAILY_RETURN,
    sh.DAILY_CHANGE
FROM STG_STOCK_HISTORY sh
JOIN DIM_DATE dd
  ON sh.DATE = dd.FULL_DATE
JOIN DIM_SYMBOL ds
  ON sh.SYMBOL = ds.SYMBOL_KEY
JOIN DIM_COMPANY dc
  ON sh.SYMBOL = dc.SYMBOL



--merge fact table

MERGE INTO AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 tgt
USING (
    SELECT
        dd.DATE_KEY,
        ds.SYMBOL_KEY,
        dc.COMPANY_KEY,
        sh.OPEN,
        sh.HIGH,
        sh.LOW,
        sh.CLOSE,
        sh.VOLUME,
		sh.ADJCLOSE,
        sh.VOLAVG,
        sh.CHANGES,
        sh.MA_7,
        sh.MA_30,
        sh.DAILY_RETURN,
        sh.DAILY_CHANGE
    FROM STG_STOCK_HISTORY sh
    JOIN DIM_DATE dd
      ON sh.DATE = dd.FULL_DATE
    JOIN DIM_SYMBOL ds
      ON sh.SYMBOL = ds.SYMBOL_KEY
    JOIN DIM_COMPANY dc
      ON sh.SYMBOL = dc.SYMBOL
) src
ON  tgt.DATE_KEY    = src.DATE
AND tgt.SYMBOL_KEY  = src.SYMBOL
AND tgt.COMPANY_KEY = src.COMPANY_KEY

WHEN MATCHED THEN UPDATE SET
    tgt.OPEN_PRICE   = src.OPEN,
    tgt.HIGH_PRICE   = src.HIGH,
    tgt.LOW_PRICE    = src.LOW,
    tgt.CLOSE_PRICE  = src.CLOSE,
    tgt.VOLUME       = src.VOLUME,
	tgt.ADJCLOSE_PRICE    = src.ADJ_CLOSE,
    tgt.VOLAVG       = src.VOLAVG,
    tgt.CHANGES      = src.CHANGES,
    tgt.MA_7         = src.MA_7,
    tgt.MA_30        = src.MA_30,
    tgt.DAILY_RETURN = src.DAILY_RETURN,
    tgt.DAILY_CHANGE = src.DAILY_CHANGE,
    tgt.LOAD_TS      = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    DATE_KEY,
    SYMBOL_KEY,
    COMPANY_KEY,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    CLOSE_PRICE,
	VOLUME,
    ADJ_CLOSE,
    VOLAVG,
    CHANGES,
    MA_7,
    MA_30,
    DAILY_RETURN,
    DAILY_CHANGE
)
VALUES (
    src.DATE,
    src.SYMBOL,
    src.COMPANY_KEY,
    src.OPEN,
    src.HIGH,
    src.LOW,
    src.CLOSE,
    src.VOLUME,
	src.ADJ_CLOSE,
    src.VOLAVG,
    src.CHANGES,
    src.MA_7,
    src.MA_30,
    src.DAILY_RETURN,
    src.DAILY_CHANGE
);

  




