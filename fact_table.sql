
--create fact table
CREATE TABLE IF NOT EXISTS AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 (
    STOCK_DAILY_KEY NUMBER AUTOINCREMENT,

    -- Foreign keys
    DATE_KEY        DATE NOT NULL,
    COMPANY_KEY     VARCHAR(16) NOT NULL,

    -- Measures
    OPEN_PRICE      NUMBER(18,6),
    HIGH_PRICE      NUMBER(18,6),
    LOW_PRICE       NUMBER(18,6),
    CLOSE_PRICE     NUMBER(18,6),
    VOLUME          NUMBER(18,0),
	ADJCLOSE_PRICE  NUMBER(18,6),

    VOLAVG          NUMBER(18,6),
    CHANGES         NUMBER(18,6),
    MA_7            NUMBER(18,6),
    MA_30           NUMBER(18,6),
    DAILY_RETURN    NUMBER(18,6),
    DAILY_CHANGE    NUMBER(18,6),

    -- Metadata
    LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_FACT_STOCK_HISTORY_1
        PRIMARY KEY (STOCK_DAILY_KEY)
);

--insert data
INSERT INTO AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 (
    DATE_KEY,
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
    DAILY_CHANGE
)
SELECT
    dd.DATE_KEY,
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
JOIN DIM_COMPANY dc
  ON sh.SYMBOL = dc.SYMBOL;


--merge fact table for new data coming in
MERGE INTO AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 tgt
USING (
   SELECT
        dd.DATE_KEY                    AS DATE_KEY,
        dc.COMPANY_KEY                 AS COMPANY_KEY,

        sh.OPEN                        AS OPEN_PRICE,
        sh.HIGH                        AS HIGH_PRICE,
        sh.LOW                         AS LOW_PRICE,
		sh.ADJCLOSE                    AS ADJCLOSE_PRICE,
        sh.CLOSE                       AS CLOSE_PRICE,
        sh.VOLUME                      AS VOLUME,

        /* ===== Derived metrics ===== */

        -- Daily absolute change
        sh.CLOSE - sh.OPEN             AS DAILY_CHANGE,

        -- Daily return (%)
        CASE 
            WHEN sh.OPEN <> 0 
            THEN (sh.CLOSE - sh.OPEN) / sh.OPEN 
            ELSE NULL 
        END                            AS DAILY_RETURN,

        -- Change vs previous close
        sh.CLOSE 
        - LAG(sh.CLOSE) OVER (
            PARTITION BY dc.COMPANY_KEY
            ORDER BY dd.FULL_DATE
          )                             AS CHANGES,

        -- 7-day moving average (close)
        AVG(sh.CLOSE) OVER (
            PARTITION BY dc.COMPANY_KEY
            ORDER BY dd.FULL_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                               AS MA_7,

        -- 30-day moving average (close)
        AVG(sh.CLOSE) OVER (
            PARTITION BY dc.COMPANY_KEY
            ORDER BY dd.FULL_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                               AS MA_30,

        -- 7-day rolling average volume
        AVG(sh.VOLUME) OVER (
            PARTITION BY dc.COMPANY_KEY
            ORDER BY dd.FULL_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                               AS VOLAVG
    FROM STG_STOCK_HISTORY sh
    JOIN DIM_DATE dd
      ON sh.DATE = dd.FULL_DATE
    JOIN DIM_COMPANY dc
      ON sh.SYMBOL = dc.SYMBOL
) src
ON  tgt.DATE_KEY    = src.DATE_KEY
AND tgt.COMPANY_KEY = src.COMPANY_KEY

WHEN MATCHED THEN UPDATE SET
    tgt.OPEN_PRICE      = src.OPEN,
    tgt.HIGH_PRICE      = src.HIGH,
    tgt.LOW_PRICE       = src.LOW,
    tgt.CLOSE_PRICE     = src.CLOSE,
    tgt.VOLUME          = src.VOLUME,
	tgt.ADJCLOSE_PRICE  = src.ADJCLOSE,
    tgt.VOLAVG          = src.VOLAVG,
    tgt.CHANGES         = src.CHANGES,
    tgt.MA_7            = src.MA_7,
    tgt.MA_30           = src.MA_30,
    tgt.DAILY_RETURN    = src.DAILY_RETURN,
    tgt.DAILY_CHANGE    = src.DAILY_CHANGE,
    tgt.LOAD_TS         = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    DATE_KEY,
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
    DAILY_CHANGE
)
VALUES (
    src.DATE_KEY,
    src.COMPANY_KEY,
    src.OPEN,
    src.HIGH,
    src.LOW,
    src.CLOSE,
	src.VOLUME,
    src.ADJCLOSE,
    src.VOLAVG,
    src.CHANGES,
    src.MA_7,
    src.MA_30,
    src.DAILY_RETURN,
    src.DAILY_CHANGE
);


  




