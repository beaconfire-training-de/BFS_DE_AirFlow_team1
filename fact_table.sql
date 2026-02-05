
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
FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
JOIN DIM_DATE dd
  ON sh.DATE = dd.FULL_DATE
JOIN DIM_COMPANY dc
  ON sh.SYMBOL = dc.SYMBOL;

--update with new columns data
UPDATE AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 tgt
SET
    -- Daily absolute change
    DAILY_CHANGE = tgt.CLOSE_PRICE - tgt.OPEN_PRICE,

    -- Daily return
    DAILY_RETURN = CASE
        WHEN tgt.OPEN_PRICE <> 0
        THEN (tgt.CLOSE_PRICE - tgt.OPEN_PRICE) / tgt.OPEN_PRICE
        ELSE NULL
    END,

    -- Change vs previous close
    CHANGES = tgt.CLOSE_PRICE
        - LAG(tgt.CLOSE_PRICE) OVER (
            PARTITION BY tgt.COMPANY_KEY
            ORDER BY tgt.DATE_KEY
        ),

    -- 7-day moving average (close)
    MA_7 = AVG(tgt.CLOSE_PRICE) OVER (
        PARTITION BY tgt.COMPANY_KEY
        ORDER BY tgt.DATE_KEY
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ),

    -- 30-day moving average (close)
    MA_30 = AVG(tgt.CLOSE_PRICE) OVER (
        PARTITION BY tgt.COMPANY_KEY
        ORDER BY tgt.DATE_KEY
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ),

    -- 7-day average volume
    VOLAVG = AVG(tgt.VOLUME) OVER (
        PARTITION BY tgt.COMPANY_KEY
        ORDER BY tgt.DATE_KEY
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ),

    LOAD_TS = CURRENT_TIMESTAMP;
    
--merge fact table for new data coming in
MERGE INTO AIRFLOW0105.DEV.FACT_STOCK_HISTORY_1 tgt
USING (
    SELECT
        dd.DATE_KEY,
        dc.COMPANY_KEY,
        sh.OPEN,
        sh.HIGH,
        sh.LOW,
        sh.CLOSE,
        sh.VOLUME,
               -- Derived metrics
        sh.CLOSE - sh.OPEN AS DAILY_CHANGE,

        CASE
            WHEN sh.OPEN <> 0
            THEN (sh.CLOSE - sh.OPEN) / sh.OPEN
            ELSE NULL
        END AS DAILY_RETURN,

        sh.CLOSE
        - LAG(sh.CLOSE) OVER (
            PARTITION BY dc.COMPANY_KEY
            ORDER BY dd.FULL_DATE
        ) AS CHANGES,

        AVG(sh.CLOSE) OVER (
            PARTITION BY dc.COMPANY_KEY
            ORDER BY dd.FULL_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS MA_7,

        AVG(sh.CLOSE) OVER (
            PARTITION BY dc.COMPANY_KEY
            ORDER BY dd.FULL_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS MA_30,

        AVG(sh.VOLUME) OVER (
            PARTITION BY dc.COMPANY_KEY
            ORDER BY dd.FULL_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS VOLAVG

    FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY sh
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


  




