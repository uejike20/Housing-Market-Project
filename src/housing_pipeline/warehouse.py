"""Snowflake transactional upsert; no credentials stored in source control."""
import os
import snowflake.connector

DDL = """CREATE TABLE IF NOT EXISTS RAW.FRED_MORTGAGE_RATES (
    RECORD_KEY VARCHAR NOT NULL, SERIES_ID VARCHAR NOT NULL,
    OBSERVATION_DATE DATE NOT NULL, VALUE FLOAT NOT NULL,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)"""
MERGE = """MERGE INTO RAW.FRED_MORTGAGE_RATES t
USING (SELECT %s AS RECORD_KEY, %s AS SERIES_ID,
       TO_DATE(%s) AS OBSERVATION_DATE, %s AS VALUE) s
ON t.RECORD_KEY = s.RECORD_KEY
WHEN MATCHED AND t.VALUE <> s.VALUE THEN UPDATE SET
    t.VALUE = s.VALUE, t.INGESTED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (RECORD_KEY, SERIES_ID, OBSERVATION_DATE, VALUE)
VALUES (s.RECORD_KEY, s.SERIES_ID, s.OBSERVATION_DATE, s.VALUE)"""


def connect():
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing Snowflake configuration: {', '.join(missing)}")
    return snowflake.connector.connect(account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"], database=os.environ["SNOWFLAKE_DATABASE"],
        schema="RAW", autocommit=False)


def upsert(rows: list[dict]) -> int:
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(DDL)
                for row in rows:
                    cur.execute(MERGE, (row["record_key"], row["series_id"],
                                        row["observation_date"], row["value"]))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return len(rows)
