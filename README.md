# Housing Market Intelligence

This starter ingests weekly FRED 30-year mortgage rates (`MORTGAGE30US`) into Snowflake and builds a monthly summary with dbt. It runs locally with a single Prefect worker or process.

## How the pipeline works

1. `ingest.py` calculates the date range, requests observations from FRED, drops missing values, and converts rates to numbers. Each observation gets a stable SHA-256 key based on its series ID and date.
2. `warehouse.py` creates `RAW.FRED_MORTGAGE_RATES` when necessary. Its Snowflake `MERGE` inserts new observations and updates existing rows when FRED revises a rate. The loader commits the batch or rolls back the writes on failure.
3. `flow.py` runs extraction and loading as Prefect tasks with retries. It saves the window end in a local JSON checkpoint after the warehouse commit and logs the dates, rows processed, and runtime. The row count includes observations passed to the loader even if their stored values were already current.
4. dbt builds `ANALYTICS.STG_MORTGAGE_RATES` as a view that keeps values from 0 through 30. It builds `ANALYTICS.MART_MONTHLY_MORTGAGE_RATES` as a table with the average rate and observation count for each calendar month with valid observations. dbt checks key uniqueness and required fields.

## Run locally

### 1. Install the project

Use Python 3.11 or newer. From the repository root:

```sh
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
pytest -q
ruff check src tests
```

The unit tests cover key stability, missing observations, overlapping windows, and checkpoint persistence. They run without FRED or Snowflake credentials.

### 2. Configure credentials and Snowflake

```sh
cp .env.example .env
```

Fill in the FRED API key and the Snowflake account, user, password, warehouse, database, and dbt role. Keep credentials in the ignored `.env` file. The Python loader uses the Snowflake user's default role. dbt uses `SNOWFLAKE_ROLE` from the profile.

Run [sql/bootstrap.sql](sql/bootstrap.sql) in Snowflake with a setup role to create `HOUSING_INTELLIGENCE`, `RAW`, and `ANALYTICS`. If you choose a different database name, update the bootstrap SQL and `SNOWFLAKE_DATABASE` together. The loader's role needs warehouse and database usage, USAGE on RAW, CREATE TABLE on RAW, and INSERT and UPDATE on the raw table if it is owned by another role. The dbt role needs warehouse and database usage, USAGE on RAW and ANALYTICS, SELECT on the raw table, and CREATE VIEW and CREATE TABLE on ANALYTICS.

### 3. Load FRED observations

```sh
python -m housing_pipeline.flow
```

The first run requests the previous 365 days through today. Later runs start 35 days before the last successful window end by default so that recent revisions are picked up. Set `LOOKBACK_DAYS` in `.env` to adjust that overlap. The default checkpoint is `.state/fred.json` and `CHECKPOINT_PATH` overrides it.

### 4. Build and check the dbt models

From the repository root, use the example profile if you need a new one. If you already have a dbt profile file, add the `housing_intelligence` entry without overwriting existing profiles.

```sh
mkdir -p ~/.dbt
cp -n dbt/profiles.yml.example ~/.dbt/profiles.yml
set -a
. ./.env
set +a
cd dbt
dbt debug
dbt build
```

Use shell-compatible quoting in `.env` for values with spaces or shell metacharacters. dbt reads the exported environment variables.

## Reruns and backfills

Run the same command again after a failed load. A failed warehouse write leaves the checkpoint at the previous successful date. If the warehouse commit succeeds but the checkpoint write fails, the next run processes the window again using the same row keys. To cover earlier revisions, temporarily increase `LOOKBACK_DAYS` on a run with an existing checkpoint. The first-run window is fixed at 365 days in the current implementation.

## Current scope and next milestones

The checkpoint lives on disk and supports one ingestion process. The loader executes one `MERGE` per row. Larger or concurrent deployments need durable shared run state, a lock, and a set-based merge.

Future work includes Census building permits, a housing-price source, freshness and quality checks, run auditing and notifications, scheduled Prefect deployment, CI/CD, and a dashboard.
