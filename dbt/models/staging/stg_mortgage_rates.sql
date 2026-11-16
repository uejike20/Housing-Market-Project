select record_key, series_id, observation_date, value as mortgage_rate,
       ingested_at
from {{ source('raw', 'FRED_MORTGAGE_RATES') }}
where value >= 0 and value <= 30
