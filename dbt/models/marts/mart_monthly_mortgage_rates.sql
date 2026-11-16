select date_trunc('month', observation_date)::date as month,
       avg(mortgage_rate) as avg_mortgage_rate,
       count(*) as observations
from {{ ref('stg_mortgage_rates') }}
group by 1
