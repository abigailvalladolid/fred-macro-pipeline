select
    series_id,
    observation_date,
    value,
    value_prior_year,
    yoy_pct_change,
    series_id || '_' || cast(observation_date as varchar) as observation_id
from {{ ref('int_observations_yoy') }}
