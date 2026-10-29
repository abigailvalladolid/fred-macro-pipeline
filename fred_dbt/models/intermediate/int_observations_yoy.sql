with obs as (
    select * from {{ ref('stg_fred__observations') }}
    where value is not null
)
select
    cur.series_id,
    cur.observation_date,
    cur.value,
    prev.value as value_prior_year,
    (cur.value / prev.value - 1) * 100 as yoy_pct_change
from obs as cur
left join obs as prev
    on
        cur.series_id = prev.series_id
        and prev.observation_date = cast(cur.observation_date - interval 1 year as date)
