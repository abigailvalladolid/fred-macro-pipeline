with obs as (
    select * from {{ ref('stg_fred__observations') }}
    where value is not null
),

series as (
    select
        series_id,
        frequency
    from {{ ref('stg_fred__series') }}
)

select
    obs.series_id,
    cast(date_trunc('quarter', obs.observation_date) as date) as quarter_start,
    avg(obs.value) as value,
    count(*) as n_observations,
    count(*) = case series.frequency when 'Monthly' then 3 else 1 end
        as is_complete_quarter
from obs
inner join series using (series_id)
group by obs.series_id, quarter_start, series.frequency
