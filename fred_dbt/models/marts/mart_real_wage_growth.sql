with wages as (
    select
        observation_date,
        yoy_pct_change as wage_growth_pct
    from {{ ref('fct_economic_observations') }}
    where series_id = 'CES0500000003'
),
inflation as (
    select
        observation_date,
        yoy_pct_change as inflation_pct
    from {{ ref('fct_economic_observations') }}
    where series_id = 'CPIAUCSL'
)
select
    w.observation_date,
    w.wage_growth_pct,
    i.inflation_pct,
    w.wage_growth_pct - i.inflation_pct as real_wage_growth_pct
from wages as w
inner join inflation as i using (observation_date)
where
    w.wage_growth_pct is not null
    and i.inflation_pct is not null
