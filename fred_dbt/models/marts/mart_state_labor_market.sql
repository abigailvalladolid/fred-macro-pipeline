with national as (
    select
        observation_date,
        value as national_rate
    from {{ ref('fct_economic_observations') }}
    where series_id = 'UNRATE'
),

states as (
    select
        observation_date,
        value as state_rate,
        left(series_id, 2) as state
    from {{ ref('fct_economic_observations') }}
    where series_id like '__UR'
)

select
    s.state,
    s.observation_date,
    s.state_rate,
    n.national_rate,
    s.state_rate - n.national_rate as gap_vs_national
from states as s
inner join national as n using (observation_date)
