select
    quarter_start,
    max(case when series_id = 'GDP' then value end) as gdp,
    max(case when series_id = 'UNRATE' then value end) as avg_unemployment_rate,
    max(case when series_id = 'FEDFUNDS' then value end) as avg_fed_funds_rate
from {{ ref('int_observations_quarterly') }}
where is_complete_quarter
group by quarter_start
order by quarter_start
