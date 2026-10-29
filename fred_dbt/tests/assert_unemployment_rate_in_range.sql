select *
from {{ ref('fct_economic_observations') }}
where (series_id = 'UNRATE' or series_id like '__UR')
  and (value < 0 or value > 100)