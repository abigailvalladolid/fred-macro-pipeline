select
    series_id,
    title,
    frequency,
    units,
    seasonal_adjustment,
    _loaded_at as loaded_at
from {{ source('fred', 'fred_series') }}
