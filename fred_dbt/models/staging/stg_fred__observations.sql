with source as (
    select * from {{ source('fred', 'fred_observations') }}
),
cleaned as (
    select
        series_id,
        observation_date,
        cast(nullif(value, '.') as double) as value,
        realtime_start,
        realtime_end,
        _loaded_at as loaded_at
    from source
)
select * from cleaned
