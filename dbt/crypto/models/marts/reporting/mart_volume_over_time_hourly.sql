with hourly_volume as (
    select
        timestamp_trunc(open_time, hour) as kline_hour,
        symbol,
        sum(volume) as total_volume
    from {{ ref('fct_klines') }}
    where open_time >= timestamp_sub(current_timestamp(), interval 24 hour)
    group by 1, 2
)

select
    kline_hour,
    symbol,
    total_volume
from hourly_volume
order by kline_hour asc, total_volume desc
