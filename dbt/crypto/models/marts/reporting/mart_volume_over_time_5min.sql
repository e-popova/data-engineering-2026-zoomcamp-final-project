with five_min_volume as (
    select
        TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(close_time), 300) * 300) AS kline_5min,
        symbol,
        sum(volume) as total_volume
    from {{ ref('fct_klines') }}
    where open_time >= timestamp_sub(current_timestamp(), interval 24 hour)
    group by 1, 2
)

select
    kline_5min,
    symbol,
    total_volume
from five_min_volume
order by kline_5min asc, total_volume desc
