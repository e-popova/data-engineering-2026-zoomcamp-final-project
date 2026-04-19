with fct_klines as (
    select * from {{ ref('fct_klines') }}
    where open_time >= timestamp_sub(current_timestamp(), interval 24 hour)
),

volume_by_symbol as (
    select
        symbol,
        sum(volume) as total_volume
    from fct_klines
    group by 1
),

final as (
    select
        symbol,
        total_volume,
        safe_divide(total_volume, sum(total_volume) over ()) as volume_share
    from volume_by_symbol
)

select * from final
order by volume_share desc

