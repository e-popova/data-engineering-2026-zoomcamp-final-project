with fct_klines as (
    select * from {{ ref('fct_klines') }}
    where open_time >= timestamp_sub(current_timestamp(), interval 24 hour)
),

dim_symbols as (
    select * from {{ ref('dim_symbols') }}
),

joined as (
    select
        s.category,
        sum(f.volume) as total_volume
    from fct_klines f
    left join dim_symbols s on f.symbol = s.symbol
    group by 1
),

final as (
    select
        category,
        total_volume,
        safe_divide(total_volume, sum(total_volume) over ()) as volume_share
    from joined
)

select * from final
order by volume_share desc
