{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by={
      "field": "close_time",
      "granularity": "hour"
    },
    cluster_by=['symbol'],
    unique_key='dbt_pk'
  )
}}

with raw_data as (
    select
        symbol,
        open_time,
        close_time,
        open,
        high,
        low,
        close,
        volume,
        trades,
        ingested_at,
        symbol || '-' || cast(close_time as string) as dbt_pk
    from {{ source('crypto_raw', 'klines') }}

    {% if is_incremental() %}
    -- Берем данные только за последние 3 дня, чтобы не сканировать всю историю
    where ingested_at >= (
        select timestamp_sub(max(ingested_at), interval 5 minute)
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select *
    from raw_data
    qualify row_number() over (
        partition by symbol, close_time
        order by ingested_at desc
    ) = 1
)

select * from deduplicated
