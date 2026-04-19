{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by={
      "field": "close_time",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    cluster_by=['symbol'],
    unique_key='dbt_pk'
  )
}}

with stg_klines as (
    select
        *
    from {{ ref('stg_crypto_raw__klines') }}

    {% if is_incremental() %}
        where ingested_at >= (
            select timestamp_sub(max(ingested_at), interval 5 minute)
            from {{ this }}
        )
    {% endif %}
)

select * from stg_klines