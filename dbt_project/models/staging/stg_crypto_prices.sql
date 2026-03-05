with source as (
    select
        symbol,
        quote_currency,
        price,
        change_24h,
        to_timestamp(collected_at) as collected_ts,
        to_timestamp(source_last_updated_at) as source_updated_ts
    from {{ source('raw', 'fact_crypto_prices_raw') }}
)

select * from source
