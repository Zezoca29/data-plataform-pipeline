select
    symbol,
    quote_currency,
    date_trunc('hour', collected_ts) as collected_hour,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    avg(change_24h) as avg_change_24h,
    count(*) as records_in_hour
from {{ ref('stg_crypto_prices') }}
group by 1,2,3
