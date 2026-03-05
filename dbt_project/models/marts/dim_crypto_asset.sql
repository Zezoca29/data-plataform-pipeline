select distinct
    symbol,
    quote_currency
from {{ ref('stg_crypto_prices') }}
