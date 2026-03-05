-- Top ativos com maior volatilidade média de 24h
select
    symbol,
    round(avg(abs(avg_change_24h))::numeric, 2) as avg_abs_24h_change
from public.fct_crypto_prices_hourly
group by symbol
order by avg_abs_24h_change desc;

-- Último preço médio por ativo
select distinct on (symbol)
    symbol,
    collected_hour,
    avg_price
from public.fct_crypto_prices_hourly
order by symbol, collected_hour desc;
