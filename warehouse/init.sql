create table if not exists public.fact_crypto_prices_raw (
    id bigserial primary key,
    symbol text not null,
    quote_currency text not null,
    price numeric,
    change_24h numeric,
    source_last_updated_at bigint,
    collected_at bigint not null,
    created_at timestamp default now()
);

create table if not exists public.pipeline_audit (
    id bigserial primary key,
    pipeline_name text not null,
    status text not null,
    executed_at timestamp not null
);
