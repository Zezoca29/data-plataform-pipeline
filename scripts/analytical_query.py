from postgres_connection import get_connection

QUERY = """
select
    symbol,
    round(avg(avg_price)::numeric, 2) as rolling_avg_price,
    round(avg(avg_change_24h)::numeric, 2) as rolling_avg_change
from public.fct_crypto_prices_hourly
group by symbol
order by rolling_avg_change desc;
"""


if __name__ == "__main__":
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            for row in cur.fetchall():
                print(row)
