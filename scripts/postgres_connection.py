import os

import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "warehouse"),
        user=os.getenv("POSTGRES_USER", "warehouse"),
        password=os.getenv("POSTGRES_PASSWORD", "warehouse"),
    )


if __name__ == "__main__":
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT now()")
            print("Database connection OK:", cur.fetchone()[0])
