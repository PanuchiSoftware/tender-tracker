import os
import psycopg2


def get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL is not set")
    return psycopg2.connect(db_url)