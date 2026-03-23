import os
import sqlite3

try:
    import psycopg2
except ImportError:
    psycopg2 = None


def is_postgres():
    db_url = os.getenv("DATABASE_URL")
    return bool(db_url and db_url.startswith("postgres"))


def get_conn():
    db_url = os.getenv("DATABASE_URL")

    if is_postgres():
        if psycopg2 is None:
            raise ImportError("psycopg2 is not installed. Run: pip install psycopg2-binary")
        return psycopg2.connect(db_url)

    return sqlite3.connect("tenders.db")