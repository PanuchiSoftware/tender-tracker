import os
import sqlite3
import psycopg2


def get_conn():

    db_url = os.getenv("DATABASE_URL")

    if db_url:
        return psycopg2.connect(db_url)

    return sqlite3.connect("tenders.db")