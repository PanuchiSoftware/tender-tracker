import os
import sqlite3
import psycopg2

SQLITE_DB = "tenders.db"
POSTGRES_URL = os.getenv("DATABASE_URL")

if not POSTGRES_URL:
    raise ValueError("DATABASE_URL is not set")

sqlite_conn = sqlite3.connect(SQLITE_DB)
sqlite_cur = sqlite_conn.cursor()

pg_conn = psycopg2.connect(POSTGRES_URL)
pg_cur = pg_conn.cursor()

# Copy tenders
sqlite_cur.execute("""
SELECT
    source, source_id, title, link, ca, published_at, deadline_at,
    status, estimated_value, notice_id, country, cpv_code, cpv_label,
    source_url, summary
FROM tenders
""")

tenders = sqlite_cur.fetchall()

for row in tenders:
    pg_cur.execute("""
    INSERT INTO tenders (
        source, source_id, title, link, ca, published_at, deadline_at,
        status, estimated_value, notice_id, country, cpv_code, cpv_label,
        source_url, summary
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source, source_id) DO NOTHING
    """, row)

# Copy tender_matches
sqlite_cur.execute("""
SELECT
    profile_name, source, source_id, score, matched_terms
FROM tender_matches
""")

matches = sqlite_cur.fetchall()

for row in matches:
    pg_cur.execute("""
    INSERT INTO tender_matches (
        profile_name, source, source_id, score, matched_terms
    )
    VALUES (%s, %s, %s, %s, %s)
    """, row)

pg_conn.commit()

sqlite_conn.close()
pg_conn.close()

print("Copied tenders and tender_matches from SQLite to Postgres.")