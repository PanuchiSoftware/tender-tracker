from db import get_conn

with get_conn() as conn:
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tenders (
        id SERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        source_id TEXT NOT NULL,
        title TEXT NOT NULL,
        link TEXT,
        ca TEXT,
        published_at TEXT,
        deadline_at TEXT,
        status TEXT,
        estimated_value TEXT,
        notice_id TEXT,
        country TEXT,
        cpv_code TEXT,
        cpv_label TEXT,
        source_url TEXT,
        summary TEXT,
        UNIQUE(source, source_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tender_matches (
        id SERIAL PRIMARY KEY,
        profile_name TEXT NOT NULL,
        source TEXT NOT NULL,
        source_id TEXT NOT NULL,
        score INTEGER NOT NULL,
        matched_terms TEXT
    )
    """)

    conn.commit()

print("Tender tables created in Postgres.")