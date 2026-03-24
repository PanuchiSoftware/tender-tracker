from db import get_conn

with get_conn() as conn:
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_profiles (
        id SERIAL PRIMARY KEY,
        user_id INTEGER UNIQUE,
        profile_name TEXT,
        min_score INTEGER,
        source_filter TEXT,
        country_filter TEXT,
        cpv_filter TEXT,
        only_with_deadline INTEGER,
        due_within INTEGER,
        search_text TEXT,
        sort_by TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_bookmarks (
        id SERIAL PRIMARY KEY,
        user_id INTEGER,
        source TEXT,
        source_id TEXT,
        created_at TEXT,
        UNIQUE(user_id, source, source_id)
    )
    """)

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
        matched_terms TEXT,
        computed_at TEXT NOT NULL,
        UNIQUE(profile_name, source, source_id)
    )
    """)

    conn.commit()

print("All Postgres tables created successfully.")