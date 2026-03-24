from db import get_conn

with get_conn() as conn:
    cur = conn.cursor()

    # users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT
    )
    """)

    # user_profiles
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

    # user_bookmarks
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

    # tenders
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

    # tender_matches
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

    # add missing columns safely
    cur.execute("""
    ALTER TABLE tender_matches
    ADD COLUMN IF NOT EXISTS matched_terms TEXT
    """)
    cur.execute("""
    ALTER TABLE tender_matches
    ADD COLUMN IF NOT EXISTS computed_at TEXT
    """)
    cur.execute("""
    ALTER TABLE tenders
    ADD COLUMN IF NOT EXISTS summary TEXT
    """)
    cur.execute("""
    ALTER TABLE tenders
    ADD COLUMN IF NOT EXISTS country TEXT
    """)
    cur.execute("""
    ALTER TABLE tenders
    ADD COLUMN IF NOT EXISTS cpv_code TEXT
    """)
    cur.execute("""
    ALTER TABLE tenders
    ADD COLUMN IF NOT EXISTS cpv_label TEXT
    """)
    cur.execute("""
    ALTER TABLE tenders
    ADD COLUMN IF NOT EXISTS source_url TEXT
    """)
    cur.execute("""
    ALTER TABLE tenders
    ADD COLUMN IF NOT EXISTS notice_id TEXT
    """)

    # backfill computed_at if null
    cur.execute("""
    UPDATE tender_matches
    SET computed_at = NOW()::text
    WHERE computed_at IS NULL
    """)

    conn.commit()

print("Postgres schema fixed.")