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
        user_id INTEGER REFERENCES users(id),
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
        created_at TEXT
    )
    """)

    conn.commit()