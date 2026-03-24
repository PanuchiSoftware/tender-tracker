from db import get_conn

with get_conn() as conn:
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS tender_matches")

    cur.execute("""
    CREATE TABLE tender_matches (
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

print("tender_matches reset successfully.")