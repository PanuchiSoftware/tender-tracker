import sqlite3

DB_PATH = "tenders.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL UNIQUE,
        profile_name TEXT NOT NULL DEFAULT 'Default',
        min_score INTEGER NOT NULL DEFAULT 0,
        source_filter TEXT NOT NULL DEFAULT 'All',
        country_filter TEXT,
        cpv_filter TEXT,
        only_with_deadline INTEGER NOT NULL DEFAULT 0,
        due_within INTEGER NOT NULL DEFAULT 180,
        search_text TEXT,
        sort_by TEXT NOT NULL DEFAULT 'priority desc (recommended)',
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        keyword TEXT NOT NULL,
        weight INTEGER NOT NULL DEFAULT 1,
        UNIQUE(user_id, keyword),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_bookmarks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        source TEXT NOT NULL,
        source_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(user_id, source, source_id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """)

    conn.commit()
    conn.close()
    print("Multi-user tables created.")

if __name__ == "__main__":
    main()