import sqlite3

DB_PATH = "tenders.db"

with sqlite3.connect(DB_PATH) as conn:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(source, source_id)
        );
        """
    )
    conn.commit()

print("Bookmarks table ready.")