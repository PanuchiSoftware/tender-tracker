import sqlite3

DB_PATH = "tenders.db"

NEW_COLUMNS = {
    "notice_id": "TEXT",
    "country": "TEXT",
    "cpv_code": "TEXT",
    "cpv_label": "TEXT",
    "source_url": "TEXT",
}

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    existing = {
        row[1]
        for row in cur.execute("PRAGMA table_info(tenders)").fetchall()
    }

    for col, col_type in NEW_COLUMNS.items():
        if col not in existing:
            sql = f"ALTER TABLE tenders ADD COLUMN {col} {col_type};"
            print(f"Adding column: {col}")
            cur.execute(sql)

    conn.commit()
    conn.close()
    print("TED schema migration complete.")

if __name__ == "__main__":
    main()