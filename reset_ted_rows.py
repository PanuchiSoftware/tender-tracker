import sqlite3

conn = sqlite3.connect("tenders.db")
conn.execute("DELETE FROM tenders WHERE source = 'TED'")
conn.commit()
conn.close()

print("Deleted old TED rows.")