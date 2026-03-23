import sqlite3

conn = sqlite3.connect("tenders.db")

count = conn.execute(
    "SELECT count(*) FROM tenders WHERE source='TED'"
).fetchone()[0]

print("Total TED tenders:", count)

conn.close()