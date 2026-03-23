import sqlite3

conn = sqlite3.connect("tenders.db")

conn.execute("""
ALTER TABLE tenders
ADD COLUMN summary TEXT
""")

conn.commit()
conn.close()

print("Summary column added")