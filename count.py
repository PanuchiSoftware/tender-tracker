import sqlite3

conn = sqlite3.connect("tenders.db")

count = conn.execute(
    "SELECT COUNT(*) FROM tenders WHERE source = 'ETENDERS_GOV_IE';"
).fetchone()[0]

print("Total ETENDERS_GOV_IE tenders:", count)

conn.close()