import sqlite3

conn = sqlite3.connect("tenders.db")

total_ted = conn.execute(
    "SELECT COUNT(*) FROM tenders WHERE source='TED'"
).fetchone()[0]

matched_ted = conn.execute(
    """
    SELECT COUNT(*)
    FROM tender_matches m
    JOIN tenders t
      ON t.source = m.source AND t.source_id = m.source_id
    WHERE t.source = 'TED'
    """
).fetchone()[0]

scored_ted = conn.execute(
    """
    SELECT COUNT(*)
    FROM tender_matches m
    JOIN tenders t
      ON t.source = m.source AND t.source_id = m.source_id
    WHERE t.source = 'TED'
      AND m.score > 0
    """
).fetchone()[0]

print("Total TED tenders:", total_ted)
print("Matched TED tenders:", matched_ted)
print("Scored TED tenders (>0):", scored_ted)

conn.close()