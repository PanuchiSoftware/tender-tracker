import os
import sqlite3
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

conn = sqlite3.connect("tenders.db")

rows = conn.execute("""
SELECT id, title
FROM tenders
WHERE summary IS NULL
LIMIT 20
""").fetchall()

for row in rows:
    tender_id = row[0]
    title = row[1]

    prompt = f"Summarise this public procurement tender in two short sentences: {title}"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    summary = response.choices[0].message.content.strip()

    conn.execute(
        "UPDATE tenders SET summary=? WHERE id=?",
        (summary, tender_id)
    )

conn.commit()
conn.close()

print("Summaries generated")