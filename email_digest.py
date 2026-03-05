import os
import sqlite3
from datetime import datetime
import resend
from dotenv import load_dotenv

DB_PATH = "tenders.db"


def fetch_top_matches(conn, profile_name, min_score, max_items):

    cur = conn.execute(
        """
        SELECT m.score, m.matched_terms,
               t.published_at, t.deadline_at, t.title, t.ca, t.estimated_value, t.link
        FROM tender_matches m
        JOIN tenders t
          ON t.source = m.source AND t.source_id = m.source_id
        WHERE m.profile_name = ?
          AND m.score >= ?
        ORDER BY
            CASE WHEN t.deadline_at IS NULL THEN 1 ELSE 0 END,
            t.deadline_at ASC,
            m.score DESC
        LIMIT ?;
        """,
        (profile_name, min_score, max_items),
    )

    return cur.fetchall()


def build_email(rows):

    lines = []
    lines.append("<h2>Daily Tender Matches</h2>")

    if not rows:
        lines.append("<p>No tenders matched today.</p>")
        return "\n".join(lines)

    lines.append("<ol>")

    for score, matched, pub, deadline, title, ca, est, link in rows:

        lines.append("<li>")
        lines.append(f"<b>{title}</b><br>")
        lines.append(f"Score: {score}<br>")

        if ca:
            lines.append(f"Authority: {ca}<br>")

        if deadline:
            lines.append(f"Deadline: {deadline}<br>")

        if est:
            lines.append(f"Value: {est}<br>")

        lines.append(f"<a href='{link}'>View Tender</a>")
        lines.append("</li><br>")

    lines.append("</ol>")

    return "\n".join(lines)


def main():

    load_dotenv()

    resend.api_key = os.getenv("RESEND_API_KEY")

    to_email = os.getenv("TO_EMAIL")
    profile_name = os.getenv("PROFILE_NAME", "Default")
    min_score = int(os.getenv("MIN_SCORE", "8"))
    max_items = int(os.getenv("MAX_ITEMS", "20"))

    conn = sqlite3.connect(DB_PATH)

    rows = fetch_top_matches(conn, profile_name, min_score, max_items)

    html = build_email(rows)

    params = {
        "from": "Tender Tracker <onboarding@resend.dev>",
        "to": [to_email],
        "subject": f"Tender Matches – {datetime.now().strftime('%Y-%m-%d')}",
        "html": html,
    }

    resend.Emails.send(params)

    print("Email sent successfully")


if __name__ == "__main__":
    main()