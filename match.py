import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

DB_PATH = "tenders.db"
PROFILE_PATH = "profile.json"


@dataclass
class Keyword:
    term: str
    weight: int


def load_profile(path: str) -> Tuple[str, List[Keyword], List[str]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    name = data.get("name", "Profile")
    keywords = [Keyword(k["term"].lower().strip(), int(k.get("weight", 1))) for k in data.get("keywords", [])]
    exclude = [x.lower().strip() for x in data.get("exclude", [])]
    return name, keywords, exclude


def normalize(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().lower()


def score_text(text: str, keywords: List[Keyword]) -> Tuple[int, List[str]]:
    """
    Score:
    - If keyword term appears in text -> add weight
    - Return UNIQUE matched terms (no duplicates)
    """
    score = 0
    matched_set = []
    seen = set()

    for k in keywords:
        if k.term and k.term in text:
            score += k.weight
            if k.term not in seen:
                matched_set.append(k.term)
                seen.add(k.term)

    return score, matched_set


def is_excluded(text: str, exclude_terms: List[str]) -> bool:
    for x in exclude_terms:
        if x and x in text:
            return True
    return False


def ensure_match_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tender_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_id TEXT NOT NULL,
            profile_name TEXT NOT NULL,
            score INTEGER NOT NULL,
            matched_terms TEXT,
            computed_at TEXT NOT NULL,
            UNIQUE(source, source_id, profile_name)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tender_matches_score ON tender_matches(score);")
    conn.commit()


def compute_matches(conn: sqlite3.Connection, profile_name: str, keywords: List[Keyword], exclude_terms: List[str]) -> int:
    """
    Compute score based ONLY on tender content fields (title + CA + estimated_value).
    IMPORTANT: do NOT score URLs (they create false matches like 'api' from 'prepare').
    """
    ensure_match_table(conn)

    cur = conn.execute(
        """
        SELECT source, source_id, title, ca, estimated_value, published_at
        FROM tenders
        WHERE source IN ('ETENDERS_GOV_IE', 'TED')
        ORDER BY published_at DESC
        LIMIT 500;
        """
    )
    rows = cur.fetchall()

    now = datetime.utcnow().isoformat()
    upserts = 0

    for source, source_id, title, ca, estimated_value, published_at in rows:
        # Only real content fields:
        blob = " ".join([normalize(title), normalize(ca), normalize(estimated_value)])

        if is_excluded(blob, exclude_terms):
            score = 0
            matched_terms = []
        else:
            score, matched_terms = score_text(blob, keywords)

        conn.execute(
            """
            INSERT INTO tender_matches (source, source_id, profile_name, score, matched_terms, computed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, source_id, profile_name) DO UPDATE SET
                score=excluded.score,
                matched_terms=excluded.matched_terms,
                computed_at=excluded.computed_at
            ;
            """,
            (source, source_id, profile_name, score, ", ".join(matched_terms), now),
        )
        upserts += 1

    conn.commit()
    return upserts


def print_top(conn: sqlite3.Connection, profile_name: str, limit: int = 20) -> None:
    cur = conn.execute(
        """
        SELECT m.score, m.matched_terms,
               t.published_at, t.deadline_at, t.title, t.ca, t.estimated_value, t.link
        FROM tender_matches m
        JOIN tenders t
          ON t.source = m.source AND t.source_id = m.source_id
        WHERE m.profile_name = ?
          AND t.source = 'ETENDERS_GOV_IE'
        ORDER BY m.score DESC, t.published_at DESC
        LIMIT ?;
        """,
        (profile_name, limit),
    )
    rows = cur.fetchall()

    print(f"\nTop {len(rows)} relevant tenders for profile: {profile_name}\n")
    for score, matched_terms, published_at, deadline_at, title, ca, estimated_value, link in rows:
        print(f"Score: {score} | Published: {published_at} | Deadline: {deadline_at}")
        print(f"Title: {title}")
        if ca:
            print(f"CA: {ca}")
        if estimated_value:
            print(f"Est: {estimated_value}")
        if matched_terms:
            print(f"Matched: {matched_terms}")
        print(f"Link: {link}\n")


def main() -> None:
    profile_name, keywords, exclude_terms = load_profile(PROFILE_PATH)

    with sqlite3.connect(DB_PATH) as conn:
        n = compute_matches(conn, profile_name, keywords, exclude_terms)
        print(f"Computed/updated matches for {n} tenders.")
        print_top(conn, profile_name, limit=20)


if __name__ == "__main__":
    main()