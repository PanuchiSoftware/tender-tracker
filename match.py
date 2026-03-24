import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from db import get_conn

PROFILE_PATH = "profile.json"


def ensure_tender_matches_table() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS tender_matches (
        id SERIAL PRIMARY KEY,
        profile_name TEXT NOT NULL,
        source TEXT NOT NULL,
        source_id TEXT NOT NULL,
        score INTEGER NOT NULL,
        matched_terms TEXT,
        computed_at TEXT NOT NULL,
        UNIQUE(profile_name, source, source_id)
    )
    """

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()


def load_profiles() -> List[Dict[str, Any]]:
    with open(PROFILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "profiles" in data:
        return data["profiles"]

    if isinstance(data, list):
        return data

    if isinstance(data, dict) and "name" in data:
        return [data]

    raise ValueError("profile.json format not recognised")


def normalize_profile(profile: Dict[str, Any]) -> Tuple[str, List[Tuple[str, int]], List[str]]:
    name = profile.get("name", "Default")
    raw_keywords = profile.get("keywords", [])
    raw_exclude = profile.get("exclude", [])

    keywords: List[Tuple[str, int]] = []
    for item in raw_keywords:
        if isinstance(item, str):
            keywords.append((item.strip().lower(), 1))
        elif isinstance(item, dict):
            term = str(item.get("term", "")).strip().lower()
            weight = int(item.get("weight", 1))
            if term:
                keywords.append((term, weight))

    exclude_terms = [str(x).strip().lower() for x in raw_exclude if str(x).strip()]
    return name, keywords, exclude_terms


def fetch_tenders() -> List[Tuple[str, str, str, str, str, str]]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                source,
                source_id,
                COALESCE(title, ''),
                COALESCE(ca, ''),
                COALESCE(cpv_code, ''),
                COALESCE(country, '')
            FROM tenders
            WHERE source IN ('ETENDERS_GOV_IE', 'TED')
            """
        )
        return cur.fetchall()


def score_tender(text: str, keywords: List[Tuple[str, int]], exclude_terms: List[str]) -> Tuple[int, List[str]]:
    score = 0
    matched_terms: List[str] = []
    haystack = text.lower()

    for term in exclude_terms:
        if term and term in haystack:
            return 0, []

    for term, weight in keywords:
        if term and term in haystack:
            score += weight
            matched_terms.append(term)

    return score, matched_terms


def save_matches(profile_name: str, rows: List[Tuple[str, str, int, str]]) -> None:
    ensure_tender_matches_table()
    computed_at = datetime.now(timezone.utc).isoformat()

    delete_sql = "DELETE FROM tender_matches WHERE profile_name = %s"

    insert_sql = """
    INSERT INTO tender_matches (
        profile_name, source, source_id, score, matched_terms, computed_at
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (profile_name, source, source_id) DO UPDATE SET
        score = EXCLUDED.score,
        matched_terms = EXCLUDED.matched_terms,
        computed_at = EXCLUDED.computed_at
    """

    payload = [
        (profile_name, source, source_id, score, matched_terms, computed_at)
        for source, source_id, score, matched_terms in rows
    ]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(delete_sql, (profile_name,))
        if payload:
            cur.executemany(insert_sql, payload)
        conn.commit()


def print_top(profile_name: str, limit: int = 20) -> None:
    sql = """
    SELECT
        m.score,
        t.published_at,
        t.deadline_at,
        t.title,
        t.ca,
        t.estimated_value,
        m.matched_terms,
        t.link
    FROM tender_matches m
    JOIN tenders t
      ON t.source = m.source AND t.source_id = m.source_id
    WHERE m.profile_name = %s
    ORDER BY m.score DESC, t.published_at DESC NULLS LAST
    LIMIT %s
    """

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (profile_name, limit))
        rows = cur.fetchall()

    print(f"\nTop {len(rows)} relevant tenders for profile: {profile_name}\n")
    for row in rows:
        score, published_at, deadline_at, title, ca, estimated_value, matched_terms, link = row
        print(f"Score: {score} | Published: {published_at} | Deadline: {deadline_at}")
        print(f"Title: {title}")
        print(f"CA: {ca}")
        if estimated_value:
            print(f"Est: {estimated_value}")
        if matched_terms:
            print(f"Matched: {matched_terms}")
        print(f"Link: {link}\n")


def main() -> None:
    profiles = load_profiles()
    tenders = fetch_tenders()

    total_written = 0

    for profile in profiles:
        profile_name, keywords, exclude_terms = normalize_profile(profile)
        rows_to_save: List[Tuple[str, str, int, str]] = []

        for source, source_id, title, ca, cpv_code, country in tenders:
            combined_text = " | ".join([title, ca, cpv_code, country])
            score, matched_terms = score_tender(combined_text, keywords, exclude_terms)
            rows_to_save.append((source, source_id, score, ", ".join(matched_terms)))

        save_matches(profile_name, rows_to_save)
        total_written += len(rows_to_save)

        print(f"Computed/updated matches for {len(rows_to_save)} tenders.")
        print_top(profile_name, limit=20)

    print(f"\nFinished. Total match rows written: {total_written}")


if __name__ == "__main__":
    main()