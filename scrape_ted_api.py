import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

DB_PATH = "tenders.db"
TED_SEARCH_URL = "https://api.ted.europa.eu/v3/notices/search"

# Start small
PAGE_SIZE = 50
MAX_PAGES = 5
LOOKBACK_DAYS = 14


@dataclass
class Tender:
    source: str
    source_id: str
    title: str
    link: str
    ca: Optional[str]
    published_at: Optional[str]
    deadline_at: Optional[str]
    status: Optional[str]
    estimated_value: Optional[str]
    notice_id: Optional[str]
    country: Optional[str]
    cpv_code: Optional[str]
    cpv_label: Optional[str]
    source_url: Optional[str]


def normalize_dt(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = str(value).strip()
    try:
        # Handles ISO-like values if TED gives them
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return value


def expert_query() -> str:
    today = datetime.now(timezone.utc).date()
    since = today - timedelta(days=LOOKBACK_DAYS)
    # TED help docs show expert search supports publication-date queries like publication-date=YYYYMMDD.
    # We use a range.
    return f"publication-date>={since.strftime('%Y%m%d')} AND publication-date<={today.strftime('%Y%m%d')}"


def post_search(page: int) -> Dict[str, Any]:
    payload = {
        "query": expert_query(),
        "page": page,
        "limit": PAGE_SIZE,
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "TenderTracker/1.0",
    }

    resp = requests.post(TED_SEARCH_URL, json=payload, headers=headers, timeout=60)

    if resp.status_code >= 400:
        print("TED API error status:", resp.status_code)
        print("TED API response:", resp.text[:1500])
        resp.raise_for_status()

    return resp.json()


def pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return None


def first_list_value(value: Any) -> Optional[Any]:
    if isinstance(value, list) and value:
        return value[0]
    return value


def parse_notice(item: Dict[str, Any]) -> Tender:
    # TED response shapes may vary, so we read defensively.
    title = first_list_value(pick(item, "title", "noticeTitle", "officialTitle")) or "Untitled notice"

    notice_id = str(first_list_value(pick(item, "noticeId", "id", "publicationNumber", "tedId")) or "")
    source_id = notice_id or str(first_list_value(pick(item, "id")) or "")

    country = first_list_value(pick(item, "country", "buyerCountry", "countryCode"))
    cpv_code = first_list_value(pick(item, "cpvCode", "mainCpvCode", "mainCPV"))
    cpv_label = first_list_value(pick(item, "cpvLabel", "mainCpvLabel", "mainCPVLabel"))

    published_at = normalize_dt(first_list_value(pick(item, "publicationDate", "publishedAt", "datePublished")))
    deadline_at = normalize_dt(first_list_value(pick(item, "deadline", "submissionDeadline", "deadlineDate")))

    ca = first_list_value(pick(item, "buyerName", "contractingAuthority", "organisationName", "organizationName"))
    status = first_list_value(pick(item, "noticeType", "type", "formType"))
    estimated_value = str(first_list_value(pick(item, "estimatedValue", "value"))) if pick(item, "estimatedValue", "value") else None

    source_url = first_list_value(pick(item, "url", "noticeUrl", "tedUrl"))
    if not source_url and notice_id:
        source_url = f"https://ted.europa.eu/en/notice/-/detail/{quote(notice_id)}"

    return Tender(
        source="TED",
        source_id=source_id or source_url or title,
        title=str(title),
        link=source_url or "",
        ca=str(ca) if ca else None,
        published_at=published_at,
        deadline_at=deadline_at,
        status=str(status) if status else None,
        estimated_value=estimated_value,
        notice_id=notice_id or None,
        country=str(country) if country else None,
        cpv_code=str(cpv_code) if cpv_code else None,
        cpv_label=str(cpv_label) if cpv_label else None,
        source_url=source_url,
    )


def extract_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # TED response may use different top-level keys depending on version/result type.
    for key in ("results", "items", "notices", "content"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tenders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_id TEXT NOT NULL,
            title TEXT NOT NULL,
            link TEXT NOT NULL,
            ca TEXT,
            published_at TEXT,
            deadline_at TEXT,
            status TEXT,
            estimated_value TEXT,
            notice_id TEXT,
            country TEXT,
            cpv_code TEXT,
            cpv_label TEXT,
            source_url TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(source, source_id)
        );
        """
    )
    conn.commit()


def upsert(conn: sqlite3.Connection, tenders: List[Tender]) -> int:
    now = datetime.now(timezone.utc).isoformat()

    for t in tenders:
        conn.execute(
            """
            INSERT INTO tenders (
                source, source_id, title, link, ca, published_at, deadline_at,
                status, estimated_value, notice_id, country, cpv_code, cpv_label,
                source_url, first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, source_id) DO UPDATE SET
                title=excluded.title,
                link=excluded.link,
                ca=excluded.ca,
                published_at=excluded.published_at,
                deadline_at=excluded.deadline_at,
                status=excluded.status,
                estimated_value=excluded.estimated_value,
                notice_id=excluded.notice_id,
                country=excluded.country,
                cpv_code=excluded.cpv_code,
                cpv_label=excluded.cpv_label,
                source_url=excluded.source_url,
                last_seen_at=excluded.last_seen_at
            ;
            """,
            (
                t.source,
                t.source_id,
                t.title,
                t.link,
                t.ca,
                t.published_at,
                t.deadline_at,
                t.status,
                t.estimated_value,
                t.notice_id,
                t.country,
                t.cpv_code,
                t.cpv_label,
                t.source_url,
                now,
                now,
            ),
        )

    conn.commit()
    return len(tenders)


def print_latest(conn: sqlite3.Connection, limit: int = 10) -> None:
    cur = conn.execute(
        """
        SELECT published_at, country, cpv_code, title, ca, link
        FROM tenders
        WHERE source = 'TED'
        ORDER BY published_at DESC
        LIMIT ?;
        """,
        (limit,),
    )
    rows = cur.fetchall()

    print(f"\nLatest {len(rows)} TED notices:\n")
    for published_at, country, cpv_code, title, ca, link in rows:
        print(f"- Published: {published_at} | Country: {country} | CPV: {cpv_code}")
        print(f"  {title}")
        if ca:
            print(f"  CA: {ca}")
        print(f"  {link}\n")


def main():
    all_tenders: List[Tender] = []

    for page in range(1, MAX_PAGES + 1):
        print(f"Fetching TED page {page}...")
        payload = post_search(page)

        if page == 1:
            print("Top-level response keys:", list(payload.keys())[:20])

        items = extract_items(payload)
        print(f"Items found on page {page}: {len(items)}")

        if not items:
            break

        parsed = [parse_notice(item) for item in items]
        all_tenders.extend(parsed)

    if not all_tenders:
        print("No TED notices collected.")
        return

    dedup = {}
    for t in all_tenders:
        dedup[(t.source, t.source_id)] = t
    unique = list(dedup.values())

    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        n = upsert(conn, unique)
        print(f"Upserted {n} TED notices into {DB_PATH}")
        print_latest(conn)


if __name__ == "__main__":
    main()