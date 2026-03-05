import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
from urllib.parse import urljoin
from datetime import timezone

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

SEARCH_URL = "https://www.etenders.gov.ie/epps/quickSearchAction.do?searchType=cftFTS"
BASE_URL = "https://www.etenders.gov.ie"
DB_PATH = "tenders.db"


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
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(source, source_id)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tenders_published_at ON tenders(published_at);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tenders_deadline_at ON tenders(deadline_at);")
    conn.commit()


def normalize_dt(dt_text: str) -> Optional[str]:
    """
    Site shows: 03/03/2026 12:51:22 (dd/mm/yyyy) sometimes with IST.
    We'll parse day-first and force UTC if tz is missing or unknown.
    """
    dt_text = (dt_text or "").strip()
    if not dt_text:
        return None
    try:
        dt = dateparser.parse(dt_text, dayfirst=True, ignoretz=True)
        # ignoretz=True makes it naive, then we set UTC
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return dt_text


def fetch_html() -> str:
    headers = {"User-Agent": "TenderTracker/0.1 (+local dev)"}
    r = requests.get(SEARCH_URL, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def parse_table(html: str) -> List[Tender]:
    soup = BeautifulSoup(html, "lxml")

    # Find the first table on the page (the results table)
    table = soup.find("table")
    if not table:
        raise RuntimeError("Could not find results table on the page.")

    rows = table.find_all("tr")
    if not rows or len(rows) < 2:
        return []

    tenders: List[Tender] = []

    # Skip header row (first row)
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if len(tds) < 8:
            # Some rows may be pagination/empty; skip
            continue

        # Column mapping based on page view:
        # #, Title, Resource ID, CA, Info, Date published, Submission Deadline, Procedure, Status, Notice PDF, Award date, Estimated value, Cycle
        title_td = tds[1]
        rid_td = tds[2]
        ca_td = tds[3]
        published_td = tds[5]
        deadline_td = tds[6]
        status_td = tds[8] if len(tds) > 8 else None
        est_value_td = tds[11] if len(tds) > 11 else None

        a = title_td.find("a")
        title = title_td.get_text(" ", strip=True)
        href = a["href"].strip() if a and a.has_attr("href") else None
        link = urljoin(BASE_URL, href) if href else SEARCH_URL

        resource_id = rid_td.get_text(" ", strip=True)
        ca = ca_td.get_text(" ", strip=True) or None

        published_at = normalize_dt(published_td.get_text(" ", strip=True))
        deadline_at = normalize_dt(deadline_td.get_text(" ", strip=True))

        status = status_td.get_text(" ", strip=True) if status_td else None
        estimated_value = est_value_td.get_text(" ", strip=True) if est_value_td else None

        # Use Resource ID as stable source_id
        if not resource_id:
            # fallback to link (should not happen often)
            resource_id = link

        tenders.append(
            Tender(
                source="ETENDERS_GOV_IE",
                source_id=resource_id,
                title=title,
                link=link,
                ca=ca,
                published_at=published_at,
                deadline_at=deadline_at,
                status=status,
                estimated_value=estimated_value,
            )
        )

    return tenders


def upsert(conn: sqlite3.Connection, tenders: List[Tender]) -> int:
    now = datetime.utcnow().isoformat()
    for t in tenders:
        conn.execute(
            """
            INSERT INTO tenders (
                source, source_id, title, link, ca, published_at, deadline_at, status, estimated_value,
                first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, source_id) DO UPDATE SET
                title=excluded.title,
                link=excluded.link,
                ca=excluded.ca,
                published_at=excluded.published_at,
                deadline_at=excluded.deadline_at,
                status=excluded.status,
                estimated_value=excluded.estimated_value,
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
                now,
                now,
            ),
        )
    conn.commit()
    return len(tenders)


def print_latest(conn: sqlite3.Connection, limit: int = 10) -> None:
    cur = conn.execute(
        """
        SELECT published_at, deadline_at, title, ca, estimated_value, link
        FROM tenders
        WHERE source = 'ETENDERS_GOV_IE'
        ORDER BY published_at DESC
        LIMIT ?;
        """,
        (limit,),
    )
    rows = cur.fetchall()
    print(f"\nLatest {len(rows)} tenders (etenders.gov.ie):\n")
    for published_at, deadline_at, title, ca, estimated_value, link in rows:
        print(f"- Published: {published_at} | Deadline: {deadline_at}")
        print(f"  {title}")
        if ca:
            print(f"  CA: {ca}")
        if estimated_value:
            print(f"  Est: {estimated_value}")
        print(f"  {link}\n")


def main() -> None:
    print("Fetching etenders.gov.ie search results…")
    html = fetch_html()

    tenders = parse_table(html)
    print(f"Parsed {len(tenders)} rows from results table.")

    if not tenders:
        print("No tenders parsed. If this persists, the page layout may have changed.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        n = upsert(conn, tenders)
        print(f"Upserted {n} items into {DB_PATH}")
        print_latest(conn, limit=10)


if __name__ == "__main__":
    main()