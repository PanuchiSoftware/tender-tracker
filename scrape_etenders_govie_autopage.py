import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

SEARCH_URL = "https://www.etenders.gov.ie/epps/quickSearchAction.do?searchType=cftFTS"
BASE_URL = "https://www.etenders.gov.ie"
DB_PATH = "tenders.db"

# How many pages to try at most (10 rows/page typical)
MAX_PAGES = 50  # ~500 tenders


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
    eTenders shows dd/mm/yyyy and sometimes includes 'IST'.
    ignoretz=True avoids timezone warnings; we then force UTC.
    """
    dt_text = (dt_text or "").strip()
    if not dt_text:
        return None
    try:
        dt = dateparser.parse(dt_text, dayfirst=True, ignoretz=True)
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return dt_text


def fetch_html(session: requests.Session, url: str) -> str:
    headers = {"User-Agent": "TenderTracker/0.1 (+local dev)"}
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def parse_results_table(html: str) -> List[Tender]:
    soup = BeautifulSoup(html, "lxml")

    # Find a table that looks like the results table
    tables = soup.find_all("table")
    table = None
    for t in tables:
        txt = t.get_text(" ", strip=True).lower()
        if "resource id" in txt and "submission deadline" in txt:
            table = t
            break
    if table is None and tables:
        table = tables[0]

    if not table:
        return []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    tenders: List[Tender] = []
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        # Observed layout
        title_td = tds[1]
        rid_td = tds[2]
        ca_td = tds[3]
        published_td = tds[5] if len(tds) > 5 else None
        deadline_td = tds[6] if len(tds) > 6 else None
        status_td = tds[8] if len(tds) > 8 else None
        est_value_td = tds[11] if len(tds) > 11 else None

        a = title_td.find("a")
        title = title_td.get_text(" ", strip=True)
        href = a["href"].strip() if a and a.has_attr("href") else None
        link = urljoin(BASE_URL, href) if href else SEARCH_URL

        resource_id = rid_td.get_text(" ", strip=True) or link
        ca = ca_td.get_text(" ", strip=True) or None

        published_at = normalize_dt(published_td.get_text(" ", strip=True) if published_td else "")
        deadline_at = normalize_dt(deadline_td.get_text(" ", strip=True) if deadline_td else "")

        status = status_td.get_text(" ", strip=True) if status_td else None
        estimated_value = est_value_td.get_text(" ", strip=True) if est_value_td else None

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


def detect_page_param(html: str) -> Optional[str]:
    """
    Try to detect a displaytag-style paging parameter like: d-1234567-p=1
    """
    # Look for any occurrence of d-<digits>-p=<digits>
    m = re.search(r"(d-\d+-p)=(\d+)", html)
    if m:
        return m.group(1)

    # Some sites use: d-<digits>-p (without equals in scripts); fallback
    m2 = re.search(r"(d-\d+-p)", html)
    if m2:
        return m2.group(1)

    return None


def set_query_param(url: str, key: str, value: str) -> str:
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q[key] = value
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


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


def main() -> None:
    with requests.Session() as s:
        print("Fetching page 1…")
        html1 = fetch_html(s, SEARCH_URL)
        page_param = detect_page_param(html1)

        if not page_param:
            print("Could not detect a paging parameter in HTML.")
            print("Next step: we’ll dump a snippet of HTML to detect it precisely.")
            # Dump a small hint to help us adjust quickly
            hint = "\n".join(html1.splitlines()[:200])
            with open("page1_debug.html", "w", encoding="utf-8") as f:
                f.write(html1)
            print("Saved full page HTML to page1_debug.html")
            return

        print(f"Detected paging param: {page_param}")

        all_tenders: Dict[str, Tender] = {}

        # Page 1
        tenders = parse_results_table(html1)
        print(f"Parsed {len(tenders)} rows from page 1.")
        for t in tenders:
            all_tenders[t.source_id] = t

        # Pages 2..N
        for page in range(2, MAX_PAGES + 1):
            url = set_query_param(SEARCH_URL, page_param, str(page))
            print(f"Fetching page {page}…")
            htmlp = fetch_html(s, url)
            tenders = parse_results_table(htmlp)
            print(f"Parsed {len(tenders)} rows from page {page}.")
            if not tenders:
                print("No rows parsed; stopping.")
                break

            before = len(all_tenders)
            for t in tenders:
                all_tenders[t.source_id] = t
            after = len(all_tenders)

            # If a page adds nothing new, stop
            if after == before:
                print("No new tenders found on this page; stopping.")
                break

        unique = list(all_tenders.values())

    print(f"Total collected (unique): {len(unique)}")
    if not unique:
        print("No tenders collected.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        n = upsert(conn, unique)
        print(f"Upserted {n} items into {DB_PATH}")

        cur = conn.execute(
            """
            SELECT published_at, deadline_at, title, ca, estimated_value, link
            FROM tenders
            WHERE source = 'ETENDERS_GOV_IE'
            ORDER BY published_at DESC
            LIMIT 10;
            """
        )
        rows = cur.fetchall()
        print("\nLatest 10 tenders (etenders.gov.ie):\n")
        for published_at, deadline_at, title, ca, estimated_value, link in rows:
            print(f"- Published: {published_at} | Deadline: {deadline_at}")
            print(f"  {title}")
            if ca:
                print(f"  CA: {ca}")
            if estimated_value:
                print(f"  Est: {estimated_value}")
            print(f"  {link}\n")


if __name__ == "__main__":
    main()