import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

SEARCH_URL = "https://www.etenders.gov.ie/epps/quickSearchAction.do?searchType=cftFTS"
BASE_URL = "https://www.etenders.gov.ie"
DB_PATH = "tenders.db"

# How many pages to fetch (each page is usually ~10 rows)
MAX_PAGES = 30  # 30 pages ~= 300 tenders (adjust later)


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
    eTenders shows dd/mm/yyyy and sometimes "IST".
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


def fetch_page(session: requests.Session, url: str, form: Optional[Dict[str, str]] = None) -> str:
    headers = {"User-Agent": "TenderTracker/0.1 (+local dev)"}
    if form is None:
        r = session.get(url, headers=headers, timeout=30)
    else:
        r = session.post(url, data=form, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def parse_results_table(html: str) -> List[Tender]:
    soup = BeautifulSoup(html, "lxml")

    # Results table is typically the first table containing "Resource ID" header
    tables = soup.find_all("table")
    table = None
    for t in tables:
        th_text = t.get_text(" ", strip=True).lower()
        if "resource id" in th_text and "submission deadline" in th_text:
            table = t
            break
    if table is None:
        # fallback to first table
        table = soup.find("table")

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

        # Columns (based on observed layout):
        # 0 #, 1 Title, 2 Resource ID, 3 CA, 5 Date published, 6 Submission Deadline, ... 8 Status, 11 Estimated value
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


def find_next_form(html: str) -> Optional[Tuple[str, Dict[str, str]]]:
    """
    Tries to find a "Next" pagination control and build a POST payload.
    This is intentionally generic: we extract the first form and reuse its hidden inputs.
    """
    soup = BeautifulSoup(html, "lxml")
    form = soup.find("form")
    if not form:
        return None

    action = form.get("action") or SEARCH_URL
    action_url = urljoin(BASE_URL, action)

    # Collect hidden inputs
    payload: Dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        itype = (inp.get("type") or "").lower()
        value = inp.get("value") or ""
        if itype in ("hidden", "text"):
            payload[name] = value

    # Look for a submit/button/link that indicates "Next"
    # Common labels: Next, >, >>, "nextPage"
    next_candidate = None
    for inp in form.find_all("input"):
        itype = (inp.get("type") or "").lower()
        val = (inp.get("value") or "").strip().lower()
        name = inp.get("name")
        if itype in ("submit", "button") and ("next" in val or val in (">", ">>")):
            next_candidate = (name, inp.get("value") or "")
            break

    # If we found a submit button, include it
    if next_candidate and next_candidate[0]:
        payload[next_candidate[0]] = next_candidate[1]
        return action_url, payload

    # If no explicit next button found, we can’t safely paginate with this heuristic
    return None


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
        html1 = fetch_page(s, SEARCH_URL)
        all_tenders: List[Tender] = []
        tenders = parse_results_table(html1)
        print(f"Parsed {len(tenders)} rows from page 1.")
        all_tenders.extend(tenders)

        current_html = html1
        for page in range(2, MAX_PAGES + 1):
            nxt = find_next_form(current_html)
            if not nxt:
                print(f"Pagination: could not find a Next action after page {page-1}. Stopping.")
                break

            action_url, payload = nxt
            print(f"Fetching page {page}…")
            current_html = fetch_page(s, action_url, form=payload)
            tenders = parse_results_table(current_html)
            print(f"Parsed {len(tenders)} rows from page {page}.")
            if not tenders:
                print("No rows parsed; stopping.")
                break
            all_tenders.extend(tenders)

        # Deduplicate by source_id
        dedup: Dict[str, Tender] = {}
        for t in all_tenders:
            dedup[t.source_id] = t
        unique = list(dedup.values())

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