import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET
import time

import requests

DB_PATH = "tenders.db"
TED_SEARCH_URL = "https://api.ted.europa.eu/v3/notices/search"

PAGE_SIZE = 10
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


def expert_query() -> str:
    today = datetime.now(timezone.utc).date()
    since = today - timedelta(days=LOOKBACK_DAYS)
    return f"publication-date>={since.strftime('%Y%m%d')} AND publication-date<={today.strftime('%Y%m%d')}"


def post_search(page: int) -> Dict[str, Any]:
    payload = {
        "query": expert_query(),
        "fields": ["publication-number"],
        "limit": PAGE_SIZE,
        "scope": "ACTIVE",
        "checkQuerySyntax": False,
        "paginationMode": "ITERATION",
        "page": page,
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "User-Agent": "TenderTracker/1.0",
    }

    resp = requests.post(TED_SEARCH_URL, json=payload, headers=headers, timeout=60)

    if resp.status_code >= 400:
        print("TED API error status:", resp.status_code)
        print("TED API response:", resp.text[:1500])
        resp.raise_for_status()

    return resp.json()


def extract_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return payload.get("notices", [])


def normalize_dt(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = str(value).strip()
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return value


def fetch_notice_xml(xml_url: str) -> Optional[ET.Element]:
    headers = {"User-Agent": "TenderTracker/1.0"}

    for attempt in range(3):
        try:
            resp = requests.get(xml_url, headers=headers, timeout=60)

            if resp.status_code == 429:
                wait = 2 * (attempt + 1)
                print(f"Rate limited on {xml_url}, waiting {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return ET.fromstring(resp.content)

        except Exception as e:
            if attempt == 2:
                print(f"Failed to fetch/parse XML: {xml_url} -> {e}")
                return None
            time.sleep(2)

    return None


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def first_text_by_localname(root: ET.Element, names: List[str]) -> Optional[str]:
    names_set = set(names)
    for el in root.iter():
        if local_name(el.tag) in names_set:
            text = (el.text or "").strip()
            if text:
                return text
    return None


def all_texts_by_localname(root: ET.Element, names: List[str]) -> List[str]:
    names_set = set(names)
    results = []
    for el in root.iter():
        if local_name(el.tag) in names_set:
            text = (el.text or "").strip()
            if text:
                results.append(text)
    return results


def find_text_under_path(root: ET.Element, parent_names: List[str], target_names: List[str]) -> Optional[str]:
    parent_set = set(parent_names)
    target_set = set(target_names)

    for parent in root.iter():
        if local_name(parent.tag) in parent_set:
            for child in parent.iter():
                if local_name(child.tag) in target_set:
                    text = (child.text or "").strip()
                    if text:
                        return text
    return None


def parse_notice_detail_xml(root: ET.Element) -> Dict[str, Optional[str]]:
    # Better title extraction
    title = (
        find_text_under_path(root, ["ProcurementProjectLot", "ProcurementProject"], ["Name", "Title"])
        or find_text_under_path(root, ["TenderResult"], ["Description"])
        or None
    )

    # Dates
    publication_date = first_text_by_localname(root, ["PublicationDate", "IssueDate"])
    deadline_date = first_text_by_localname(root, ["ReceiptDeadlineDate", "EndDate"])

    # Buyer / authority
    buyer_name = find_text_under_path(root, ["ContractingParty", "Organization", "PartyName"], ["Name"]) \
        or first_text_by_localname(root, ["Name"])

    # Country
    country = first_text_by_localname(root, ["IdentificationCode"])

    # CPV
    cpv_codes = all_texts_by_localname(root, ["ItemClassificationCode"])
    cpv_code = cpv_codes[0] if cpv_codes else None

    # Estimated value
    estimated_value = first_text_by_localname(
        root,
        ["EstimatedOverallContractAmount", "ValueAmount", "PayableAmount"]
    )

    return {
        "title": title,
        "published_at": normalize_dt(publication_date),
        "deadline_at": normalize_dt(deadline_date),
        "ca": buyer_name,
        "country": country,
        "cpv_code": cpv_code,
        "estimated_value": estimated_value,
    }


def parse_notice(item: Dict[str, Any]) -> Tender:
    notice_id = str(item.get("publication-number") or "").strip()

    links = item.get("links", {}) or {}
    xml_links = links.get("xml", {}) or {}
    html_links = links.get("html", {}) or {}
    html_direct_links = links.get("htmlDirect", {}) or {}

    xml_url = xml_links.get("MUL")
    source_url = (
        html_links.get("ENG")
        or html_direct_links.get("ENG")
        or f"https://ted.europa.eu/en/notice/-/detail/{notice_id}"
    )

    detail = {
        "title": None,
        "published_at": None,
        "deadline_at": None,
        "ca": None,
        "country": None,
        "cpv_code": None,
        "estimated_value": None,
    }

    if xml_url:
        time.sleep(0.4)
        root = fetch_notice_xml(xml_url)
        if root is not None:
            # DEBUG: print the first few XML tags for the first notice
            if notice_id == "120581-2026":
                print("\nFIRST XML TAGS SAMPLE:\n")
                seen = []
                for el in root.iter():
                    tag_name = local_name(el.tag)
                    if tag_name not in seen:
                        seen.append(tag_name)
                    if len(seen) >= 80:
                        break
                print(seen)
    
            detail = parse_notice_detail_xml(root)

    title = detail["title"] or f"TED Notice {notice_id}"

    return Tender(
        source="TED",
        source_id=notice_id or source_url,
        title=title,
        link=source_url,
        ca=detail["ca"],
        published_at=detail["published_at"],
        deadline_at=detail["deadline_at"],
        status=None,
        estimated_value=detail["estimated_value"],
        notice_id=notice_id or None,
        country=detail["country"],
        cpv_code=detail["cpv_code"],
        cpv_label=None,
        source_url=source_url,
    )


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
        ORDER BY rowid DESC
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