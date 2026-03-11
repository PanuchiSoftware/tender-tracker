import os
import hmac
import sqlite3
import hashlib
from datetime import datetime, timezone

DB_PATH = "tenders.db"


def get_conn():
    return sqlite3.connect(DB_PATH)


def hash_password(password: str, salt: bytes | None = None) -> str:
    if salt is None:
        salt = os.urandom(16)
    pwd_hash = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        100_000,
    )
    return salt.hex() + ":" + pwd_hash.hex()


def verify_password(password: str, stored: str) -> bool:
    salt_hex, hash_hex = stored.split(":")
    salt = bytes.fromhex(salt_hex)
    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        100_000,
    )
    return hmac.compare_digest(candidate.hex(), hash_hex)


def create_user(email: str, password: str) -> tuple[bool, str]:
    email = email.strip().lower()
    if not email or not password:
        return False, "Email and password are required."

    conn = get_conn()
    cur = conn.cursor()

    existing = cur.execute(
        "SELECT id FROM users WHERE email = ?",
        (email,),
    ).fetchone()

    if existing:
        conn.close()
        return False, "User already exists."

    password_hash = hash_password(password)
    created_at = datetime.now(timezone.utc).isoformat()

    cur.execute(
        "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
        (email, password_hash, created_at),
    )
    user_id = cur.lastrowid

    cur.execute(
        """
        INSERT INTO user_profiles (
            user_id, profile_name, min_score, source_filter, country_filter,
            cpv_filter, only_with_deadline, due_within, search_text, sort_by
        )
        VALUES (?, 'Default', 0, 'All', '', '', 0, 180, '', 'priority desc (recommended)')
        """,
        (user_id,),
    )

    conn.commit()
    conn.close()
    return True, "Account created."


def authenticate_user(email: str, password: str) -> tuple[bool, dict | None]:
    email = email.strip().lower()
    conn = get_conn()
    cur = conn.cursor()

    row = cur.execute(
        "SELECT id, email, password_hash FROM users WHERE email = ?",
        (email,),
    ).fetchone()

    conn.close()

    if not row:
        return False, None

    user_id, email, password_hash = row
    if not verify_password(password, password_hash):
        return False, None

    return True, {"id": user_id, "email": email}


def get_user_profile(user_id: int) -> dict:
    conn = get_conn()
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT profile_name, min_score, source_filter, country_filter, cpv_filter,
               only_with_deadline, due_within, search_text, sort_by
        FROM user_profiles
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()

    conn.close()

    if not row:
        return {
            "profile_name": "Default",
            "min_score": 0,
            "source_filter": "All",
            "country_filter": "",
            "cpv_filter": "",
            "only_with_deadline": 0,
            "due_within": 180,
            "search_text": "",
            "sort_by": "priority desc (recommended)",
        }

    return {
        "profile_name": row[0],
        "min_score": row[1],
        "source_filter": row[2],
        "country_filter": row[3] or "",
        "cpv_filter": row[4] or "",
        "only_with_deadline": row[5],
        "due_within": row[6],
        "search_text": row[7] or "",
        "sort_by": row[8],
    }


def save_user_profile(
    user_id: int,
    profile_name: str,
    min_score: int,
    source_filter: str,
    country_filter: str,
    cpv_filter: str,
    only_with_deadline: int,
    due_within: int,
    search_text: str,
    sort_by: str,
) -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE user_profiles
        SET profile_name = ?,
            min_score = ?,
            source_filter = ?,
            country_filter = ?,
            cpv_filter = ?,
            only_with_deadline = ?,
            due_within = ?,
            search_text = ?,
            sort_by = ?
        WHERE user_id = ?
        """,
        (
            profile_name,
            min_score,
            source_filter,
            country_filter,
            cpv_filter,
            only_with_deadline,
            due_within,
            search_text,
            sort_by,
            user_id,
        ),
    )

    conn.commit()
    conn.close()


def get_user_bookmark_state(user_id: int, source: str, source_id: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT 1
        FROM user_bookmarks
        WHERE user_id = ? AND source = ? AND source_id = ?
        """,
        (user_id, source, source_id),
    ).fetchone()

    conn.close()
    return row is not None


def toggle_user_bookmark(user_id: int, source: str, source_id: str, value: bool) -> None:
    conn = get_conn()
    cur = conn.cursor()

    if value:
        cur.execute(
            """
            INSERT OR IGNORE INTO user_bookmarks (user_id, source, source_id, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, source, source_id, datetime.now(timezone.utc).isoformat()),
        )
    else:
        cur.execute(
            """
            DELETE FROM user_bookmarks
            WHERE user_id = ? AND source = ? AND source_id = ?
            """,
            (user_id, source, source_id),
        )

    conn.commit()
    conn.close()