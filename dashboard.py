import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

DB_PATH = "tenders.db"

st.set_page_config(page_title="Tender Tracker", layout="wide")
st.title("Tender Tracker – Ranked Matches")

# -------------------------
# Sidebar controls
# -------------------------
profile = st.sidebar.text_input("Profile name", value="Default")
min_score = st.sidebar.slider("Minimum score", 0, 30, 8)
limit = st.sidebar.slider("Max rows", 50, 2000, 500, step=50)
search = st.sidebar.text_input("Search (title / CA)", value="")
bookmarks_only = st.sidebar.checkbox("Bookmarks only", value=False)

# Deadline filter
only_with_deadline = st.sidebar.checkbox("Only tenders with a deadline", value=True)
due_within = st.sidebar.slider("Due within (days)", 0, 180, 30)

# Sorting
sort_by = st.sidebar.selectbox(
    "Sort by",
    ["priority desc (recommended)", "score desc", "deadline asc", "published desc"],
    index=0,
)

order_sql = {
    "priority desc (recommended)": "priority DESC, m.score DESC, t.published_at DESC",
    "score desc": "m.score DESC, t.published_at DESC",
    "deadline asc": "CASE WHEN t.deadline_at IS NULL THEN 1 ELSE 0 END, t.deadline_at ASC, m.score DESC",
    "published desc": "t.published_at DESC, m.score DESC",
}[sort_by]

# -------------------------
# SQL query
# -------------------------
params = [profile, min_score]
search_sql = ""
deadline_sql = ""

if search.strip():
    search_sql = " AND (lower(t.title) LIKE lower(?) OR lower(coalesce(t.ca,'')) LIKE lower(?)) "
    params.extend([f"%{search.strip()}%", f"%{search.strip()}%"])

# We'll compute due_days in SQL. For filtering by due_within we filter in pandas (more robust).
# Optionally, you could filter in SQL too, but ISO timestamps + sqlite parsing can be finicky.

params.append(limit)

sql = f"""
SELECT
  t.source,
  t.source_id,
  m.score,
  m.matched_terms,
  t.published_at,
  t.deadline_at,
  CAST(
    (julianday(substr(t.deadline_at, 1, 19)) - julianday('now')) AS INTEGER
  ) AS due_days,
  CASE
    WHEN t.deadline_at IS NULL THEN 0
    WHEN (julianday(substr(t.deadline_at, 1, 19)) - julianday('now')) <= 3 THEN 10
    WHEN (julianday(substr(t.deadline_at, 1, 19)) - julianday('now')) <= 7 THEN 7
    WHEN (julianday(substr(t.deadline_at, 1, 19)) - julianday('now')) <= 14 THEN 4
    ELSE 1
  END AS urgency,
  (
    m.score +
    CASE
      WHEN t.deadline_at IS NULL THEN 0
      WHEN (julianday(substr(t.deadline_at, 1, 19)) - julianday('now')) <= 3 THEN 10
      WHEN (julianday(substr(t.deadline_at, 1, 19)) - julianday('now')) <= 7 THEN 7
      WHEN (julianday(substr(t.deadline_at, 1, 19)) - julianday('now')) <= 14 THEN 4
      ELSE 1
    END
  ) AS priority,
  t.title,
  t.ca,
  t.estimated_value,
  t.link,
  CASE WHEN b.source_id IS NULL THEN 0 ELSE 1 END AS bookmarked
FROM tender_matches m
JOIN tenders t
  ON t.source = m.source AND t.source_id = m.source_id
LEFT JOIN bookmarks b
  ON b.source = t.source AND b.source_id = t.source_id
WHERE m.profile_name = ?
  AND m.score >= ?
  AND t.source = 'ETENDERS_GOV_IE'
  {search_sql}
ORDER BY {order_sql}
LIMIT ?;
"""

with sqlite3.connect(DB_PATH) as conn:
    df = pd.read_sql_query(sql, conn, params=params)

# -------------------------
# Filters in pandas (safer)
# -------------------------
if bookmarks_only:
    df = df[df["bookmarked"] == 1]

if only_with_deadline:
    df = df[df["deadline_at"].notna()]

# due_days can be NULL for some rows, so guard
if "due_days" in df.columns:
    df = df[df["due_days"].notna()]
    df = df[df["due_days"].between(0, due_within)]

if df.empty:
    st.warning("No results for these filters.")
    st.stop()

# -------------------------
# Bookmark toggle
# -------------------------
def toggle_bookmark(source: str, source_id: str, value: bool) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        if value:
            conn.execute(
                "INSERT OR IGNORE INTO bookmarks(source, source_id, created_at) VALUES (?, ?, ?)",
                (source, source_id, datetime.utcnow().isoformat()),
            )
        else:
            conn.execute(
                "DELETE FROM bookmarks WHERE source=? AND source_id=?",
                (source, source_id),
            )
        conn.commit()


st.subheader("Bookmark a tender")

options = df[["source", "source_id", "title", "bookmarked"]].copy()
options["label"] = options["title"].astype(str).str.slice(0, 120)

choice = st.selectbox(
    "Select tender",
    options.index,
    format_func=lambda i: options.loc[i, "label"],
)

current = bool(options.loc[choice, "bookmarked"])
new_value = st.checkbox("Bookmarked", value=current)

if new_value != current:
    toggle_bookmark(
        options.loc[choice, "source"],
        options.loc[choice, "source_id"],
        new_value,
    )
    st.success("Bookmark updated")
    st.rerun()

# -------------------------
# Table display
# -------------------------
df["link"] = df["link"].astype(str)

# Optional: make due_days/priority nicer types
for col in ["score", "urgency", "priority", "due_days"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df_view = df.drop(columns=["source", "source_id"])

st.caption(f"Showing {len(df_view)} tenders")

st.dataframe(
    df_view,
    use_container_width=True,
    hide_index=True,
    column_config={
        "link": st.column_config.LinkColumn("Link", display_text="Open"),
    },
)

# -------------------------
# Stats
# -------------------------
st.divider()
c1, c2, c3, c4 = st.columns(4)

c1.metric("Rows", len(df_view))
c2.metric("Max score", int(df_view["score"].max()) if "score" in df_view else 0)
c3.metric("Max priority", int(df_view["priority"].max()) if "priority" in df_view else 0)
c4.metric("Avg priority", round(float(df_view["priority"].mean()), 2) if "priority" in df_view else 0.0)