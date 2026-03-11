import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

from auth import (
    create_user,
    authenticate_user,
    get_user_profile,
    save_user_profile,
    toggle_user_bookmark,
)

DB_PATH = "tenders.db"

st.set_page_config(page_title="Tender Tracker", layout="wide")


def logout():
    st.session_state.pop("user", None)
    st.rerun()


def login_ui():
    st.title("Tender Tracker – Login")

    tab1, tab2 = st.tabs(["Login", "Create account"])

    with tab1:
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")

        if st.button("Login"):
            ok, user = authenticate_user(email, password)
            if ok:
                st.session_state["user"] = user
                st.rerun()
            else:
                st.error("Invalid email or password.")

    with tab2:
        new_email = st.text_input("Email", key="signup_email")
        new_password = st.text_input("Password", type="password", key="signup_password")

        if st.button("Create account"):
            ok, msg = create_user(new_email, new_password)
            if ok:
                st.success(msg)
            else:
                st.error(msg)


if "user" not in st.session_state:
    login_ui()
    st.stop()

user = st.session_state["user"]
user_id = user["id"]

st.title("Tender Tracker – Ranked Matches")
st.caption(f"Logged in as {user['email']}")
st.button("Logout", on_click=logout)

profile = get_user_profile(user_id)

# -------------------------
# Sidebar controls
# -------------------------
profile_name = st.sidebar.text_input("Profile name", value=profile["profile_name"])
min_score = st.sidebar.slider("Minimum score", 0, 30, int(profile["min_score"]))
limit = st.sidebar.slider("Max rows", 50, 2000, 500, step=50)
search = st.sidebar.text_input("Search (title / CA)", value=profile["search_text"])
bookmarks_only = st.sidebar.checkbox("Bookmarks only", value=False)

source_filter = st.sidebar.selectbox(
    "Source",
    ["All", "ETENDERS_GOV_IE", "TED"],
    index=["All", "ETENDERS_GOV_IE", "TED"].index(profile["source_filter"])
    if profile["source_filter"] in ["All", "ETENDERS_GOV_IE", "TED"]
    else 0,
)

country_filter = st.sidebar.text_input("Country code", value=profile["country_filter"])
cpv_filter = st.sidebar.text_input("CPV code starts with", value=profile["cpv_filter"])

only_with_deadline = st.sidebar.checkbox(
    "Only tenders with a deadline",
    value=bool(profile["only_with_deadline"]),
)
due_within = st.sidebar.slider("Due within (days)", 0, 180, int(profile["due_within"]))

sort_options = [
    "priority desc (recommended)",
    "score desc",
    "deadline asc",
    "published desc",
]
sort_by = st.sidebar.selectbox(
    "Sort by",
    sort_options,
    index=sort_options.index(profile["sort_by"]) if profile["sort_by"] in sort_options else 0,
)

if st.sidebar.button("Save my filters"):
    save_user_profile(
        user_id=user_id,
        profile_name=profile_name,
        min_score=min_score,
        source_filter=source_filter,
        country_filter=country_filter,
        cpv_filter=cpv_filter,
        only_with_deadline=int(only_with_deadline),
        due_within=due_within,
        search_text=search,
        sort_by=sort_by,
    )
    st.sidebar.success("Filters saved")

order_sql = {
    "priority desc (recommended)": "priority DESC, m.score DESC, t.published_at DESC",
    "score desc": "m.score DESC, t.published_at DESC",
    "deadline asc": "CASE WHEN t.deadline_at IS NULL THEN 1 ELSE 0 END, t.deadline_at ASC, m.score DESC",
    "published desc": "t.published_at DESC, m.score DESC",
}[sort_by]

params = [profile_name, min_score]

search_sql = ""
if search.strip():
    search_sql = " AND (lower(t.title) LIKE lower(?) OR lower(coalesce(t.ca,'')) LIKE lower(?)) "
    params.extend([f"%{search.strip()}%", f"%{search.strip()}%"])

source_sql = ""
if source_filter != "All":
    source_sql = " AND t.source = ? "
    params.append(source_filter)

country_sql = ""
if country_filter.strip():
    country_sql = " AND lower(coalesce(t.country,'')) = lower(?) "
    params.append(country_filter.strip())

cpv_sql = ""
if cpv_filter.strip():
    cpv_sql = " AND coalesce(t.cpv_code,'') LIKE ? "
    params.append(f"{cpv_filter.strip()}%")

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
  t.country,
  t.cpv_code,
  t.cpv_label,
  t.estimated_value,
  t.link
FROM tender_matches m
JOIN tenders t
  ON t.source = m.source AND t.source_id = m.source_id
WHERE m.profile_name = ?
  AND m.score >= ?
  {search_sql}
  {source_sql}
  {country_sql}
  {cpv_sql}
ORDER BY {order_sql}
LIMIT ?;
"""

with sqlite3.connect(DB_PATH) as conn:
    df = pd.read_sql_query(sql, conn, params=params)

if only_with_deadline:
    df = df[df["deadline_at"].notna()]

if "due_days" in df.columns:
    df = df[df["due_days"].notna()]
    df = df[df["due_days"].between(0, due_within)]

if bookmarks_only:
    with sqlite3.connect(DB_PATH) as conn:
        bookmarks_df = pd.read_sql_query(
            """
            SELECT source, source_id
            FROM user_bookmarks
            WHERE user_id = ?
            """,
            conn,
            params=[user_id],
        )
    if not bookmarks_df.empty:
        df = df.merge(bookmarks_df, on=["source", "source_id"], how="inner")
    else:
        df = df.iloc[0:0]

if df.empty:
    st.warning("No results for these filters.")
    st.stop()

st.subheader("Bookmark a tender")

options = df[["source", "source_id", "title"]].copy()
options["label"] = options["title"].astype(str).str.slice(0, 120)

choice = st.selectbox(
    "Select tender",
    options.index,
    format_func=lambda i: options.loc[i, "label"],
)

selected_source = options.loc[choice, "source"]
selected_source_id = options.loc[choice, "source_id"]

with sqlite3.connect(DB_PATH) as conn:
    row = conn.execute(
        """
        SELECT 1
        FROM user_bookmarks
        WHERE user_id = ? AND source = ? AND source_id = ?
        """,
        (user_id, selected_source, selected_source_id),
    ).fetchone()

current = row is not None
new_value = st.checkbox("Bookmarked", value=current)

if new_value != current:
    toggle_user_bookmark(user_id, selected_source, selected_source_id, new_value)
    st.success("Bookmark updated")
    st.rerun()

df["link"] = df["link"].astype(str)

for col in ["score", "urgency", "priority", "due_days"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df_view = df.drop(columns=["source_id"])

st.caption(f"Showing {len(df_view)} tenders")

st.dataframe(
    df_view,
    width="stretch",
    hide_index=True,
    column_config={
        "link": st.column_config.LinkColumn("Link", display_text="Open"),
    },
)

st.divider()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", len(df_view))
c2.metric("Max score", int(df_view["score"].max()) if "score" in df_view else 0)
c3.metric("Max priority", int(df_view["priority"].max()) if "priority" in df_view else 0)
c4.metric("Avg priority", round(float(df_view["priority"].mean()), 2) if "priority" in df_view else 0.0)