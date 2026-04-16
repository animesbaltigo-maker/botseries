"""Metricas simples com SQLite."""

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from config import DATA_DIR

DB_PATH = Path(DATA_DIR) / "metrics.sqlite3"


@contextmanager
def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=30000;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_metrics_db() -> None:
    with _conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                user_id INTEGER,
                username TEXT,
                query_text TEXT,
                result_count INTEGER,
                ts REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_type_ts
            ON events(event_type, ts)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_user_ts
            ON events(user_id, ts)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users_seen (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen REAL,
                last_seen REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watched (
                user_id INTEGER,
                item_key TEXT,
                ts REAL,
                PRIMARY KEY (user_id, item_key)
            )
            """
        )


def log_event(
    event_type: str,
    user_id: int = 0,
    username: str = "",
    query_text: str = "",
    result_count: int = 0,
) -> None:
    try:
        with _conn() as conn:
            conn.execute(
                """
                INSERT INTO events (
                    event_type, user_id, username, query_text, result_count, ts
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(event_type or "").strip(),
                    int(user_id or 0),
                    str(username or "").strip(),
                    str(query_text or "").strip(),
                    int(result_count or 0),
                    time.time(),
                ),
            )
    except Exception as exc:
        print("metrics.log_event error:", exc)


def mark_user_seen(user_id: int, username: str = "") -> None:
    now = time.time()
    try:
        with _conn() as conn:
            existing = conn.execute(
                "SELECT first_seen FROM users_seen WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            first_seen = float(existing["first_seen"]) if existing else now
            conn.execute(
                """
                INSERT OR REPLACE INTO users_seen (
                    user_id, username, first_seen, last_seen
                )
                VALUES (?, ?, ?, ?)
                """,
                (user_id, username, first_seen, now),
            )
    except Exception as exc:
        print("metrics.mark_user_seen error:", exc)


def is_episode_watched(user_id: int, item_key: str) -> bool:
    try:
        with _conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM watched WHERE user_id = ? AND item_key = ?",
                (user_id, item_key),
            ).fetchone()
        return bool(row)
    except Exception:
        return False


def mark_episode_watched(user_id: int, item_key: str) -> None:
    try:
        with _conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO watched (user_id, item_key, ts)
                VALUES (?, ?, ?)
                """,
                (user_id, item_key, time.time()),
            )
    except Exception as exc:
        print("metrics.mark_episode_watched error:", exc)


def unmark_episode_watched(user_id: int, item_key: str) -> None:
    try:
        with _conn() as conn:
            conn.execute(
                "DELETE FROM watched WHERE user_id = ? AND item_key = ?",
                (user_id, item_key),
            )
    except Exception as exc:
        print("metrics.unmark_episode_watched error:", exc)


def get_stats() -> dict:
    try:
        with _conn() as conn:
            total_users = conn.execute("SELECT COUNT(*) FROM users_seen").fetchone()[0]
            total_searches = conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type = 'search'"
            ).fetchone()[0]
        return {"total_users": total_users, "total_searches": total_searches}
    except Exception:
        return {}


def _since_ts(period: str) -> float:
    now = time.time()
    if period == "hoje":
        return now - 86400
    if period == "7d":
        return now - (86400 * 7)
    if period == "30d":
        return now - (86400 * 30)
    return 0.0


def _top_rows(conn, event_types: tuple[str, ...], limit: int, since: float) -> list[dict]:
    placeholders = ",".join("?" for _ in event_types)
    params: list = list(event_types)
    query = (
        "SELECT query_text AS label, COUNT(*) AS total "
        f"FROM events WHERE event_type IN ({placeholders}) AND query_text != '' "
    )
    if since > 0:
        query += "AND ts >= ? "
        params.append(since)
    query += "GROUP BY query_text ORDER BY total DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def get_metrics_report(limit: int = 7, period: str = "total") -> dict:
    since = _since_ts(period)

    try:
        with _conn() as conn:
            top_searches = _top_rows(conn, ("search",), limit, since)
            top_opened_titles = _top_rows(conn, ("open_item", "item_open"), limit, since)
            top_watch_clicks = _top_rows(conn, ("watch_click", "player_open"), limit, since)
            top_episodes = _top_rows(conn, ("episode_click", "episode_open"), limit, since)

            if since > 0:
                no_result = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM events
                    WHERE event_type IN ('search_no_result', 'search_empty') AND ts >= ?
                    """,
                    (since,),
                ).fetchone()[0]
                new_users = conn.execute(
                    "SELECT COUNT(*) FROM users_seen WHERE first_seen >= ?",
                    (since,),
                ).fetchone()[0]
                active_users = conn.execute(
                    "SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= ?",
                    (since,),
                ).fetchone()[0]
            else:
                no_result = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM events
                    WHERE event_type IN ('search_no_result', 'search_empty')
                    """
                ).fetchone()[0]
                new_users = conn.execute("SELECT COUNT(*) FROM users_seen").fetchone()[0]
                active_users = conn.execute(
                    "SELECT COUNT(DISTINCT user_id) FROM events"
                ).fetchone()[0]

        return {
            "top_searches": top_searches,
            "top_opened_titles": top_opened_titles,
            "top_opened_animes": top_opened_titles,
            "top_watch_clicks": top_watch_clicks,
            "top_episodes": top_episodes,
            "searches_without_result": no_result,
            "new_users": new_users,
            "active_users": active_users,
        }
    except Exception as exc:
        print("get_metrics_report error:", exc)
        return {
            "top_searches": [],
            "top_opened_titles": [],
            "top_opened_animes": [],
            "top_watch_clicks": [],
            "top_episodes": [],
            "searches_without_result": 0,
            "new_users": 0,
            "active_users": 0,
        }


def clear_metrics() -> None:
    try:
        with _conn() as conn:
            conn.execute("DELETE FROM events")
    except Exception as exc:
        print("clear_metrics error:", exc)
