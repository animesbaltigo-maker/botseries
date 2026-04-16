"""Sistema simples de indicacoes com SQLite."""

import secrets
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from config import DATA_DIR

DB_PATH = Path(DATA_DIR) / "referrals.sqlite"

MIN_INTERACTIONS_TO_QUALIFY = 3
MIN_SECONDS_TO_QUALIFY = 60 * 60 * 24 * 7


@contextmanager
def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_referral_db() -> None:
    with _conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT DEFAULT '',
                first_name TEXT DEFAULT '',
                ref_code TEXT UNIQUE,
                created_at REAL DEFAULT 0,
                interactions INTEGER DEFAULT 0,
                is_blocked INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ref_code TEXT,
                referrer_id INTEGER,
                referred_user_id INTEGER,
                qualified INTEGER DEFAULT 0,
                rejected INTEGER DEFAULT 0,
                clicked_at REAL DEFAULT 0,
                qualified_at REAL DEFAULT 0,
                created_at REAL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_referrals_referred_user
            ON referrals(referred_user_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_referrals_ref_code
            ON referrals(ref_code)
            """
        )


def upsert_user(user_id: int, username: str = "", first_name: str = "") -> None:
    with _conn() as conn:
        existing = conn.execute(
            "SELECT ref_code FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not existing:
            ref_code = secrets.token_hex(5)
            conn.execute(
                """
                INSERT OR IGNORE INTO users (
                    user_id, username, first_name, ref_code, created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, username, first_name, ref_code, time.time()),
            )
        else:
            conn.execute(
                """
                UPDATE users
                SET username = ?, first_name = ?
                WHERE user_id = ?
                """,
                (username, first_name, user_id),
            )


def create_referral(user_id: int) -> str:
    upsert_user(user_id)
    with _conn() as conn:
        row = conn.execute(
            "SELECT ref_code FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return row["ref_code"] if row else ""


def register_referral_click(ref_code: str, referred_user_id: int) -> None:
    with _conn() as conn:
        referrer = conn.execute(
            "SELECT user_id FROM users WHERE ref_code = ?",
            (ref_code,),
        ).fetchone()
        if not referrer:
            return

        referrer_id = int(referrer["user_id"])
        if referrer_id == referred_user_id:
            return

        existing = conn.execute(
            "SELECT id FROM referrals WHERE referred_user_id = ?",
            (referred_user_id,),
        ).fetchone()
        if existing:
            return

        now = time.time()
        conn.execute(
            """
            INSERT INTO referrals (
                ref_code, referrer_id, referred_user_id, clicked_at, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (ref_code, referrer_id, referred_user_id, now, now),
        )


def try_qualify_referral(ref_code_or_id, is_channel_member: bool = True) -> None:
    with _conn() as conn:
        if isinstance(ref_code_or_id, int):
            rows = conn.execute(
                """
                SELECT *
                FROM referrals
                WHERE referred_user_id = ? AND qualified = 0 AND rejected = 0
                """,
                (ref_code_or_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM referrals
                WHERE ref_code = ? AND qualified = 0 AND rejected = 0
                """,
                (ref_code_or_id,),
            ).fetchall()

        now = time.time()
        for row in rows:
            user_row = conn.execute(
                """
                SELECT interactions, created_at
                FROM users
                WHERE user_id = ?
                """,
                (row["referred_user_id"],),
            ).fetchone()
            if not user_row:
                continue

            interactions = int(user_row["interactions"] or 0)
            age = now - float(row["created_at"] or now)
            if not (
                is_channel_member
                and interactions >= MIN_INTERACTIONS_TO_QUALIFY
                and age >= MIN_SECONDS_TO_QUALIFY
            ):
                continue

            conn.execute(
                """
                UPDATE referrals
                SET qualified = 1, qualified_at = ?
                WHERE id = ?
                """,
                (now, row["id"]),
            )


def register_interaction(user_id: int) -> None:
    with _conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (user_id, ref_code, created_at, interactions)
            VALUES (?, ?, ?, 0)
            """,
            (user_id, secrets.token_hex(5), time.time()),
        )
        conn.execute(
            "UPDATE users SET interactions = interactions + 1 WHERE user_id = ?",
            (user_id,),
        )


def referral_stats(user_id: int) -> dict:
    with _conn() as conn:
        ref_row = conn.execute(
            "SELECT ref_code FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not ref_row:
            return {"clicks": 0, "total": 0, "pending": 0, "qualified": 0}

        ref_code = ref_row["ref_code"]
        clicks = conn.execute(
            "SELECT COUNT(*) AS n FROM referrals WHERE ref_code = ?",
            (ref_code,),
        ).fetchone()["n"]
        qualified = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM referrals
            WHERE ref_code = ? AND qualified = 1
            """,
            (ref_code,),
        ).fetchone()["n"]
        pending = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM referrals
            WHERE ref_code = ? AND qualified = 0 AND rejected = 0
            """,
            (ref_code,),
        ).fetchone()["n"]

    return {
        "clicks": clicks,
        "total": clicks,
        "pending": pending,
        "qualified": qualified,
    }


def referral_ranking(limit: int = 3) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT u.user_id, u.username, u.first_name, COUNT(r.id) AS total
            FROM referrals r
            JOIN users u ON u.ref_code = r.ref_code
            WHERE r.qualified = 1
            GROUP BY r.ref_code
            ORDER BY total DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_all_pending_referrals() -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT r.*, u.interactions, u.is_blocked
            FROM referrals r
            LEFT JOIN users u ON u.user_id = r.referred_user_id
            WHERE r.qualified = 0 AND r.rejected = 0
            """
        ).fetchall()
    return [dict(row) for row in rows]


def referral_admin_overview() -> dict:
    with _conn() as conn:
        clicks_total = conn.execute("SELECT COUNT(*) AS n FROM referrals").fetchone()["n"]
        pending_total = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM referrals
            WHERE qualified = 0 AND rejected = 0
            """
        ).fetchone()["n"]
        approved_total = conn.execute(
            "SELECT COUNT(*) AS n FROM referrals WHERE qualified = 1"
        ).fetchone()["n"]
        rejected_total = conn.execute(
            "SELECT COUNT(*) AS n FROM referrals WHERE rejected = 1"
        ).fetchone()["n"]

    return {
        "clicks_total": clicks_total,
        "registered_total": clicks_total,
        "pending_total": pending_total,
        "approved_total": approved_total,
        "rejected_total": rejected_total,
    }
