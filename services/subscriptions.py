from __future__ import annotations

import json
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any

from config import DATA_DIR, SUBSCRIPTIONS_DB_PATH

DB_PATH = Path(SUBSCRIPTIONS_DB_PATH).expanduser()

APPROVED_EVENTS = {
    "approved",
    "approve",
    "payment_approved",
    "payment_paid",
    "paid",
    "order_approved",
    "order_paid",
    "purchase_approved",
    "purchase_paid",
    "sale_approved",
    "sale_paid",
    "compra_aprovada",
    "subscription_approved",
    "subscription_paid",
    "subscription_renewed",
}

CANCEL_EVENTS = {
    "refused",
    "rejected",
    "canceled",
    "cancelled",
    "refund",
    "refunded",
    "chargeback",
    "payment_refused",
    "payment_rejected",
    "order_canceled",
    "order_cancelled",
    "purchase_refused",
    "purchase_rejected",
    "compra_recusada",
    "subscription_canceled",
    "subscription_cancelled",
}

PLAN_DAYS = {
    "bronze": 7,
    "semanal": 7,
    "ouro": 30,
    "mensal": 30,
    "trimestral": 90,
    "semestral": 180,
    "diamante": 365,
    "anual": 365,
    "rubi": 36500,
    "vitalicio": 36500,
    "vitalício": 36500,
}

PLAN_ALIASES = {
    "9snqsp3": "mensal",
    "mensal": "mensal",
    "monthly": "mensal",
    "30d": "mensal",
    "30_dias": "mensal",
    "3fsy24d": "trimestral",
    "trimestral": "trimestral",
    "3m": "trimestral",
    "3_meses": "trimestral",
    "90d": "trimestral",
    "90_dias": "trimestral",
    "32ocvxm": "semestral",
    "semestral": "semestral",
    "6m": "semestral",
    "6_meses": "semestral",
    "180d": "semestral",
    "180_dias": "semestral",
    "u9wz86m": "anual",
    "anual": "anual",
    "annual": "anual",
    "12m": "anual",
    "12_meses": "anual",
    "365d": "anual",
    "365_dias": "anual",
}

PLAN_LABELS = {
    "mensal": "Plano mensal",
    "trimestral": "Plano trimestral",
    "semestral": "Plano semestral",
    "anual": "Plano anual",
}


def _connect():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_subscriptions_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscription_intents (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                source TEXT NOT NULL DEFAULT 'offline',
                created_at INTEGER NOT NULL,
                used_at INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_subscriptions (
                user_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'active',
                plan_code TEXT NOT NULL,
                plan_name TEXT NOT NULL,
                starts_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                cakto_order_id TEXT,
                cakto_subscription_id TEXT,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscription_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                event_type TEXT,
                user_id INTEGER,
                token TEXT,
                payload_json TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()


def _dict(row):
    return dict(row) if row else None


def create_subscription_intent(user_id: int, username: str = "", full_name: str = "") -> dict:
    init_subscriptions_db()
    token = f"anime_{user_id}_{secrets.token_urlsafe(10)}"
    now = int(time.time())
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO subscription_intents (token, user_id, username, full_name, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (token, int(user_id), username, full_name, now),
        )
        conn.commit()
    return {"token": token, "user_id": int(user_id)}


def get_intent(token: str) -> dict | None:
    init_subscriptions_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM subscription_intents WHERE token = ?",
            (str(token or "").strip(),),
        ).fetchone()
    return _dict(row)


def get_subscription(user_id: int) -> dict | None:
    init_subscriptions_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM user_subscriptions WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
    return _dict(row)


def get_active_subscription(user_id: int) -> dict | None:
    sub = get_subscription(user_id)
    if not sub or sub.get("status") != "active":
        return None
    if int(sub.get("expires_at") or 0) <= int(time.time()):
        return None
    return sub


def is_active_subscriber(user_id: int) -> bool:
    return get_active_subscription(user_id) is not None


def grant_manual_subscription(
    user_id: int,
    days: int,
    *,
    plan_code: str = "manual",
    plan_name: str = "Liberacao manual",
    updated_by: int = 0,
) -> dict:
    init_subscriptions_db()
    normalized_user_id = int(user_id or 0)
    normalized_days = int(days or 0)
    if normalized_user_id <= 0:
        raise ValueError("user_id_invalido")
    if normalized_days <= 0:
        raise ValueError("dias_invalidos")

    now = int(time.time())
    current = get_active_subscription(normalized_user_id)
    base = max(now, int((current or {}).get("expires_at") or 0))
    expires_at = base + (normalized_days * 86400)
    code = _normalize_code(plan_code) or f"manual_{normalized_days}d"
    name = _text(plan_name) or f"Liberacao manual {normalized_days}d"
    event_id = f"manual:{normalized_user_id}:{now}:{int(updated_by or 0)}"
    payload = {
        "source": "manual",
        "user_id": normalized_user_id,
        "days": normalized_days,
        "updated_by": int(updated_by or 0),
        "starts_at": now,
        "expires_at": expires_at,
    }

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO user_subscriptions (
                user_id, status, plan_code, plan_name, starts_at, expires_at,
                cakto_order_id, cakto_subscription_id, updated_at
            )
            VALUES (?, 'active', ?, ?, ?, ?, ?, '', ?)
            ON CONFLICT(user_id) DO UPDATE SET
                status = 'active',
                plan_code = excluded.plan_code,
                plan_name = excluded.plan_name,
                expires_at = excluded.expires_at,
                cakto_order_id = excluded.cakto_order_id,
                cakto_subscription_id = excluded.cakto_subscription_id,
                updated_at = excluded.updated_at
            """,
            (
                normalized_user_id,
                code,
                name,
                now,
                expires_at,
                f"manual:{int(updated_by or 0)}",
                now,
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO subscription_events (
                event_id, event_type, user_id, token, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                "manual_grant",
                normalized_user_id,
                "",
                json.dumps(payload, ensure_ascii=False),
                now,
            ),
        )
        conn.commit()
    return get_subscription(normalized_user_id) or {}


def cancel_manual_subscription(user_id: int, *, updated_by: int = 0) -> dict | None:
    init_subscriptions_db()
    normalized_user_id = int(user_id or 0)
    if normalized_user_id <= 0:
        raise ValueError("user_id_invalido")
    now = int(time.time())
    with _connect() as conn:
        conn.execute(
            "UPDATE user_subscriptions SET status = 'canceled', updated_at = ? WHERE user_id = ?",
            (now, normalized_user_id),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO subscription_events (
                event_id, event_type, user_id, token, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                f"manual_cancel:{normalized_user_id}:{now}:{int(updated_by or 0)}",
                "manual_cancel",
                normalized_user_id,
                "",
                json.dumps({"source": "manual", "user_id": normalized_user_id, "updated_by": int(updated_by or 0)}, ensure_ascii=False),
                now,
            ),
        )
        conn.commit()
    return get_subscription(normalized_user_id)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_code(value: Any) -> str:
    text = _text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def normalize_plan(value: Any) -> str:
    code = _normalize_code(value)
    if not code:
        return ""
    if code in PLAN_ALIASES:
        return PLAN_ALIASES[code]
    for alias, plan in PLAN_ALIASES.items():
        if alias and alias in code:
            return plan
    for plan in ("mensal", "trimestral", "semestral", "anual"):
        if plan in code:
            return plan
    return ""


def plan_label(plan: Any) -> str:
    code = normalize_plan(plan) or _normalize_code(plan)
    return PLAN_LABELS.get(code, _text(plan) or "BaltigoFlix")


def _walk_values(payload: Any):
    if isinstance(payload, dict):
        for key, value in payload.items():
            yield str(key), value
            yield from _walk_values(value)
    elif isinstance(payload, list):
        for value in payload:
            yield from _walk_values(value)


def extract_event_type(payload: dict) -> str:
    for key, value in _walk_values(payload):
        if key.lower() in {"event", "type", "event_type", "event_name", "webhook_event"}:
            event_type = _normalize_code(value)
            if event_type:
                return event_type
    return ""


def is_approved_payload(payload: dict) -> bool:
    event_type = extract_event_type(payload)
    if event_type in APPROVED_EVENTS:
        return True
    for key, value in _walk_values(payload):
        key_l = key.lower()
        if key_l in {"status", "payment_status", "order_status", "purchase_status", "sale_status"}:
            status = _normalize_code(value)
            if status in {"approved", "approve", "paid", "completed", "active", "confirmed"}:
                return True
    return False


def is_cancel_payload(payload: dict) -> bool:
    event_type = extract_event_type(payload)
    if event_type in CANCEL_EVENTS:
        return True
    for key, value in _walk_values(payload):
        key_l = key.lower()
        if key_l in {"status", "payment_status", "order_status", "purchase_status", "sale_status"}:
            status = _normalize_code(value)
            if status in {"refused", "rejected", "canceled", "cancelled", "refunded", "chargeback"}:
                return True
    return False


def extract_token(payload: dict) -> str:
    for key, value in _walk_values(payload):
        if key.lower() in {"external_reference", "ref", "reference", "utm_content", "src", "sck"}:
            text = _text(value)
            match = re.search(r"anime_\d+_[A-Za-z0-9_-]+", text)
            if match:
                return match.group(0)
    blob = json.dumps(payload, ensure_ascii=False)
    match = re.search(r"anime_\d+_[A-Za-z0-9_-]+", blob)
    return match.group(0) if match else ""


def extract_user_id(payload: dict, token: str = "") -> int | None:
    intent = get_intent(token) if token else None
    if intent:
        return int(intent["user_id"])
    for key, value in _walk_values(payload):
        if key.lower() in {"telegram_user_id", "telegram_id", "tg_id", "user_id"}:
            match = re.search(r"\d{5,20}", _text(value))
            if match:
                return int(match.group(0))
    return None


def extract_plan(payload: dict) -> tuple[str, str, int]:
    names = []
    for key, value in _walk_values(payload):
        if key.lower() in {"name", "title", "plan", "plan_name", "product_name", "offer_name"}:
            text = _text(value)
            if text:
                names.append(text)
                code = normalize_plan(text)
                if code:
                    return code, plan_label(code), int(PLAN_DAYS.get(code) or 30)
    blob = " ".join(names).lower()
    for code, days in PLAN_DAYS.items():
        if code in blob:
            return code, " ".join(names)[:120] or "BaltigoFlix", days
    return "mensal", "BaltigoFlix", 30


def _extract_order_ids(payload: dict) -> tuple[str, str]:
    order_id = ""
    subscription_id = ""
    for key, value in _walk_values(payload):
        key_l = key.lower()
        if not order_id and key_l in {"order_id", "orderid", "id"}:
            order_id = _text(value)
        if not subscription_id and key_l in {"subscription_id", "subscriptionid"}:
            subscription_id = _text(value)
    return order_id[:120], subscription_id[:120]


def _extract_event_id(payload: dict, token: str = "") -> str:
    priority = {
        "event_id",
        "webhook_event_id",
        "transaction_id",
        "order_id",
        "sale_id",
        "purchase_id",
        "payment_id",
        "invoice_id",
        "subscription_id",
    }
    for key, value in _walk_values(payload):
        if key.lower() in priority:
            text = _text(value)
            if text:
                return text[:160]
    return _text(payload.get("event_id") or payload.get("id") or token)[:160]


def _event_already_processed(conn: sqlite3.Connection, event_id: str) -> bool:
    if not event_id:
        return False
    row = conn.execute(
        "SELECT 1 FROM subscription_events WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone()
    return bool(row)


def activate_from_cakto(payload: dict) -> dict:
    init_subscriptions_db()
    token = extract_token(payload)
    user_id = extract_user_id(payload, token)
    if not user_id:
        raise ValueError("telegram_id_nao_encontrado")

    plan_code, plan_name, days = extract_plan(payload)
    order_id, subscription_id = _extract_order_ids(payload)
    now = int(time.time())
    current = get_active_subscription(user_id)
    base = max(now, int((current or {}).get("expires_at") or 0))
    expires_at = base + (days * 86400)
    event_id = _extract_event_id(payload, token) or order_id or token
    event_type = extract_event_type(payload)

    with _connect() as conn:
        if _event_already_processed(conn, event_id):
            current_sub = get_subscription(user_id) or {}
            current_sub["duplicate_event"] = True
            return current_sub
        conn.execute(
            """
            INSERT INTO user_subscriptions (
                user_id, status, plan_code, plan_name, starts_at, expires_at,
                cakto_order_id, cakto_subscription_id, updated_at
            )
            VALUES (?, 'active', ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                status = 'active',
                plan_code = excluded.plan_code,
                plan_name = excluded.plan_name,
                expires_at = excluded.expires_at,
                cakto_order_id = excluded.cakto_order_id,
                cakto_subscription_id = excluded.cakto_subscription_id,
                updated_at = excluded.updated_at
            """,
            (user_id, plan_code, plan_name, now, expires_at, order_id, subscription_id, now),
        )
        if token:
            conn.execute("UPDATE subscription_intents SET used_at = ? WHERE token = ?", (now, token))
        conn.execute(
            """
            INSERT OR IGNORE INTO subscription_events (
                event_id, event_type, user_id, token, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_id or None, event_type, user_id, token, json.dumps(payload, ensure_ascii=False), now),
        )
        conn.commit()
    return get_subscription(user_id) or {}


def deactivate_from_cakto(payload: dict) -> dict | None:
    init_subscriptions_db()
    token = extract_token(payload)
    user_id = extract_user_id(payload, token)
    if not user_id:
        raise ValueError("telegram_id_nao_encontrado")
    now = int(time.time())
    with _connect() as conn:
        conn.execute(
            "UPDATE user_subscriptions SET status = 'canceled', updated_at = ? WHERE user_id = ?",
            (now, user_id),
        )
        conn.commit()
    return get_subscription(user_id)
