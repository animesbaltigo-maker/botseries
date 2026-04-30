from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from config import (
    CAKTO_ANUAL_CHECKOUT_URL,
    CAKTO_MENSAL_CHECKOUT_URL,
    CAKTO_SEMESTRAL_CHECKOUT_URL,
    CAKTO_TRIMESTRAL_CHECKOUT_URL,
)
from services.subscriptions import (
    activate_from_cakto,
    deactivate_from_cakto,
    normalize_plan,
    plan_label,
)

APPROVED_STATUSES = {"approved", "aprovado", "paid", "pago", "completed", "complete", "active", "ativo"}
REVOKED_STATUSES = {"refunded", "reembolsado", "chargeback", "canceled", "cancelled", "cancelado", "refused", "recusado"}

PLAN_CHECKOUTS = (
    ("mensal", "🔥 Mensal - R$ 19,90", CAKTO_MENSAL_CHECKOUT_URL),
    ("trimestral", "⚡ Trimestral - R$ 39,90", CAKTO_TRIMESTRAL_CHECKOUT_URL),
    ("semestral", "💎 Semestral - R$ 59,90", CAKTO_SEMESTRAL_CHECKOUT_URL),
    ("anual", "🏆 Anual - R$ 129,90", CAKTO_ANUAL_CHECKOUT_URL),
)

_TRACKING_RE = re.compile(
    r"(?:^|[^a-z0-9])(?:tg|telegram)[_-]?(\d{4,})(?:[_-]plan[_-]?([a-z0-9_]+))?",
    re.IGNORECASE,
)


def _plain(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _normalize_url(url: str) -> str:
    url = str(url or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://", "tg://")):
        return url
    return f"https://{url}"


def _append_query_params(url: str, params: dict[str, str]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({key: value for key, value in params.items() if value})
    return urlunparse(parsed._replace(query=urlencode(query)))


def build_checkout_url(base_url: str, user_id: int | str, plan: str) -> str:
    plan_key = normalize_plan(plan)
    url = _normalize_url(base_url)
    if not url or not plan_key:
        return ""

    uid = str(int(user_id)).strip()
    tracking = f"tg_{uid}_plan_{plan_key}"
    return _append_query_params(
        url,
        {
            "src": f"tg_{uid}",
            "sck": tracking,
            "ref": tracking,
            "external_reference": tracking,
            "tg_id": uid,
            "plan": plan_key,
            "utm_source": "telegram",
            "utm_medium": "bot",
            "utm_campaign": "offline_anime",
            "utm_content": plan_key,
        },
    )


def get_checkout_options(user_id: int | str | None) -> list[dict[str, str]]:
    if user_id is None:
        return []
    options = []
    for plan, label, base_url in PLAN_CHECKOUTS:
        url = build_checkout_url(base_url, user_id, plan)
        if url:
            options.append({"plan": plan, "label": label, "url": url})
    return options


def _iter_nodes(value: Any, path: tuple[str, ...] = ()):
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_path = (*path, key_text)
            yield child_path, key_text, child
            yield from _iter_nodes(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_nodes(child, (*path, str(index)))


def _scalar_text(value: Any) -> str:
    if isinstance(value, (str, int, float, bool)):
        return str(value).strip()
    return ""


def _values_for_keys(payload: dict[str, Any], keys: set[str]) -> list[str]:
    values = []
    for _path, key, value in _iter_nodes(payload):
        if _plain(key) in keys:
            text = _scalar_text(value)
            if text:
                values.append(text)
    return values


def _all_scalar_strings(payload: dict[str, Any]) -> list[str]:
    return [_scalar_text(value) for _path, _key, value in _iter_nodes(payload) if _scalar_text(value)]


def extract_status(payload: dict[str, Any]) -> str:
    status_keys = {"status", "payment_status", "order_status", "transaction_status", "subscription_status"}
    for _path, key, value in _iter_nodes(payload):
        if _plain(key) in status_keys:
            status = _plain(_scalar_text(value))
            if status in APPROVED_STATUSES or status in REVOKED_STATUSES:
                return status
    return ""


def extract_access_target(payload: dict[str, Any]) -> dict[str, Any]:
    user_id: int | None = None
    plan = ""

    candidates = []
    tracking_keys = {"src", "sck", "ref", "external_reference", "reference", "utm_content", "metadata", "tracking"}
    for path, key, value in _iter_nodes(payload):
        path_text = "_".join(_plain(item) for item in path)
        if _plain(key) in tracking_keys or any(hint in path_text for hint in tracking_keys):
            text = _scalar_text(value)
            if text:
                candidates.append(text)
    candidates.extend(_all_scalar_strings(payload))

    for value in candidates:
        match = _TRACKING_RE.search(value)
        if not match:
            continue
        user_id = int(match.group(1))
        plan = normalize_plan(match.group(2) or plan)
        break

    if user_id is None:
        for value in _values_for_keys(payload, {"telegram_id", "telegram_user_id", "telegramid", "telegramuserid", "tg_id"}):
            match = re.search(r"\d{4,20}", value)
            if match:
                user_id = int(match.group(0))
                break

    if not plan:
        for value in _values_for_keys(payload, {"plan", "plano", "utm_content", "sck", "ref", "external_reference"}):
            plan = normalize_plan(value)
            if plan:
                break

    return {"user_id": user_id, "plan": plan}


def extract_webhook_secret_values(payload: dict[str, Any]) -> list[str]:
    return _values_for_keys(payload, {"secret", "webhook_secret", "webhooksecret", "cakto_secret", "caktosecret"})


def _event_id(payload: dict[str, Any], user_id: int | None) -> str:
    keys = {"event_id", "webhook_event_id", "transaction_id", "order_id", "sale_id", "purchase_id", "payment_id", "invoice_id", "subscription_id"}
    for value in _values_for_keys(payload, keys):
        return f"cakto:{value}:{user_id or 'unknown'}"
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return "cakto:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def process_cakto_webhook(payload: dict[str, Any]) -> dict[str, Any]:
    status = extract_status(payload)
    target = extract_access_target(payload)
    user_id = target.get("user_id")
    plan = normalize_plan(target.get("plan") or "")
    event_id = _event_id(payload, user_id)

    base = {
        "ok": True,
        "gateway": "cakto",
        "status": status,
        "user_id": user_id,
        "plan": plan,
        "plan_label": plan_label(plan),
        "event_id": event_id,
    }

    if status in REVOKED_STATUSES:
        if not user_id:
            return {**base, "action": "ignored", "reason": "missing_telegram_id"}
        sub = deactivate_from_cakto({**payload, "event_id": event_id, "tg_id": user_id, "plan": plan})
        return {**base, "action": "revoked", "subscription": sub}

    if status in APPROVED_STATUSES:
        if not user_id:
            return {**base, "action": "ignored", "reason": "missing_telegram_id"}
        if not plan:
            return {**base, "action": "ignored", "reason": "missing_plan"}
        sub = activate_from_cakto({**payload, "event_id": event_id, "tg_id": user_id, "plan": plan})
        return {**base, "action": "granted", "subscription": sub}

    return {**base, "action": "ignored", "reason": "event_not_handled"}
