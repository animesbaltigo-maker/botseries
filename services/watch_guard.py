import json
import threading
from datetime import datetime, timezone

from config import DATA_DIR

_STATE_PATH = DATA_DIR / "watch_guard.json"
_STATE_LOCK = threading.RLock()
_STATE_CACHE: dict | None = None
_DEFAULT_STATE = {
    "watch_blocked": False,
    "updated_at": "",
    "updated_by": 0,
    "allowed_user_ids": [],
    "allowed_updated_at": "",
    "allowed_updated_by": 0,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_user_ids(values) -> list[int]:
    if not isinstance(values, (list, tuple, set)):
        values = []

    normalized: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            user_id = int(value or 0)
        except (TypeError, ValueError):
            continue
        if user_id <= 0 or user_id in seen:
            continue
        seen.add(user_id)
        normalized.append(user_id)
    normalized.sort()
    return normalized


def _normalize_state(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        payload = {}
    return {
        "watch_blocked": bool(payload.get("watch_blocked")),
        "updated_at": str(payload.get("updated_at") or "").strip(),
        "updated_by": int(payload.get("updated_by") or 0),
        "allowed_user_ids": _normalize_user_ids(payload.get("allowed_user_ids") or []),
        "allowed_updated_at": str(payload.get("allowed_updated_at") or "").strip(),
        "allowed_updated_by": int(payload.get("allowed_updated_by") or 0),
    }


def _read_state() -> dict:
    global _STATE_CACHE
    with _STATE_LOCK:
        if _STATE_CACHE is not None:
            return dict(_STATE_CACHE)
        if not _STATE_PATH.exists():
            _STATE_CACHE = dict(_DEFAULT_STATE)
            return dict(_STATE_CACHE)
        try:
            raw = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            _STATE_CACHE = dict(_DEFAULT_STATE)
            return dict(_STATE_CACHE)
        _STATE_CACHE = _normalize_state(raw)
        return dict(_STATE_CACHE)


def _write_state(payload: dict) -> dict:
    global _STATE_CACHE
    state = _normalize_state(payload)
    with _STATE_LOCK:
        _STATE_CACHE = dict(state)
        _STATE_PATH.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return dict(state)


def get_watch_block_status() -> dict:
    return _read_state()


def is_watch_blocked() -> bool:
    return bool(_read_state().get("watch_blocked"))


def get_watch_allowed_user_ids() -> list[int]:
    return list(_read_state().get("allowed_user_ids") or [])


def is_watch_allowed_for_user(user_id: int) -> bool:
    try:
        normalized_user_id = int(user_id or 0)
    except (TypeError, ValueError):
        normalized_user_id = 0
    if normalized_user_id <= 0:
        return False
    return normalized_user_id in set(get_watch_allowed_user_ids())


def is_watch_block_active_for_user(user_id: int) -> bool:
    return is_watch_blocked() and not is_watch_allowed_for_user(user_id)


def set_watch_blocked(value: bool, *, updated_by: int = 0) -> dict:
    current = _read_state()
    state = {
        "watch_blocked": bool(value),
        "updated_at": _utc_now_iso(),
        "updated_by": int(updated_by or 0),
        "allowed_user_ids": list(current.get("allowed_user_ids") or []),
        "allowed_updated_at": str(current.get("allowed_updated_at") or "").strip(),
        "allowed_updated_by": int(current.get("allowed_updated_by") or 0),
    }
    return _write_state(state)


def add_watch_allowed_users(user_ids: list[int], *, updated_by: int = 0) -> dict:
    current = _read_state()
    merged_ids = _normalize_user_ids(list(current.get("allowed_user_ids") or []) + list(user_ids or []))
    state = {
        **current,
        "allowed_user_ids": merged_ids,
        "allowed_updated_at": _utc_now_iso(),
        "allowed_updated_by": int(updated_by or 0),
    }
    return _write_state(state)


def remove_watch_allowed_users(user_ids: list[int], *, updated_by: int = 0) -> dict:
    current = _read_state()
    removal_ids = set(_normalize_user_ids(user_ids or []))
    remaining_ids = [
        user_id
        for user_id in list(current.get("allowed_user_ids") or [])
        if user_id not in removal_ids
    ]
    state = {
        **current,
        "allowed_user_ids": _normalize_user_ids(remaining_ids),
        "allowed_updated_at": _utc_now_iso(),
        "allowed_updated_by": int(updated_by or 0),
    }
    return _write_state(state)


def clear_watch_allowed_users(*, updated_by: int = 0) -> dict:
    current = _read_state()
    state = {
        **current,
        "allowed_user_ids": [],
        "allowed_updated_at": _utc_now_iso(),
        "allowed_updated_by": int(updated_by or 0),
    }
    return _write_state(state)
