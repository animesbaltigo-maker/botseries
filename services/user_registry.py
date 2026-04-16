"""Registro simples de usuarios com cache e lock."""

import json
from threading import Lock
from pathlib import Path

from config import DATA_DIR

USERS_FILE = Path(DATA_DIR) / "users.json"

_LOCK = Lock()
_CACHE: dict[str, str] | None = None


def _load_unlocked() -> dict[str, str]:
    global _CACHE

    if _CACHE is not None:
        return dict(_CACHE)

    if not USERS_FILE.exists():
        _CACHE = {}
        return {}

    try:
        with USERS_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        _CACHE = {}
        return {}

    if not isinstance(data, dict):
        _CACHE = {}
        return {}

    normalized = {str(key): str(value or "") for key, value in data.items()}
    _CACHE = dict(normalized)
    return normalized


def _save_unlocked(data: dict[str, str]) -> None:
    global _CACHE

    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    normalized = {str(key): str(value or "") for key, value in data.items()}
    with USERS_FILE.open("w", encoding="utf-8") as file:
        json.dump(normalized, file, ensure_ascii=False, indent=2)
    _CACHE = dict(normalized)


def register_user(user_id: int | None, name: str = "") -> None:
    if not user_id:
        return

    with _LOCK:
        data = _load_unlocked()
        key = str(int(user_id))
        value = str(name or "").strip()
        if data.get(key) == value:
            return
        data[key] = value
        _save_unlocked(data)


def get_all_users() -> list[int]:
    with _LOCK:
        data = _load_unlocked()

    users: list[int] = []
    for key in data:
        try:
            users.append(int(key))
        except Exception:
            continue
    return users


def get_total_users() -> int:
    return len(get_all_users())


def remove_user(user_id: int | None) -> bool:
    if not user_id:
        return False

    with _LOCK:
        data = _load_unlocked()
        key = str(int(user_id))
        if key not in data:
            return False
        del data[key]
        _save_unlocked(data)
        return True
