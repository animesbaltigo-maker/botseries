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
}


def _normalize_state(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        payload = {}
    return {
        "watch_blocked": bool(payload.get("watch_blocked")),
        "updated_at": str(payload.get("updated_at") or "").strip(),
        "updated_by": int(payload.get("updated_by") or 0),
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


def set_watch_blocked(value: bool, *, updated_by: int = 0) -> dict:
    state = {
        "watch_blocked": bool(value),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_by": int(updated_by or 0),
    }
    return _write_state(state)
