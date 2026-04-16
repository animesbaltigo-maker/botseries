import json
import secrets
from pathlib import Path

from config import DATA_DIR

START_PAYLOAD_PREFIX = "pb_s_"
_STATE_PATH = Path(DATA_DIR) / "start_payloads.json"


def _load() -> dict:
    if not _STATE_PATH.exists():
        return {}
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(data: dict) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def create_start_payload(payload: dict) -> str:
    data = _load()
    token = secrets.token_hex(8)
    data[token] = payload
    _save(data)
    return token


def get_start_payload(token: str) -> dict:
    return _load().get(token) or {}


def build_start_link(bot_username: str, token: str) -> str:
    username = str(bot_username or "").strip().lstrip("@")
    return f"https://t.me/{username}?start={START_PAYLOAD_PREFIX}{token}"