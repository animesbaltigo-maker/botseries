"""Sistema de bingo para o canal."""
import json
import os
import random
from pathlib import Path
from config import DATA_DIR

BINGO_PATH = str(Path(DATA_DIR) / "bingo.json")
NUMBERS_MIN = 1
NUMBERS_MAX = 75
CARD_SIZE = 6


def _load() -> dict:
    if not os.path.exists(BINGO_PATH):
        return {"active": False, "started": False, "players": {}, "drawn": []}
    try:
        with open(BINGO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"active": False, "started": False, "players": {}, "drawn": []}


def _save(data: dict):
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    with open(BINGO_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_data() -> dict:
    return _load()


def register_player(user_id: int, name: str) -> tuple:
    data = _load()
    uid = str(user_id)

    if data.get("started"):
        return [], "started"

    if uid in data["players"]:
        return data["players"][uid]["numbers"], "exists"

    nums = sorted(random.sample(range(NUMBERS_MIN, NUMBERS_MAX + 1), CARD_SIZE))
    data["players"][uid] = {"name": name, "numbers": nums}
    data["active"] = True
    _save(data)
    return nums, "new"


def start_bingo() -> bool:
    data = _load()
    if data.get("started"):
        return False
    data["started"] = True
    data["drawn"] = []
    _save(data)
    return True


def draw_number() -> int | None:
    data = _load()
    if not data.get("started"):
        return None

    remaining = [n for n in range(NUMBERS_MIN, NUMBERS_MAX + 1) if n not in data["drawn"]]
    if not remaining:
        return None

    n = random.choice(remaining)
    data["drawn"].append(n)
    _save(data)
    return n


def get_ranking() -> list:
    data = _load()
    drawn = set(data.get("drawn", []))
    results = []
    for uid, pdata in data.get("players", {}).items():
        nums = set(pdata.get("numbers", []))
        hits = len(nums & drawn)
        results.append((pdata.get("name", uid), hits, sorted(pdata.get("numbers", []))))
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:3]


def get_almost() -> list:
    """Jogadores a 1 número do bingo."""
    data = _load()
    drawn = set(data.get("drawn", []))
    almost = []
    for uid, pdata in data.get("players", {}).items():
        nums = set(pdata.get("numbers", []))
        hits = len(nums & drawn)
        if hits == CARD_SIZE - 1:
            almost.append((uid, pdata.get("name", uid)))
    return almost


def check_winner() -> dict | None:
    data = _load()
    drawn = set(data.get("drawn", []))
    for uid, pdata in data.get("players", {}).items():
        nums = set(pdata.get("numbers", []))
        if nums.issubset(drawn):
            data["started"] = False
            data["active"] = False
            _save(data)
            return {"name": pdata.get("name", uid), "numbers": sorted(pdata.get("numbers", []))}
    return None


def reset():
    data = {"active": False, "started": False, "players": {}, "drawn": []}
    _save(data)
