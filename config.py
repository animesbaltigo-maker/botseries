import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _load_local_env() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    try:
        raw_lines = env_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return

    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


_load_local_env()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _normalize_telegram_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith(("https://", "http://")):
        return raw
    if raw.startswith(("t.me/", "telegram.me/")):
        return f"https://{raw.lstrip('/')}"
    if raw.startswith("@"):
        raw = raw[1:]
    return f"https://t.me/{raw.lstrip('/')}"


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "SeriesBrazilBot").strip().lstrip("@")
BOT_BRAND = os.getenv("BOT_BRAND", "Series Brazil Bot").strip() or "Series Brazil Bot"

SOURCE_SITE_BASE = os.getenv("SOURCE_SITE_BASE", "https://www.pobreflixtv.hair").strip().rstrip("/")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()
TMDB_ACCESS_TOKEN = os.getenv("TMDB_ACCESS_TOKEN", "").strip()

REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "").strip()
REQUIRED_CHANNEL_URL = _normalize_telegram_url(
    os.getenv("REQUIRED_CHANNEL_URL", "").strip() or REQUIRED_CHANNEL
)
CANAL_POSTAGEM = os.getenv("CANAL_POSTAGEM", "").strip()

STICKER_DIVISOR = os.getenv(
    "STICKER_DIVISOR",
    "CAACAgQAAx0CbKkU-AACFJtps_kRLpeUt2Gvd7mT4d0gS1vyCgACOhUAAqDAiFJSU5pkUMltvzoE",
).strip()

ADMIN_IDS = [
    int(value.strip())
    for value in os.getenv("ADMIN_IDS", "").split(",")
    if value.strip().isdigit()
]


ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

DATA_DIR = Path(os.getenv("DATA_DIR", "data").strip())

STICKER_DIVISOR = os.getenv("STICKER_DIVISOR", "").strip()
CANAL_POSTAGEM = os.getenv("CANAL_POSTAGEM", "").strip()
CANAL_POSTAGEM_EPISODIOS = os.getenv("CANAL_POSTAGEM_EPISODIOS", CANAL_POSTAGEM).strip()
CANAL_ATUALIZACOES_TAG = os.getenv("CANAL_ATUALIZACOES_TAG", "@AtualizacoesOn").strip()
CANAL_POSTAGEM_DESENHOS = os.getenv("CANAL_POSTAGEM_DESENHOS", "").strip()

SEARCH_LIMIT = _env_int("SEARCH_LIMIT", 10)
SEARCH_PAGE_SIZE = _env_int("SEARCH_PAGE_SIZE", 8)
EPISODES_PER_PAGE = _env_int("EPISODES_PER_PAGE", 12)
RECENT_ITEMS_LIMIT = _env_int("RECENT_ITEMS_LIMIT", 12)
ANTI_FLOOD_SECONDS = _env_float("ANTI_FLOOD_SECONDS", 1.0)
HTTP_TIMEOUT = _env_int("HTTP_TIMEOUT", 35)
SEARCH_SESSION_TTL_SECONDS = _env_int("SEARCH_SESSION_TTL_SECONDS", 7200)
GROUP_AI_HTTP_TIMEOUT = _env_int("GROUP_AI_HTTP_TIMEOUT", 10)

BOT_API_MAX_CONCURRENT_UPDATES = _env_int("BOT_API_MAX_CONCURRENT_UPDATES", 1000)
BOT_API_CONNECTION_POOL = _env_int("BOT_API_CONNECTION_POOL", 512)
BOT_API_POOL_TIMEOUT = _env_float("BOT_API_POOL_TIMEOUT", 30.0)
BOT_API_CONNECT_TIMEOUT = _env_float("BOT_API_CONNECT_TIMEOUT", 10.0)
BOT_API_READ_TIMEOUT = _env_float("BOT_API_READ_TIMEOUT", 25.0)
BOT_API_WRITE_TIMEOUT = _env_float("BOT_API_WRITE_TIMEOUT", 25.0)
SCRAPER_CONCURRENCY = _env_int("SCRAPER_CONCURRENCY", 64)
SCRAPER_CONNECTION_LIMIT = _env_int("SCRAPER_CONNECTION_LIMIT", 256)
