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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on", "sim"}:
        return True
    if raw in {"0", "false", "no", "off", "nao", "não"}:
        return False
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
API_ID = (
    _env_int("API_ID", 0)
    or _env_int("USERBOT_API_ID", 0)
    or _env_int("USERBOT_APP_ID", 0)
    or _env_int("TELETHON_API_ID", 0)
    or _env_int("TELEGRAM_API_ID", 0)
    or _env_int("TG_API_ID", 0)
)
API_HASH = (
    os.getenv("API_HASH", "").strip()
    or os.getenv("USERBOT_API_HASH", "").strip()
    or os.getenv("USERBOT_APP_HASH", "").strip()
    or os.getenv("TELETHON_API_HASH", "").strip()
    or os.getenv("TELEGRAM_API_HASH", "").strip()
    or os.getenv("TG_API_HASH", "").strip()
)
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

_DATA_DIR_RAW = Path(os.getenv("DATA_DIR", "data").strip())
DATA_DIR = _DATA_DIR_RAW if _DATA_DIR_RAW.is_absolute() else BASE_DIR / _DATA_DIR_RAW
DATA_DIR.mkdir(parents=True, exist_ok=True)

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
UPSTREAM_PROXY_URL = (
    os.getenv("UPSTREAM_PROXY_URL", "").strip()
    or os.getenv("SCRAPER_PROXY_URL", "").strip()
    or os.getenv("HTTPS_PROXY", "").strip()
    or os.getenv("HTTP_PROXY", "").strip()
    or os.getenv("ALL_PROXY", "").strip()
)

BOT_API_MAX_CONCURRENT_UPDATES = _env_int("BOT_API_MAX_CONCURRENT_UPDATES", 1000)
BOT_API_CONNECTION_POOL = _env_int("BOT_API_CONNECTION_POOL", 512)
BOT_API_POOL_TIMEOUT = _env_float("BOT_API_POOL_TIMEOUT", 30.0)
BOT_API_CONNECT_TIMEOUT = _env_float("BOT_API_CONNECT_TIMEOUT", 10.0)
BOT_API_READ_TIMEOUT = _env_float("BOT_API_READ_TIMEOUT", 25.0)
BOT_API_WRITE_TIMEOUT = _env_float("BOT_API_WRITE_TIMEOUT", 25.0)
SCRAPER_CONCURRENCY = _env_int("SCRAPER_CONCURRENCY", 64)
SCRAPER_CONNECTION_LIMIT = _env_int("SCRAPER_CONNECTION_LIMIT", 256)

VIDEO_SEND_ENABLED = _env_bool("VIDEO_SEND_ENABLED", True)
VIDEO_SEND_CONCURRENCY = _env_int("VIDEO_SEND_CONCURRENCY", 1)
VIDEO_SEND_MAX_MB = _env_int("VIDEO_SEND_MAX_MB", 1900)
VIDEO_DOWNLOAD_MAX_MB = _env_int("VIDEO_DOWNLOAD_MAX_MB", VIDEO_SEND_MAX_MB)
VIDEO_DOWNLOAD_QUEUE_LIMIT = _env_int("VIDEO_DOWNLOAD_QUEUE_LIMIT", 20)
VIDEO_DOWNLOAD_WORKERS = _env_int("VIDEO_DOWNLOAD_WORKERS", 2)
VIDEO_DOWNLOAD_CHUNK_MB = _env_int("VIDEO_DOWNLOAD_CHUNK_MB", 8)
VIDEO_DOWNLOAD_PARALLEL = _env_bool("VIDEO_DOWNLOAD_PARALLEL", True)
VIDEO_DOWNLOAD_PARALLEL_WORKERS = _env_int("VIDEO_DOWNLOAD_PARALLEL_WORKERS", 8)
VIDEO_DOWNLOAD_PART_MB = _env_int("VIDEO_DOWNLOAD_PART_MB", 8)
VIDEO_DOWNLOAD_TRUST_ENV = _env_bool("VIDEO_DOWNLOAD_TRUST_ENV", False)
VIDEO_DOWNLOAD_CACHE_DIR = os.getenv("VIDEO_DOWNLOAD_CACHE_DIR", str(DATA_DIR / "video_cache")).strip()
VIDEO_CACHE_TTL_HOURS = _env_int("VIDEO_CACHE_TTL_HOURS", 1)
VIDEO_CACHE_CLEANUP_INTERVAL_SECONDS = _env_int("VIDEO_CACHE_CLEANUP_INTERVAL_SECONDS", 600)
VIDEO_UPLOAD_MAX_MB = _env_int("VIDEO_UPLOAD_MAX_MB", 49)
TELETHON_UPLOAD_MAX_MB = _env_int("TELETHON_UPLOAD_MAX_MB", VIDEO_SEND_MAX_MB)
TELETHON_PARALLEL_UPLOAD = _env_bool("TELETHON_PARALLEL_UPLOAD", True)
TELETHON_PARALLEL_UPLOAD_THRESHOLD_MB = _env_int("TELETHON_PARALLEL_UPLOAD_THRESHOLD_MB", 20)
TELETHON_PARALLEL_UPLOAD_WORKERS = (
    _env_int("TELETHON_PARALLEL_UPLOAD_WORKERS", 0)
    or _env_int("USERBOT_UPLOAD_WORKERS", 0)
    or 16
)
TELETHON_SESSION_NAME = (
    os.getenv("TELETHON_SESSION_NAME", "").strip()
    or os.getenv("USERBOT_SESSION_NAME", "").strip()
    or str(DATA_DIR / "pobreflix_uploader_bot")
)
VIDEO_DOWNLOAD_PROTECT_CONTENT = _env_bool("VIDEO_DOWNLOAD_PROTECT_CONTENT", True)
VIDEO_SEND_TIMEOUT = _env_int("VIDEO_SEND_TIMEOUT", 3600)
VIDEO_TMP_DIR = Path(os.getenv("VIDEO_TMP_DIR", str(DATA_DIR / "video_tmp")).strip())
VIDEO_TMP_DIR.mkdir(parents=True, exist_ok=True)
EPISODE_CACHE_CHAT_ID = os.getenv("EPISODE_CACHE_CHAT_ID", "").strip()

WATCH_BLOCK_BRAND = os.getenv("WATCH_BLOCK_BRAND", "BaltigoFlix").strip() or "BaltigoFlix"
WATCH_BLOCK_URL = (
    os.getenv("WATCH_BLOCK_URL", "http://baltigoflix.com.br/").strip()
    or "http://baltigoflix.com.br/"
)
WATCH_BLOCK_PROMO_COOLDOWN = _env_int("WATCH_BLOCK_PROMO_COOLDOWN", 45)

BALTIGOFLIX_SUBSCRIBE_URL = os.getenv("BALTIGOFLIX_SUBSCRIBE_URL", "http://baltigoflix.com.br/").strip()
BALTIGOFLIX_SUPPORT_URL = os.getenv("BALTIGOFLIX_SUPPORT_URL", "https://t.me/SourceBaltigo_Bot").strip()
_SUBSCRIPTIONS_DB_RAW = Path(
    os.getenv("SUBSCRIPTIONS_DB_PATH", "").strip()
    or os.getenv("BALTIGO_SUBSCRIPTIONS_DB_PATH", "").strip()
    or str(DATA_DIR / "offline_subscriptions.sqlite3")
)
SUBSCRIPTIONS_DB_PATH = str(
    _SUBSCRIPTIONS_DB_RAW if _SUBSCRIPTIONS_DB_RAW.is_absolute() else BASE_DIR / _SUBSCRIPTIONS_DB_RAW
)
CAKTO_CHECKOUT_URL = os.getenv("CAKTO_CHECKOUT_URL", "").strip()
CAKTO_MENSAL_CHECKOUT_URL = os.getenv("CAKTO_MENSAL_CHECKOUT_URL", "https://pay.cakto.com.br/9snqsP3").strip()
CAKTO_TRIMESTRAL_CHECKOUT_URL = os.getenv("CAKTO_TRIMESTRAL_CHECKOUT_URL", "https://pay.cakto.com.br/3fsy24d").strip()
CAKTO_SEMESTRAL_CHECKOUT_URL = os.getenv("CAKTO_SEMESTRAL_CHECKOUT_URL", "https://pay.cakto.com.br/32ocvxm").strip()
CAKTO_ANUAL_CHECKOUT_URL = os.getenv("CAKTO_ANUAL_CHECKOUT_URL", "https://pay.cakto.com.br/u9wz86m").strip()
CAKTO_BRONZE_CHECKOUT_URL = os.getenv("CAKTO_BRONZE_CHECKOUT_URL", CAKTO_MENSAL_CHECKOUT_URL).strip()
CAKTO_OURO_CHECKOUT_URL = os.getenv("CAKTO_OURO_CHECKOUT_URL", CAKTO_TRIMESTRAL_CHECKOUT_URL).strip()
CAKTO_DIAMANTE_CHECKOUT_URL = os.getenv("CAKTO_DIAMANTE_CHECKOUT_URL", CAKTO_SEMESTRAL_CHECKOUT_URL).strip()
CAKTO_RUBI_CHECKOUT_URL = os.getenv("CAKTO_RUBI_CHECKOUT_URL", CAKTO_ANUAL_CHECKOUT_URL).strip()
CAKTO_WEBHOOK_SECRET = os.getenv("CAKTO_WEBHOOK_SECRET", "").strip()
CAKTO_CLIENT_ID = os.getenv("CAKTO_CLIENT_ID", "").strip()
CAKTO_CLIENT_SECRET = os.getenv("CAKTO_CLIENT_SECRET", "").strip()
CAKTO_API_BASE_URL = os.getenv("CAKTO_API_BASE_URL", "https://api.cakto.com.br").strip().rstrip("/")
CAKTO_ORDER_SYNC_LIMIT = _env_int("CAKTO_ORDER_SYNC_LIMIT", 100)
