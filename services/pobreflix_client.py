"""
Cliente do catalogo - scraper para series e filmes.
Fonte base configuravel via SOURCE_SITE_BASE.
"""

import asyncio
import copy
import ast
import base64
from difflib import SequenceMatcher
import json
import logging
import re
import time
import unicodedata
from typing import Optional
from urllib.parse import parse_qs, unquote_plus, urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

from config import (
    HTTP_TIMEOUT,
    SCRAPER_CONCURRENCY,
    SCRAPER_CONNECTION_LIMIT,
    SEARCH_LIMIT,
    SOURCE_SITE_BASE,
    UPSTREAM_PROXY_URL,
)
from services.tmdb_client import enrich_media_metadata

try:
    from Cryptodome.Cipher import AES
except Exception:
    AES = None

BASE_URL = SOURCE_SITE_BASE
SEARCH_URL = f"{BASE_URL}/pesquisar/"
LOGGER = logging.getLogger(__name__)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "Referer": f"{BASE_URL}/",
    "Origin": BASE_URL,
}

# ── Caches simples ──────────────────────────────────────────────────────────
_SEARCH_CACHE: dict[str, tuple[float, list]] = {}
_DETAIL_CACHE: dict[str, tuple[float, dict]] = {}
_EPISODES_CACHE: dict[str, tuple[float, list]] = {}

PLAYER_SERVER_ORDER = ("streamtape",)
PLAYER_SERVER_LABELS = {
    "byse": "Byse",
    "doodstream": "DoodStream",
    "streamtape": "StreamTape",
    "mixdrop": "MixDrop",
}

CACHE_TTL_SEARCH  = 60 * 10   # 10 min
CACHE_TTL_DETAIL  = 60 * 30   # 30 min
CACHE_TTL_EPISODES = 60 * 5   # 5 min

# Semáforo global para não massacrar o site
_SEM = asyncio.Semaphore(SCRAPER_CONCURRENCY)
_SESSION: aiohttp.ClientSession | None = None
_SESSION_LOCK = asyncio.Lock()
_PROXY_SENSITIVE_HOSTS = (
    "streamtape.com",
    "tapecontent.net",
    "tpead.net",
)


def _now() -> float:
    return time.monotonic()


def _cached(cache: dict, key: str, ttl: float):
    item = cache.get(key)
    if item and (_now() - item[0]) < ttl:
        return item[1]
    return None


def _set_cache(cache: dict, key: str, value):
    cache[key] = (_now(), value)


def _delete_cache(cache: dict, key: str) -> None:
    cache.pop(key, None)


def _looks_temporary_media_url(url: str) -> bool:
    value = (url or "").strip().lower()
    if not value:
        return False

    if "streamtape.com/get_video" in value or "/get_video?" in value:
        return True

    volatile_bits = ("expires=", "token=", "ip=")
    if any(bit in value for bit in volatile_bits):
        return True

    return False


def _episodes_payload_is_volatile(episodes: list[dict]) -> bool:
    for episode in episodes or []:
        if not isinstance(episode, dict):
            continue

        if _looks_temporary_media_url(str(episode.get("url") or "")):
            return True

        player_links = episode.get("player_links") or {}
        if isinstance(player_links, dict):
            if _looks_temporary_media_url(str(player_links.get("player_url") or "")):
                return True

            downloads = player_links.get("downloads") or {}
            if isinstance(downloads, dict):
                for item in downloads.values():
                    if not isinstance(item, dict):
                        continue
                    if _looks_temporary_media_url(str(item.get("url") or "")):
                        return True

    return False


def _clone_episodes(episodes: list[dict]) -> list[dict]:
    return copy.deepcopy(episodes or [])


def _normalize_search_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text).lower().strip()
    return re.sub(r"\s+", " ", text)


def _query_requests_audio(query: str, audio_key: str) -> bool:
    normalized = _normalize_search_text(query)
    return audio_key in normalized.split()


def _search_item_unique_key(item: dict) -> str:
    url = str(item.get("url") or "").strip()
    title = _normalize_search_text(item.get("title") or "")
    year = str(item.get("year") or "").strip()
    duration = str(item.get("duration") or "").strip()
    audio_key = "dublado" if item.get("is_dubbed") else "legendado"
    return url or f"{title}|{year}|{duration}|{audio_key}"


def _search_group_key(item: dict) -> str:
    title = _normalize_search_text(item.get("title") or "")
    year = str(item.get("year") or "").strip()
    duration = str(item.get("duration") or "").strip()
    if year:
        return f"{title}|{year}"
    return f"{title}|{duration}"


def _infer_search_item_type(item: dict) -> str:
    candidate_urls = [str(item.get("url") or "").strip().lower()]
    audio_urls = item.get("audio_urls") or {}
    if isinstance(audio_urls, dict):
        candidate_urls.extend(str(value or "").strip().lower() for value in audio_urls.values())

    for value in candidate_urls:
        if not value:
            continue
        if "/series/" in value or re.search(r"-u\d+-", value) or re.search(r"-\d+x\d+(?:-|/|$)", value):
            return "series"

    duration_text = str(item.get("duration") or "").strip().lower()
    duration_match = re.search(r"(\d+)\s*min", duration_text)
    normalized_title = _normalize_search_text(item.get("title") or "")
    if duration_match and "filme" not in normalized_title and "movie" not in normalized_title:
        try:
            duration_minutes = int(duration_match.group(1))
        except ValueError:
            duration_minutes = 0
        if 1 <= duration_minutes <= 70:
            return "series"
    return "movie"


def _merge_audio_urls_from_item(target: dict[str, str], item: dict) -> None:
    item_audio_urls = item.get("audio_urls") or {}
    if isinstance(item_audio_urls, dict):
        for audio_key in ("dublado", "legendado"):
            audio_url = str(item_audio_urls.get(audio_key) or "").strip()
            if audio_url and audio_key not in target:
                target[audio_key] = audio_url
    item_url = str(item.get("url") or "").strip()
    if not item_url:
        return
    audio_key = "dublado" if item.get("is_dubbed") else "legendado"
    if audio_key not in target:
        target[audio_key] = item_url


def _collapse_search_results(query: str, items: list[dict]) -> list[dict]:
    if not items:
        return []

    prefer_legendado = _query_requests_audio(query, "legendado")
    prefer_dublado = _query_requests_audio(query, "dublado") and not prefer_legendado
    grouped: dict[str, dict] = {}

    for item in items:
        group_key = _search_group_key(item)
        if not group_key:
            continue

        current = grouped.get(group_key)
        if current is None:
            current = dict(item)
            current["audio_urls"] = {}
            grouped[group_key] = current

        _merge_audio_urls_from_item(current["audio_urls"], item)

        if not str(current.get("image") or "").strip() and str(item.get("image") or "").strip():
            current["image"] = item["image"]
        if not str(current.get("duration") or "").strip() and str(item.get("duration") or "").strip():
            current["duration"] = item["duration"]

        current_type = str(current.get("type") or "").strip().lower()
        item_type = str(item.get("type") or _infer_search_item_type(item)).strip().lower()
        if item_type == "series" or not current_type:
            current["type"] = item_type or current_type or "movie"

    collapsed: list[dict] = []
    for item in grouped.values():
        audio_urls = {
            audio_key: str(url or "").strip()
            for audio_key, url in (item.get("audio_urls") or {}).items()
            if str(url or "").strip()
        }
        audio_options = [audio_key for audio_key in ("dublado", "legendado") if audio_urls.get(audio_key)]

        default_audio = str(item.get("default_audio") or "").strip().lower()
        if prefer_legendado and "legendado" in audio_options:
            default_audio = "legendado"
        elif prefer_dublado and "dublado" in audio_options:
            default_audio = "dublado"
        elif default_audio not in audio_options:
            if "dublado" in audio_options:
                default_audio = "dublado"
            elif audio_options:
                default_audio = audio_options[0]
            else:
                default_audio = "dublado" if item.get("is_dubbed") else "legendado"

        chosen_url = str(audio_urls.get(default_audio) or item.get("url") or "").strip()
        if chosen_url:
            item["url"] = chosen_url

        item["audio_urls"] = audio_urls
        item["audio_options"] = audio_options
        item["default_audio"] = default_audio
        item["is_dubbed"] = default_audio == "dublado"
        item["audio"] = "Dual" if len(audio_options) > 1 else ("Dublado" if default_audio == "dublado" else "Legendado")
        item["type"] = str(item.get("type") or _infer_search_item_type(item)).strip().lower() or "movie"
        collapsed.append(item)

    return collapsed


def _item_supports_audio(item: dict, audio_key: str) -> bool:
    audio_urls = item.get("audio_urls") or {}
    if isinstance(audio_urls, dict) and str(audio_urls.get(audio_key) or "").strip():
        return True

    audio_options = [str(option).strip().lower() for option in (item.get("audio_options") or []) if str(option).strip()]
    if audio_key in audio_options:
        return True

    if audio_key == "dublado":
        return bool(item.get("is_dubbed"))
    return not bool(item.get("is_dubbed"))


def _search_score(query: str, item: dict) -> int:
    normalized_query = _normalize_search_text(query)
    normalized_title = _normalize_search_text(item.get("title") or "")
    if not normalized_query or not normalized_title:
        return 0

    score = 0
    if normalized_query == normalized_title:
        score += 1000
    if normalized_title.startswith(normalized_query):
        score += 700
    if normalized_query in normalized_title:
        score += 500

    query_words = normalized_query.split()
    title_words = normalized_title.split()
    query_set = set(query_words)
    title_set = set(title_words)
    overlap = len(query_set & title_set)
    score += overlap * 80

    if query_words and all(word in title_set for word in query_words[: min(3, len(query_words))]):
        score += 140

    query_year = re.search(r"\b(19|20)\d{2}\b", normalized_query)
    item_year = str(item.get("year") or "").strip()
    if query_year:
        if item_year == query_year.group(0):
            score += 220
        elif item_year:
            score -= 120

    if "dublado" in normalized_query and _item_supports_audio(item, "dublado"):
        score += 35
    if "legendado" in normalized_query and _item_supports_audio(item, "legendado"):
        score += 35

    ratio = SequenceMatcher(None, normalized_query, normalized_title).ratio()
    score += int(ratio * 220)
    if ratio >= 0.92:
        score += 120
    elif ratio >= 0.78:
        score += 60

    score -= max(0, len(title_words) - len(query_words)) * 3
    return score


def _rank_search_results(query: str, items: list[dict]) -> list[dict]:
    normalized_query = _normalize_search_text(query)
    if not normalized_query:
        return items[:SEARCH_LIMIT]

    query_words = [word for word in normalized_query.split() if len(word) >= 2]
    scored: list[tuple[int, str, dict]] = []
    for item in items:
        normalized_title = _normalize_search_text(item.get("title") or "")
        if not normalized_title:
            continue

        score = _search_score(query, item)
        if score < 100 and not any(word in normalized_title for word in query_words):
            continue
        scored.append((score, normalized_title, item))

    if not scored:
        return []

    scored.sort(key=lambda entry: (entry[0], -len(entry[1])), reverse=True)
    best_score = scored[0][0]
    if best_score < 160:
        return []

    min_score = 120 if len(query_words) <= 1 else 150
    min_score = max(min_score, best_score - 260)
    return [item for score, _, item in scored if score >= min_score][:SEARCH_LIMIT]


async def _collect_search_attempts(query: str) -> list[dict]:
    html_text = ""
    last_error: Exception | None = None
    fallback_items: list[dict] = []
    attempts = [
        ("GET", None, {"p": query}),
        ("POST", {"p": query}, None),
        ("GET", None, {"s": query}),
        ("POST", {"s": query}, None),
        ("GET", None, {"search": query}),
        ("POST", {"term": query}, None),
        ("GET", None, {"term": query}),
    ]
    collected_items: dict[str, dict] = {}

    for method, data, params in attempts:
        attempt_fields: set[str] = set()
        if isinstance(data, dict):
            attempt_fields.update(data.keys())
        if isinstance(params, dict):
            attempt_fields.update(params.keys())

        try:
            if method == "POST":
                html_text = await _post(SEARCH_URL, data=data, referer=f"{BASE_URL}/")
            else:
                html_text = await _get(SEARCH_URL, params=params, referer=f"{BASE_URL}/")
        except Exception as error:
            last_error = error
            continue

        soup = BeautifulSoup(html_text, "html.parser")
        parsed = _parse_items(soup)
        seen_urls: set[str] = set()
        unique: list[dict] = []
        for item in parsed:
            unique_key = _search_item_unique_key(item)
            if not unique_key or unique_key in seen_urls:
                continue
            seen_urls.add(unique_key)
            unique.append(item)

        if unique and not fallback_items:
            fallback_items = unique

        for item in unique:
            unique_key = _search_item_unique_key(item)
            if unique_key and unique_key not in collected_items:
                collected_items[unique_key] = item

        if "p" in attempt_fields and collected_items:
            return list(collected_items.values()) or fallback_items

    if last_error is not None and not html_text and not collected_items:
        raise last_error

    return list(collected_items.values()) or fallback_items


async def _get_session() -> aiohttp.ClientSession:
    global _SESSION

    if _SESSION is not None and not _SESSION.closed:
        return _SESSION

    async with _SESSION_LOCK:
        if _SESSION is None or _SESSION.closed:
            connector = aiohttp.TCPConnector(limit=SCRAPER_CONNECTION_LIMIT, ttl_dns_cache=300)
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
            _SESSION = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                # We pass the proxy explicitly per request so we can truly
                # alternate between direct and proxied routes when needed.
                trust_env=False,
            )
    return _SESSION


async def close_catalog_client() -> None:
    global _SESSION

    async with _SESSION_LOCK:
        if _SESSION is not None and not _SESSION.closed:
            await _SESSION.close()
        _SESSION = None


def _absolute_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith("//"):
        return f"https:{url}"
    if url.startswith(("http://", "https://")):
        return url
    if url.startswith("/"):
        return f"{BASE_URL}{url}"
    return f"{BASE_URL}/{url.lstrip('/')}"


def _url_host(url: str) -> str:
    try:
        return urlparse((url or "").strip()).netloc.lower()
    except Exception:
        return ""


def _should_prefer_proxy(url: str) -> bool:
    host = _url_host(url)
    if not host or not UPSTREAM_PROXY_URL:
        return False
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in _PROXY_SENSITIVE_HOSTS)


def _request_proxy_chain(url: str) -> list[str | None]:
    if not UPSTREAM_PROXY_URL:
        return [None]
    if _should_prefer_proxy(url):
        return [UPSTREAM_PROXY_URL, None]
    return [None, UPSTREAM_PROXY_URL]


def _normalize_server_name(server: str) -> str:
    server = (server or "").strip().lower()
    if server == "filemoon":
        return "byse"
    return server


def _server_label(server: str) -> str:
    normalized = _normalize_server_name(server)
    return PLAYER_SERVER_LABELS.get(normalized, normalized.title())


def _decode_possible_escaped_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""

    value = value.replace("\\/", "/")
    value = value.replace("\\u0026", "&")
    value = value.replace("\\x26", "&")
    value = value.replace("&amp;", "&")
    return value.strip(" '\"")


def _base64_url_decode(value: str) -> bytes:
    value = (value or "").strip()
    if not value:
        return b""

    normalized = value.replace("-", "+").replace("_", "/")
    normalized += "=" * ((4 - len(normalized) % 4) % 4)
    return base64.b64decode(normalized)


def _is_source_site_url(url: str) -> bool:
    try:
        host = urlparse((url or "").strip()).netloc.lower()
        source_host = urlparse(BASE_URL).netloc.lower()
    except Exception:
        return False
    return bool(source_host) and source_host in host


def _is_direct_video_url(url: str) -> bool:
    value = (url or "").lower()
    blocked_fragments = (
        "open-graph.mp4",
        "/preview.mp4",
        "/poster.mp4",
        "/thumb.mp4",
        "/thumbnail.mp4",
        "/logo.mp4",
    )
    if any(fragment in value for fragment in blocked_fragments):
        return False
    return any(
        token in value
        for token in (
            ".m3u8",
            ".mp4",
            "googlevideo.com/videoplayback",
            "/videoplayback?",
            "streamtape.com/get_video?",
            "/get_video?",
        )
    )


def _is_unwanted_external_target(url: str) -> bool:
    try:
        parsed = urlparse((url or "").strip())
    except Exception:
        return False

    host = parsed.netloc.lower()
    if not host:
        return False

    blocked_hosts = (
        "one.one.one.one",
        "etv-embed.icu",
        "discord.com",
        "discord.gg",
        "discordapp.com",
        "facebook.com",
        "twitter.com",
        "x.com",
        "reddit.com",
        "t.me",
        "telegram.me",
        "youtube.com",
        "youtu.be",
    )
    return any(blocked in host for blocked in blocked_hosts)


def _direct_video_score(url: str) -> tuple[int, int]:
    value = (url or "").lower()
    score = 0
    if "stream=1" in value:
        score += 80
    if "get_video" in value:
        score += 40
    if ".m3u8" in value:
        score += 50
    if "1080" in value or "itag=37" in value:
        score += 30
    elif "720" in value or "itag=22" in value:
        score += 20
    elif "480" in value or "360" in value or "itag=18" in value:
        score += 10
    return score, len(url or "")


def _pick_best_direct_video_url(urls: list[str]) -> str:
    cleaned = [str(url or "").strip() for url in urls if str(url or "").strip()]
    if not cleaned:
        return ""
    return max(cleaned, key=_direct_video_score)


def _normalize_streamtape_video_url(url: str, base_url: str = "") -> str:
    candidate = _decode_possible_escaped_url(url)
    if not candidate:
        return ""

    lowered = candidate.lower()
    if "streamtape.com" not in lowered and "tpead.net" not in lowered and "get_video" not in lowered:
        return ""

    try:
        base_host = urlparse(base_url).netloc.lower()
    except Exception:
        base_host = ""
    preferred_host = "tpead.net" if "tpead.net" in lowered or "tpead.net" in base_host else "streamtape.com"

    direct_match = re.search(r"(get_video\?id=[^\"'\s<]+)", candidate, flags=re.IGNORECASE)
    if direct_match:
        normalized = f"https://{preferred_host}/{direct_match.group(1)}"
    else:
        normalized = ""

    if not normalized:
        if candidate.startswith(("http://", "https://")):
            normalized = candidate
        elif candidate.startswith("//"):
            normalized = f"https:{candidate}"
        elif candidate.startswith(("/streamtape.com/", "/tpead.net/")):
            normalized = f"https://{candidate.lstrip('/')}"
        elif candidate.startswith(("streamtape.com/", "tpead.net/")):
            normalized = f"https://{candidate}"
        elif candidate.startswith("/get_video"):
            normalized = f"https://{preferred_host}{candidate}"
        elif candidate.startswith("get_video"):
            normalized = f"https://{preferred_host}/{candidate.lstrip('/')}"

    if not normalized and base_url:
        if "streamtape.com" in base_host or "tpead.net" in base_host:
            normalized = urljoin(base_url, candidate)

    if not normalized:
        return ""

    if "get_video" in normalized and "stream=1" not in normalized:
        separator = "&" if "?" in normalized else "?"
        normalized = f"{normalized}{separator}stream=1"
    return normalized


def _decode_js_string_literal(value: str) -> str:
    raw = str(value or "").strip()
    if len(raw) < 2 or raw[0] not in {"'", '"'} or raw[-1] != raw[0]:
        return ""
    try:
        return ast.literal_eval(raw)
    except Exception:
        inner = raw[1:-1]
        inner = inner.replace("\\/", "/")
        inner = inner.replace('\\"', '"').replace("\\'", "'")
        inner = inner.replace("\\n", "\n").replace("\\t", "\t")
        return inner


def _split_js_concat_expression(expr: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quote = ""
    escape = False
    depth = 0

    for char in str(expr or ""):
        if quote:
            current.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = ""
            continue

        if char in {"'", '"'}:
            quote = char
            current.append(char)
            continue
        if char == "(":
            depth += 1
            current.append(char)
            continue
        if char == ")":
            depth = max(0, depth - 1)
            current.append(char)
            continue
        if char == "+" and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(char)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _evaluate_simple_js_string_expr(expr: str) -> str:
    resolved_parts: list[str] = []
    for part in _split_js_concat_expression(expr):
        literal_match = re.fullmatch(r"""(['"])(?:\\.|(?!\1).)*\1""", part)
        if literal_match:
            resolved_parts.append(_decode_js_string_literal(part))
            continue

        string_match = re.search(r"""(['"])(?:\\.|(?!\1).)*\1""", part)
        if not string_match:
            continue

        raw_string = string_match.group(0)
        value = _decode_js_string_literal(raw_string)
        if not value:
            continue

        for offset in re.findall(r"\.substring\((\d+)\)", part):
            try:
                value = value[int(offset):]
            except Exception:
                continue
        resolved_parts.append(value)

    return "".join(resolved_parts).strip()


def _extract_streamtape_runtime_urls(html_text: str, base_url: str = "") -> list[str]:
    if not html_text:
        return []

    found: list[str] = []
    seen: set[str] = set()
    pattern = re.compile(
        r"""document\.getElementById\((['"])[^'"]+\1\)\.innerHTML\s*=\s*([^;]+);""",
        flags=re.IGNORECASE,
    )

    for match in pattern.finditer(html_text):
        expr = str(match.group(2) or "").strip()
        candidate = _evaluate_simple_js_string_expr(expr)
        normalized = _normalize_streamtape_video_url(candidate, base_url=base_url)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        found.append(normalized)

    return found


def _extract_direct_video_urls(html_text: str, base_url: str = "") -> list[str]:
    if not html_text:
        return []

    candidates = []
    seen = set()

    def _push(url: str):
        candidate = _decode_possible_escaped_url(url)
        if not candidate:
            return

        streamtape_candidate = _normalize_streamtape_video_url(candidate, base_url=base_url)
        if streamtape_candidate:
            candidate = streamtape_candidate
        elif base_url:
            candidate = urljoin(base_url, candidate)

        if not candidate.startswith(("http://", "https://")):
            return
        if not _is_direct_video_url(candidate):
            return
        if candidate in seen:
            return

        seen.add(candidate)
        candidates.append(candidate)

    patterns = [
        r'https?://[^\s"\'<>\\]+\.m3u8(?:\?[^\s"\'<>\\]*)?',
        r'https?://[^\s"\'<>\\]+\.mp4(?:\?[^\s"\'<>\\]*)?',
        r'https?://[^\s"\'<>\\]*googlevideo\.com/videoplayback[^\s"\'<>\\]*',
        r'(?:https?:)?//streamtape\.com/get_video[^\s"\'<>\\]*',
        r'/streamtape\.com/get_video[^\s"\'<>\\]*',
        r'/get_video[^\s"\'<>\\]*',
        r'https?:\\/\\/[^\s"\'<>\\]+\.m3u8(?:\?[^\s"\'<>\\]*)?',
        r'https?:\\/\\/[^\s"\'<>\\]+\.mp4(?:\?[^\s"\'<>\\]*)?',
        r'https?:\\/\\/[^\s"\'<>\\]*googlevideo\.com\\/videoplayback[^\s"\'<>\\]*',
        r'(?:https?:\\/\\/|\\/\\/)?streamtape\.com\\/get_video[^\s"\'<>\\]*',
        r'\\/streamtape\.com\\/get_video[^\s"\'<>\\]*',
        r'\\/get_video[^\s"\'<>\\]*',
    ]

    soup = BeautifulSoup(html_text, "html.parser")
    streamtape_runtime_urls = _extract_streamtape_runtime_urls(html_text, base_url=base_url)
    for runtime_url in streamtape_runtime_urls:
        _push(runtime_url)

    if not streamtape_runtime_urls:
        for node_id in ("norobotlink", "robotlink", "ideoolink", "ideoooolink", "botlink", "captchalink"):
            node = soup.select_one(f"#{node_id}")
            if node:
                _push(node.get_text(" ", strip=True))

    for pattern in patterns:
        for match in re.findall(pattern, html_text, flags=re.IGNORECASE):
            if streamtape_runtime_urls and "get_video" in str(match):
                continue
            _push(match)

    for tag in soup.find_all(["source", "video"]):
        for attr in ("src", "data-src"):
            value = (tag.get(attr) or "").strip()
            if value:
                _push(value)

    for tag in soup.find_all(attrs={"data-video": True}):
        value = (tag.get("data-video") or "").strip()
        if value:
            _push(value)

    attr_patterns = [
        r'''["'](?:file|src|video|stream|url|hls|playlist)["']\s*:\s*["']([^"']+)["']''',
        r"""(?:file|src|video|stream|url|hls|playlist)\s*=\s*["']([^"']+)["']""",
    ]

    for pattern in attr_patterns:
        for match in re.findall(pattern, html_text, flags=re.IGNORECASE):
            _push(match)

    return candidates


def _extract_byse_code(url: str) -> str:
    try:
        path_parts = [part for part in urlparse((url or "").strip()).path.split("/") if part]
    except Exception:
        return ""

    for index, segment in enumerate(path_parts[:-1]):
        if segment.lower() in {"e", "d", "download", "dwn"}:
            return path_parts[index + 1].strip()
    return ""


def _pick_best_byse_source_url(sources: list[dict]) -> str:
    best_url = ""
    best_score = (-1, -1)

    for source in sources:
        if not isinstance(source, dict):
            continue

        url = str(source.get("url") or "").strip()
        if not url:
            continue

        score = list(_direct_video_score(url))
        mime_type = str(source.get("mime_type") or "").lower()
        if "mpegurl" in mime_type:
            score[0] += 200
        elif "mp4" in mime_type:
            score[0] += 120

        try:
            height = int(source.get("height") or 0)
        except Exception:
            height = 0
        score[0] += height

        label = str(source.get("label") or source.get("quality") or "").lower()
        if "1080" in label:
            score[0] += 30
        elif "720" in label:
            score[0] += 20

        score_tuple = (score[0], -score[1])
        if score_tuple > best_score:
            best_score = score_tuple
            best_url = url

    return best_url


def _decrypt_byse_playback_sources(playback_payload: dict) -> list[dict]:
    if not isinstance(playback_payload, dict) or AES is None:
        return []

    key_parts = playback_payload.get("key_parts") or []
    iv = str(playback_payload.get("iv") or "").strip()
    payload = str(playback_payload.get("payload") or "").strip()
    if not key_parts or not iv or not payload:
        return []

    try:
        key = b"".join(_base64_url_decode(str(part)) for part in key_parts)
        nonce = _base64_url_decode(iv)
        encrypted = _base64_url_decode(payload)
        if len(encrypted) < 17:
            return []

        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
        plaintext = cipher.decrypt_and_verify(encrypted[:-16], encrypted[-16:])
        decoded = json.loads(plaintext.decode("utf-8"))
    except Exception:
        return []

    sources = decoded.get("sources") if isinstance(decoded, dict) else []
    return [item for item in sources if isinstance(item, dict)]


def _extract_iframe_sources(html_text: str, base_url: str = "") -> list[str]:
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    found = []
    seen = set()

    for iframe in soup.find_all("iframe"):
        src = _decode_possible_escaped_url(iframe.get("src", ""))
        if not src:
            continue
        src = urljoin(base_url, src) if base_url else _absolute_url(src)
        if not src or src in seen:
            continue
        seen.add(src)
        found.append(src)

    return found


def _extract_js_redirect_candidates(html_text: str, base_url: str = "") -> list[str]:
    if not html_text:
        return []

    found = []
    seen = set()
    patterns = [
        r"""location(?:\.href)?\s*=\s*['"]([^'"]+)['"]""",
        r"""location\.replace\(\s*['"]([^'"]+)['"]\s*\)""",
        r"""window\.open\(\s*['"]([^'"]+)['"]""",
    ]

    for pattern in patterns:
        for match in re.findall(pattern, html_text, flags=re.IGNORECASE):
            candidate = _decode_possible_escaped_url(match)
            if not candidate:
                continue
            candidate = urljoin(base_url, candidate) if base_url else _absolute_url(candidate)
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            found.append(candidate)

    return found


def _extract_player_links(html_text: str) -> dict:
    player_candidates: dict[str, dict] = {}
    downloads: dict[str, dict] = {}

    for vid_id, server in re.findall(r"C_Video\('(\d+)','([\w-]+)'\)", html_text):
        normalized = _normalize_server_name(server)
        if normalized != "streamtape":
            continue
        if normalized in player_candidates:
            continue
        player_candidates[normalized] = {
            "server": normalized,
            "source_server": server,
            "video_id": vid_id,
            "embed_url": f"{BASE_URL}/e/getembed.php?sv={server}&id={vid_id}",
        }

    preferred_player_url = ""
    candidate = player_candidates.get("streamtape")
    if candidate:
        preferred_player_url = candidate["embed_url"]

    soup = BeautifulSoup(html_text, "html.parser")
    for link in soup.select("#baixar_menu a[href]"):
        href = _absolute_url(link.get("href", ""))
        if not href:
            continue

        match = re.search(r"[?&]sv=([\w-]+)", href, flags=re.IGNORECASE)
        if match:
            normalized = _normalize_server_name(match.group(1))
        else:
            text = link.get_text(" ", strip=True).lower()
            if "streamtape" in text:
                normalized = "streamtape"
            else:
                continue

        if normalized != "streamtape":
            continue
        downloads[normalized] = {
            "label": _server_label(normalized),
            "url": href,
        }

    token_match = re.search(r"token=([A-Za-z0-9]+)", html_text)
    token = token_match.group(1) if token_match else ""
    if token:
        candidate = player_candidates.get("streamtape")
        if candidate and "streamtape" not in downloads:
            downloads["streamtape"] = {
                "label": _server_label("streamtape"),
                "url": (
                    f"{BASE_URL}/e/redirect.php"
                    f"?sv={candidate['source_server']}&id={candidate['video_id']}&token={token}"
                ),
            }

    return {
        "player_url": preferred_player_url,
        "downloads": downloads,
    }


def _extract_series_embed_sources(html_text: str, base_url: str = "") -> dict[str, str]:
    targets: dict[str, str] = {}
    if not html_text:
        return targets

    for raw_url in re.findall(r"""url:\s*['"]([^'"]*embed\.php[^'"]*)['"]""", html_text, flags=re.IGNORECASE):
        resolved = urljoin(base_url or BASE_URL, _decode_possible_escaped_url(raw_url))
        parsed = urlparse(resolved)
        audio = unquote_plus((parse_qs(parsed.query).get("audio") or [""])[0]).strip().lower()
        if audio and audio not in targets:
            targets[audio] = resolved

    return targets


def _preferred_audio_key(embed_sources: dict[str, str]) -> str:
    for key in ("dublado", "legendado"):
        if key in embed_sources:
            return key
    return next(iter(embed_sources), "")


def _normalize_audio_choice(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"dub", "dublado", "br", "pt"}:
        return "dublado"
    if normalized in {"leg", "legendado", "en", "us"}:
        return "legendado"
    return ""


def _extract_embed_seasons(embed_html_text: str) -> list[int]:
    if not embed_html_text:
        return []

    soup = BeautifulSoup(embed_html_text, "html.parser")
    seasons = []
    seen = set()

    for node in soup.select("[data-open-sub]"):
        target = str(node.get("data-open-sub") or "").strip()
        match = re.search(r"epSub(\d+)", target, flags=re.IGNORECASE)
        if not match:
            match = re.search(r"(\d+)", node.get_text(" ", strip=True))
        if not match:
            continue

        number = int(match.group(1))
        if number not in seen:
            seen.add(number)
            seasons.append(number)

    return sorted(seasons)


def _extract_embed_episodes(embed_html_text: str) -> list[dict]:
    if not embed_html_text:
        return []

    soup = BeautifulSoup(embed_html_text, "html.parser")
    episodes = []
    seen = set()

    for node in soup.select("[data-ep][data-season]"):
        season = str(node.get("data-season") or "").strip()
        episode = str(node.get("data-ep") or "").strip()
        if not season or not episode:
            continue

        key = (season, episode)
        if key in seen:
            continue
        seen.add(key)

        downloads = {}
        player_url = ""
        for server in PLAYER_SERVER_ORDER:
            source_url = _decode_possible_escaped_url(node.get(f"data-url-{server}", ""))
            download_url = _decode_possible_escaped_url(node.get(f"data-dl-{server}", ""))
            if source_url and not player_url:
                player_url = source_url
            if source_url or download_url:
                downloads[server] = {
                    "label": _server_label(server),
                    "url": download_url or source_url,
                }

        try:
            label = f"Episódio {int(episode):02d}"
        except Exception:
            label = f"Episódio {episode}"

        episodes.append({
            "url": player_url,
            "label": label,
            "episode": episode,
            "slug": f"s{season}e{episode}",
            "season": int(season),
            "player_links": {
                "player_url": player_url,
                "downloads": downloads,
            },
        })

    def _sort_key(item: dict) -> tuple[int, int]:
        try:
            return int(item.get("season") or 0), int(item.get("episode") or 0)
        except Exception:
            return 0, 0

    episodes.sort(key=_sort_key)
    return episodes


# ── HTTP ────────────────────────────────────────────────────────────────────

async def _request_page(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    data: dict | None = None,
    referer: str = "",
    extra_headers: dict | None = None,
) -> tuple[str, str]:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    if extra_headers:
        for key, value in extra_headers.items():
            if value:
                headers[key] = value

    last_error: Exception | None = None
    proxy_chain = _request_proxy_chain(url)

    for attempt in range(2):
        for request_proxy in proxy_chain:
            try:
                async with _SEM:
                    session = await _get_session()
                    async with session.request(
                        method.upper(),
                        url,
                        params=params,
                        data=data,
                        headers=headers,
                        allow_redirects=True,
                        timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                        proxy=request_proxy,
                    ) as resp:
                        resp.raise_for_status()
                        return str(resp.url), await resp.text(
                            encoding="utf-8",
                            errors="replace",
                        )
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if len(proxy_chain) > 1:
                    route_name = "proxy" if request_proxy else "direct"
                    LOGGER.debug(
                        "Falha na rota %s para %s %s: %r",
                        route_name,
                        method.upper(),
                        url,
                        exc,
                    )
                continue

        if attempt == 0:
            await asyncio.sleep(0.35)
            continue
        break

    if last_error is not None:
        raise last_error
    raise RuntimeError("Falha inesperada ao buscar pagina do catalogo.")


async def _get(
    url: str,
    params: dict | None = None,
    referer: str = "",
    extra_headers: dict | None = None,
) -> str:
    _, html_text = await _request_page(
        "GET",
        url,
        params=params,
        referer=referer,
        extra_headers=extra_headers,
    )
    return html_text


async def _post(
    url: str,
    data: dict | None = None,
    referer: str = "",
    extra_headers: dict | None = None,
) -> str:
    _, html_text = await _request_page(
        "POST",
        url,
        data=data,
        referer=referer,
        extra_headers=extra_headers,
    )
    return html_text


async def _fetch_url_and_html(
    url: str,
    referer: str = "",
    extra_headers: dict | None = None,
) -> tuple[str, str]:
    return await _request_page("GET", url, referer=referer, extra_headers=extra_headers)


async def _resolve_byse_direct_url(url: str) -> str:
    code = _extract_byse_code(url)
    if not code or AES is None:
        return ""

    parsed = urlparse((url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return ""

    origin = f"{parsed.scheme}://{parsed.netloc}"
    playback_api = f"{origin}/api/videos/{code}/embed/playback"
    headers = {
        "Accept": "application/json",
        "Origin": origin,
        "X-Embed-Origin": origin,
        "X-Embed-Referer": url,
        "X-Embed-Parent": url,
    }

    try:
        body = await _get(playback_api, referer=url, extra_headers=headers)
        decoded = json.loads(body)
    except Exception:
        return ""

    playback_payload = decoded.get("playback") if isinstance(decoded, dict) else {}
    sources = _decrypt_byse_playback_sources(playback_payload)
    return _pick_best_byse_source_url(sources)


async def _resolve_player_target(url: str, referer: str = "", depth: int = 0, visited: set[str] | None = None) -> str:
    normalized_url = _absolute_url(_decode_possible_escaped_url(url))
    if not normalized_url or depth > 3:
        return normalized_url

    if visited is None:
        visited = set()
    if normalized_url in visited:
        return normalized_url
    visited.add(normalized_url)

    if _is_direct_video_url(normalized_url):
        return normalized_url

    direct_from_byse = await _resolve_byse_direct_url(normalized_url)
    if direct_from_byse:
        return direct_from_byse

    try:
        final_url, html_text = await _fetch_url_and_html(normalized_url, referer=referer or BASE_URL)
    except Exception:
        return normalized_url

    if final_url and _is_direct_video_url(final_url):
        return final_url

    base_for_children = final_url or normalized_url
    direct_urls = _extract_direct_video_urls(html_text, base_url=base_for_children)
    if direct_urls:
        return _pick_best_direct_video_url(direct_urls)

    if final_url:
        direct_from_byse = await _resolve_byse_direct_url(final_url)
        if direct_from_byse:
            return direct_from_byse

    for candidate in _extract_iframe_sources(html_text, base_url=base_for_children):
        resolved = await _resolve_player_target(
            candidate,
            referer=base_for_children,
            depth=depth + 1,
            visited=visited,
        )
        if resolved and not _is_source_site_url(resolved) and not _is_unwanted_external_target(resolved):
            return resolved

    for candidate in _extract_js_redirect_candidates(html_text, base_url=base_for_children):
        resolved = await _resolve_player_target(
            candidate,
            referer=base_for_children,
            depth=depth + 1,
            visited=visited,
        )
        if resolved and not _is_source_site_url(resolved) and not _is_unwanted_external_target(resolved):
            return resolved

    if final_url and not _is_source_site_url(final_url) and not _is_unwanted_external_target(final_url):
        return final_url

    return final_url or normalized_url


async def resolve_player_links(player_links: dict | None, referer: str = "") -> dict:
    if not isinstance(player_links, dict):
        return {"player_url": None, "downloads": {}}

    downloads = player_links.get("downloads") or {}
    resolved_downloads = {}

    for server in PLAYER_SERVER_ORDER:
        item = downloads.get(server)
        if not item:
            continue

        raw_url = str(item.get("url") or "").strip()
        if not raw_url:
            continue

        resolved_url = await _resolve_player_target(raw_url, referer=referer or raw_url)
        resolved_downloads[server] = {
            "label": str(item.get("label") or _server_label(server)).strip(),
            "url": resolved_url or raw_url,
        }

    raw_player_url = str(player_links.get("player_url") or "").strip()
    resolved_player_url = (
        await _resolve_player_target(raw_player_url, referer=referer or raw_player_url)
        if raw_player_url
        else ""
    )

    if not resolved_player_url or _is_source_site_url(resolved_player_url):
        for server in PLAYER_SERVER_ORDER:
            item = resolved_downloads.get(server)
            if item and item.get("url"):
                resolved_player_url = item["url"]
                break

    return {
        "player_url": resolved_player_url or raw_player_url,
        "downloads": resolved_downloads,
    }


# ── Helpers de parsing ───────────────────────────────────────────────────────

def _parse_items(soup: BeautifulSoup) -> list[dict]:
    """Extrai cartões de resultado (filmes/séries) de uma página."""
    results = []
    for card in soup.select(".similarMovies a.block"):
        href = _absolute_url(card.get("href", ""))
        if not href:
            continue

        img = card.find("img")
        title_el = card.select_one(".info h3")
        year_el = card.select_one(".info p")
        badges = [div.get_text(" ", strip=True) for div in card.select(".top div")]

        title = ""
        if title_el:
            title = title_el.get_text(" ", strip=True)
        elif img:
            title = re.sub(r"^Assistir\s+", "", img.get("alt", ""), flags=re.IGNORECASE).strip()
        title = title or "Sem tÃ­tulo"

        quality = badges[0].strip() if badges else "HD"
        audio_raw = badges[1].strip() if len(badges) > 1 else ""
        is_dubbed = "DUB" in audio_raw.upper() or "DUBLADO" in audio_raw.upper()
        image = _absolute_url(
            (img.get("src", "") if img else "")
            or (img.get("data-src", "") if img else "")
        )

        results.append({
            "id": href.rstrip("/").split("/")[-1],
            "url": href,
            "title": title,
            "year": year_el.get_text(strip=True) if year_el else "",
            "duration": "",
            "is_dubbed": is_dubbed,
            "audio": "Dublado" if is_dubbed else "Legendado",
            "quality": quality,
            "image": image,
        })
    for card in soup.select("#collview"):
        a_tag = card.find("a", href=True)
        if not a_tag:
            continue

        href = _absolute_url(a_tag["href"].strip())

        # título
        caption = a_tag.find(class_="caption")
        title = ""
        if caption:
            # pega só o texto direto, ignora as divs internas
            for child in caption.children:
                if hasattr(child, "get_text"):
                    if child.name not in ("div",):
                        title += child.get_text(" ", strip=True)
                else:
                    title += str(child).strip()
        title = title.strip() or "Sem título"

        # ano e duração
        year_div = a_tag.select_one(".caption .y")
        dur_div  = a_tag.select_one(".caption .t")
        year = year_div.get_text(strip=True) if year_div else ""
        duration = dur_div.get_text(strip=True) if dur_div else ""

        # áudio (DUB / LEG)
        audio_div = card.select_one(".capa-audio")
        audio_raw = audio_div.get_text(strip=True).replace("⭐", "") if audio_div else ""
        is_dubbed = "DUB" in audio_raw.upper()

        # qualidade (HD / CAM)
        quali_div = card.select_one(".capa-quali")
        quality = quali_div.get_text(strip=True) if quali_div else "HD"

        # imagem
        img_div = a_tag.select_one(".vb_image_container[data-background-src]")
        image = _absolute_url(img_div["data-background-src"].strip() if img_div else "")

        # slug / id a partir da URL
        slug = href.rstrip("/").split("/")[-1]

        results.append({
            "id": slug,
            "url": href,
            "title": title,
            "year": year,
            "duration": duration,
            "is_dubbed": is_dubbed,
            "audio": "Dublado" if is_dubbed else "Legendado",
            "quality": quality,
            "image": image,
        })

    return results


def _is_series(url: str) -> bool:
    return "series" in url or "-u1-" in url or "-u2-" in url or "-u3-" in url or "/assistir-" in url and "temporada" not in url


# ── API pública ─────────────────────────────────────────────────────────────

async def search_content(query: str) -> list[dict]:
    """Busca filmes e series no catalogo."""
    query = str(query or "").strip()
    if not query:
        return []

    key = f"v2:{query.lower()}"
    cached = _cached(_SEARCH_CACHE, key, CACHE_TTL_SEARCH)
    if cached is not None:
        return cached

    raw_items = await _collect_search_attempts(query)
    collapsed_items = _collapse_search_results(query, raw_items)
    normalized_query = _normalize_search_text(query)

    if len(collapsed_items) <= 1 and "dublado" not in normalized_query and "legendado" not in normalized_query:
        merged_items = {_search_item_unique_key(item): item for item in raw_items if _search_item_unique_key(item)}
        extra_batches = await asyncio.gather(
            _collect_search_attempts(f"{query} dublado"),
            _collect_search_attempts(f"{query} legendado"),
            return_exceptions=True,
        )
        for extra_items in extra_batches:
            if isinstance(extra_items, Exception):
                continue
            for item in extra_items:
                unique_key = _search_item_unique_key(item)
                if unique_key and unique_key not in merged_items:
                    merged_items[unique_key] = item
        collapsed_items = _collapse_search_results(query, list(merged_items.values()))

    items = _rank_search_results(query, collapsed_items)

    _set_cache(_SEARCH_CACHE, key, items)
    return items


async def get_content_details(url: str) -> dict:
    """Retorna detalhes de uma pagina de serie/filme do catalogo."""
    cached = _cached(_DETAIL_CACHE, url, CACHE_TTL_DETAIL)
    if cached is not None:
        return cached

    html_text = await _get(url)
    soup = BeautifulSoup(html_text, "html.parser")
    embed_sources = _extract_series_embed_sources(html_text, base_url=url)

    # Título
    title_el = soup.select_one(".titulo, h1.ipsType_pageTitle")
    title = title_el.get_text(" ", strip=True) if title_el else "Sem título"
    # remove "1×1" tipo prefixo de episódio
    title = re.sub(r"^\d+[×x]\d+\s*", "", title).strip()

    # Sinopse
    sinopse_el = soup.select_one(".sinopse")
    description = sinopse_el.get_text(" ", strip=True) if sinopse_el else ""
    if not description:
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            description = meta_desc.get("content", "").strip()

    # Infos (ano, duração, imdb)
    year = ""
    duration = ""
    rating = ""
    info_spans = soup.select(".infos span")
    for sp in info_spans:
        txt = sp.get_text(strip=True)
        if re.match(r"^\d{4}$", txt):
            year = txt
        elif "min" in txt.lower():
            duration = txt
        elif "/" in txt and re.search(r"\d", txt):
            rating = txt

    imdb_id = ""
    imdb_url = ""
    imdb_link = soup.select_one(".infos a[href*='imdb.com/title/'], a[href*='imdb.com/title/']")
    if imdb_link:
        imdb_url = str(imdb_link.get("href") or "").strip()
        imdb_match = re.search(r"imdb\.com/title/(tt\d+)", imdb_url, flags=re.IGNORECASE)
        if imdb_match:
            imdb_id = imdb_match.group(1)
        imdb_link_rating = imdb_link.get_text(" ", strip=True)
        if not rating and "/" in imdb_link_rating and re.search(r"\d", imdb_link_rating):
            rating = imdb_link_rating

    # Elenco / diretor
    cast = ""
    director = ""
    for sp in soup.select(".extrainfo span"):
        txt = sp.get_text(" ", strip=True)
        if "Elenco" in txt or "Cast" in txt:
            cast = txt.replace("Elenco:", "").replace("Elenco :", "").strip()
        elif "Diretor" in txt or "Director" in txt:
            director = txt.replace("Diretor:", "").replace("Diretor :", "").strip()

    # Gêneros
    genres = [a.get_text(strip=True) for a in soup.select("a.generos")]

    # Imagem principal (capa)
    image = ""
    og_img = soup.find("meta", property="og:image")
    if og_img:
        image = og_img.get("content", "").strip()
    if not image:
        img_div = soup.select_one(".vb_image_container[data-background-src]")
        if img_div:
            image = img_div["data-background-src"].strip()

    # Audio
    audio_el = soup.select_one(".capa-audio")
    is_dubbed = False
    if audio_el:
        is_dubbed = "DUB" in audio_el.get_text(strip=True).upper()
    elif "dublado" in embed_sources:
        is_dubbed = True

    audio_options = [key for key in ("dublado", "legendado") if key in embed_sources]
    if not audio_options:
        audio_options = ["dublado" if is_dubbed else "legendado"]
    default_audio = audio_options[0]

    # Tipo (série ou filme)
    content_type = "series" if soup.select_one("#listagem") or "/series/" in (url or "").lower() else "movie"

    # URL canônica
    canonical = ""
    canon_el = soup.find("link", rel="canonical")
    if canon_el:
        canonical = canon_el.get("href", "").strip()

    detail = {
        "url": url,
        "canonical": canonical or url,
        "title": title,
        "description": description,
        "year": year,
        "duration": duration,
        "rating": rating,
        "cast": cast,
        "director": director,
        "genres": genres,
        "image": image,
        "poster": image,
        "is_dubbed": is_dubbed,
        "audio": "Dublado" if default_audio == "dublado" else "Legendado",
        "audio_options": audio_options,
        "default_audio": default_audio,
        "type": content_type,
        "imdb_id": imdb_id,
        "imdb_url": imdb_url,
    }

    try:
        enriched = await enrich_media_metadata(
            title=detail["title"],
            year=detail["year"],
            content_type=content_type,
            source_imdb_id=detail["imdb_id"],
            source_rating=detail["rating"],
            source_image=detail["image"],
        )
    except Exception:
        enriched = None

    if isinstance(enriched, dict):
        detail["title"] = str(enriched.get("title") or detail["title"]).strip() or detail["title"]
        detail["description"] = str(enriched.get("description") or detail["description"]).strip() or detail["description"]
        detail["year"] = str(enriched.get("year") or detail["year"]).strip() or detail["year"]
        detail["rating"] = str(enriched.get("rating") or detail["rating"]).strip() or detail["rating"]
        enriched_genres = enriched.get("genres") or []
        if isinstance(enriched_genres, list) and enriched_genres:
            detail["genres"] = enriched_genres
        enriched_image = str(enriched.get("image") or enriched.get("poster") or "").strip()
        if enriched_image:
            detail["image"] = enriched_image
        enriched_poster = str(enriched.get("poster") or enriched_image or "").strip()
        if enriched_poster:
            detail["poster"] = enriched_poster
        enriched_backdrop = str(enriched.get("backdrop") or "").strip()
        if enriched_backdrop:
            detail["backdrop"] = enriched_backdrop
        detail["imdb_id"] = str(enriched.get("imdb_id") or detail.get("imdb_id") or "").strip()
        detail["imdb_url"] = str(enriched.get("imdb_url") or detail.get("imdb_url") or "").strip()

    _set_cache(_DETAIL_CACHE, url, detail)
    return detail


async def get_episodes(series_url: str, preferred_audio: str = "") -> list[dict]:
    """Retorna lista de episódios de uma série."""
    normalized_audio = _normalize_audio_choice(preferred_audio)
    cache_key = f"{series_url}|audio={normalized_audio or 'default'}"

    cached = _cached(_EPISODES_CACHE, cache_key, CACHE_TTL_EPISODES)
    if cached is not None:
        cached_copy = _clone_episodes(cached)
        if not _episodes_payload_is_volatile(cached_copy):
            return cached_copy
        _delete_cache(_EPISODES_CACHE, cache_key)

    html_text = await _get(series_url)
    soup = BeautifulSoup(html_text, "html.parser")

    episodes = []
    for li in soup.select("#listagem li"):
        a_tag = li.find("a", href=True)
        if not a_tag:
            continue

        ep_url = a_tag["href"].strip()
        if not ep_url.startswith("http"):
            ep_url = BASE_URL + ep_url

        label = a_tag.get_text(strip=True)
        m = re.search(r"[Ee]pis[oó]dio\s*(\d+)|(\d+)", label)
        ep_num = m.group(1) or m.group(2) if m else label

        episodes.append({
            "url": ep_url,
            "label": label,
            "episode": ep_num,
            "slug": ep_url.rstrip("/").split("/")[-1],
        })

    def _ep_sort(e):
        try:
            return int(e["episode"])
        except Exception:
            return 0

    episodes.sort(key=_ep_sort)

    if not episodes:
        embed_sources = _extract_series_embed_sources(html_text, base_url=series_url)
        audio_key = normalized_audio if normalized_audio in embed_sources else _preferred_audio_key(embed_sources)
        embed_url = embed_sources.get(audio_key, "")
        if embed_url:
            embed_html = await _get(embed_url, referer=series_url)
            episodes = _extract_embed_episodes(embed_html)

    if _episodes_payload_is_volatile(episodes):
        _delete_cache(_EPISODES_CACHE, cache_key)
        return _clone_episodes(episodes)

    _set_cache(_EPISODES_CACHE, cache_key, _clone_episodes(episodes))
    return _clone_episodes(episodes)


async def get_seasons(series_url: str, preferred_audio: str = "") -> list[int]:
    """Retorna temporadas disponíveis."""
    html_text = await _get(series_url)
    soup = BeautifulSoup(html_text, "html.parser")
    normalized_audio = _normalize_audio_choice(preferred_audio)

    seasons = []
    seen = set()

    for anchor in soup.select("a[href*='temporada=']"):
        href = _absolute_url(anchor.get("href", ""))
        match = re.search(r"[?&]temporada=(\d+)", href)
        if not match:
            continue

        number = int(match.group(1))
        if number not in seen:
            seen.add(number)
            seasons.append(number)

    # Script com load(n) de temporadas
    for script in soup.find_all("script"):
        txt = script.string or ""
        found = re.findall(r"load\((\d+)\)", txt)
        for s in found:
            n = int(s)
            if n not in seen:
                seen.add(n)
                seasons.append(n)

    if len(seasons) <= 1:
        embed_sources = _extract_series_embed_sources(html_text, base_url=series_url)
        audio_key = normalized_audio if normalized_audio in embed_sources else _preferred_audio_key(embed_sources)
        embed_url = embed_sources.get(audio_key, "")
        if embed_url:
            embed_html = await _get(embed_url, referer=series_url)
            embed_seasons = _extract_embed_seasons(embed_html)
            if embed_seasons:
                seasons = embed_seasons

    if not seasons:
        seasons = [1]

    return sorted(seasons)


async def get_season_episodes(series_url: str, season: int, preferred_audio: str = "") -> list[dict]:
    """Retorna episódios de uma temporada específica."""
    url_with_season = f"{series_url}?temporada={season}"
    season_episodes = await get_episodes(url_with_season, preferred_audio=preferred_audio)
    filtered = [ep for ep in season_episodes if int(ep.get("season") or season) == season]
    if filtered:
        return filtered

    all_episodes = await get_episodes(series_url, preferred_audio=preferred_audio)
    filtered = [ep for ep in all_episodes if int(ep.get("season") or 0) == season]
    return filtered


async def get_player_links(episode_url: str, preferred_audio: str = "") -> dict:
    """Retorna player embed e links diretos dos hosts disponiveis."""
    try:
        html_text = await _get(episode_url)
        normalized_audio = _normalize_audio_choice(preferred_audio)
        if normalized_audio:
            embed_sources = _extract_series_embed_sources(html_text, base_url=episode_url)
            embed_url = embed_sources.get(normalized_audio, "")
            if embed_url:
                html_text = await _get(embed_url, referer=episode_url)
        extracted = _extract_player_links(html_text)
        return await resolve_player_links(extracted, referer=episode_url)
    except Exception as e:
        print(f"[catalog_client] Erro ao pegar links do player de {episode_url}: {e}")
    return {"player_url": None, "downloads": {}}


async def get_player_url(episode_url: str, preferred_audio: str = "") -> str | None:
    """Retorna o link final do player principal fora do site origem."""
    links = await get_player_links(episode_url, preferred_audio=preferred_audio)
    return links.get("player_url")

    try:
        html_text = await _get(episode_url)
        soup = BeautifulSoup(html_text, "html.parser")

        # Tenta pegar o player embed da função C_Video
        scripts = soup.find_all("script")
        for script in scripts:
            txt = script.string or ""
            # Padrão: C_Video('856','filemoon')
            m = re.search(r"C_Video\('(\d+)','(\w+)'\)", txt)
            if m:
                vid_id = m.group(1)
                sv = m.group(2)
                return f"{BASE_URL}/e/getembed.php?sv={sv}&id={vid_id}"

        # Fallback: tenta pegar do onclick direto
        for div in soup.select(".item[onclick]"):
            onclick = div.get("onclick", "")
            m = re.search(r"C_Video\('(\d+)','(\w+)'\)", onclick)
            if m:
                vid_id = m.group(1)
                sv = m.group(2)
                return f"{BASE_URL}/e/getembed.php?sv={sv}&id={vid_id}"

    except Exception as e:
        print(f"[catalog_client] Erro ao pegar player de {episode_url}: {e}")

    return None


async def get_recent_series(limit: int = 12) -> list[dict]:
    """Retorna séries recentemente atualizadas."""
    url = f"{BASE_URL}/assistir/series-online-online-3/"
    try:
        html_text = await _get(url)
        soup = BeautifulSoup(html_text, "html.parser")
        items = _parse_items(soup)
        return items[:limit]
    except Exception as e:
        print(f"[catalog_client] Erro get_recent_series: {e}")
        return []


async def get_recent_movies(limit: int = 12) -> list[dict]:
    """Retorna filmes recentemente adicionados."""
    url = f"{BASE_URL}/assistir/filmes-online-online-2/"
    try:
        html_text = await _get(url)
        soup = BeautifulSoup(html_text, "html.parser")
        items = _parse_items(soup)
        return items[:limit]
    except Exception as e:
        print(f"[catalog_client] Erro get_recent_movies: {e}")
        return []


def preload_popular_cache():
    """Pré-carrega cache em background (opcional)."""
    pass
