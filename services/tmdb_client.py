from __future__ import annotations

import asyncio
import re
from difflib import SequenceMatcher
from urllib.parse import urlparse

import httpx

from config import TMDB_ACCESS_TOKEN, TMDB_API_KEY
from core.http_client import get_http_client

TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"
IMDB_TITLE_BASE = "https://www.imdb.com/title"
_CACHE: dict[str, dict | None] = {}
_CACHE_LOCK = asyncio.Lock()


def _enabled() -> bool:
    return bool(TMDB_ACCESS_TOKEN or TMDB_API_KEY)


def _auth_headers() -> dict[str, str]:
    if TMDB_ACCESS_TOKEN:
        return {
            "Authorization": f"Bearer {TMDB_ACCESS_TOKEN}",
            "Accept": "application/json",
        }
    return {"Accept": "application/json"}


def _auth_params() -> dict[str, str]:
    if TMDB_API_KEY and not TMDB_ACCESS_TOKEN:
        return {"api_key": TMDB_API_KEY}
    return {}


def _normalize_title(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_rating(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    match = re.search(r"([0-9]+(?:[.,][0-9]+)?)", raw)
    if not match:
        return ""

    number = match.group(1).replace(",", ".")
    try:
        numeric = float(number)
    except ValueError:
        return ""
    return f"{numeric:.1f}/10"


def _score_candidate(query_title: str, query_year: str, item: dict, content_type: str) -> float:
    if not isinstance(item, dict):
        return 0.0

    if content_type == "series":
        candidate_title = str(item.get("name") or item.get("original_name") or "").strip()
        candidate_year = str(item.get("first_air_date") or "")[:4]
    else:
        candidate_title = str(item.get("title") or item.get("original_title") or "").strip()
        candidate_year = str(item.get("release_date") or "")[:4]

    normalized_query = _normalize_title(query_title)
    normalized_candidate = _normalize_title(candidate_title)
    if not normalized_query or not normalized_candidate:
        return 0.0

    score = SequenceMatcher(None, normalized_query, normalized_candidate).ratio() * 100
    if normalized_candidate == normalized_query:
        score += 100
    elif normalized_candidate.startswith(normalized_query):
        score += 35
    elif normalized_query in normalized_candidate:
        score += 20

    if query_year and candidate_year:
        if query_year == candidate_year:
            score += 40
        else:
            score -= 15

    vote_count = int(item.get("vote_count") or 0)
    popularity = float(item.get("popularity") or 0.0)
    score += min(vote_count, 5000) / 250
    score += min(popularity, 200) / 25
    return score


def _image_url(path: str, *, kind: str = "poster") -> str:
    raw = str(path or "").strip()
    if not raw:
        return ""
    size = "original" if kind == "backdrop" else "w780"
    return f"{TMDB_IMAGE_BASE}/{size}{raw}"


def _normalize_tmdb_image_url(url: str, *, kind: str = "poster") -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    try:
        parsed = urlparse(raw)
    except Exception:
        return raw

    if "image.tmdb.org" not in parsed.netloc.lower():
        return raw

    match = re.search(r"^/t/p/[^/]+(/.+)$", parsed.path)
    if not match:
        return raw

    size = "original" if kind == "backdrop" else "w780"
    return f"{parsed.scheme or 'https'}://{parsed.netloc}/t/p/{size}{match.group(1)}"


def _build_imdb_url(imdb_id: str) -> str:
    value = str(imdb_id or "").strip()
    if not value:
        return ""
    return f"{IMDB_TITLE_BASE}/{value}/"


def _extract_imdb_rating_from_html(html_text: str) -> str:
    text = str(html_text or "")
    if not text:
        return ""

    patterns = [
        r'"ratingValue"\s*:\s*"([0-9]+(?:\.[0-9]+)?)"',
        r'"aggregateRating".+?"ratingValue"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
        r'"heroRating".+?"aggregateRating".+?"ratingValue"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return _normalize_rating(match.group(1))
    return ""


async def _get_json(endpoint: str, *, params: dict | None = None) -> dict:
    client = await get_http_client()
    request_params = {
        **_auth_params(),
        **(params or {}),
    }
    response = await client.get(
        f"{TMDB_API_BASE}{endpoint}",
        params=request_params,
        headers=_auth_headers(),
    )
    response.raise_for_status()
    return response.json()


async def _fetch_imdb_rating(imdb_id: str) -> str:
    value = str(imdb_id or "").strip()
    if not value:
        return ""

    client = await get_http_client()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    timeout = httpx.Timeout(8.0, connect=4.0, read=8.0, write=8.0, pool=8.0)

    for path in ("", "reference/", "ratings/"):
        try:
            response = await client.get(
                f"{IMDB_TITLE_BASE}/{value}/{path}",
                headers=headers,
                timeout=timeout,
            )
        except Exception:
            continue

        if response.status_code != 200:
            continue

        rating = _extract_imdb_rating_from_html(response.text)
        if rating:
            return rating
    return ""


async def _search_tmdb(title: str, year: str, content_type: str) -> dict | None:
    if not _enabled():
        return None

    endpoint = "/search/tv" if content_type == "series" else "/search/movie"
    params = {
        "query": title,
        "include_adult": "false",
        "language": "pt-BR",
        "page": 1,
    }
    if year:
        if content_type == "series":
            params["first_air_date_year"] = year
        else:
            params["primary_release_year"] = year

    payload = await _get_json(endpoint, params=params)
    results = payload.get("results") or []
    if not isinstance(results, list) or not results:
        return None

    best = max(results[:8], key=lambda item: _score_candidate(title, year, item, content_type))
    return best if _score_candidate(title, year, best, content_type) >= 55 else None


async def _fetch_tmdb_detail(tmdb_id: int, content_type: str) -> dict | None:
    endpoint = f"/tv/{tmdb_id}" if content_type == "series" else f"/movie/{tmdb_id}"
    try:
        return await _get_json(
            endpoint,
            params={
                "language": "pt-BR",
                "append_to_response": "external_ids",
            },
        )
    except Exception:
        return None


async def _build_fallback_result(
    *,
    source_imdb_id: str,
    source_rating: str,
    source_image: str,
) -> dict | None:
    imdb_id = str(source_imdb_id or "").strip()
    rating = _normalize_rating(source_rating)
    if not rating and imdb_id:
        rating = await _fetch_imdb_rating(imdb_id)

    poster = _normalize_tmdb_image_url(source_image, kind="poster")
    backdrop = _normalize_tmdb_image_url(source_image, kind="backdrop")

    result: dict[str, str] = {}
    if rating:
        result["rating"] = rating
    if poster:
        result["image"] = poster
        result["poster"] = poster
    if backdrop:
        result["backdrop"] = backdrop
    if imdb_id:
        result["imdb_id"] = imdb_id
        result["imdb_url"] = _build_imdb_url(imdb_id)
    return result or None


async def enrich_media_metadata(
    *,
    title: str,
    year: str,
    content_type: str,
    source_imdb_id: str = "",
    source_rating: str = "",
    source_image: str = "",
) -> dict | None:
    normalized_type = "series" if content_type == "series" else "movie"
    cache_key = (
        f"{normalized_type}|{_normalize_title(title)}|{year}|"
        f"{str(source_imdb_id or '').strip()}|{_normalize_tmdb_image_url(source_image, kind='poster')}"
    )

    async with _CACHE_LOCK:
        if cache_key in _CACHE:
            return _CACHE[cache_key]

    fallback = await _build_fallback_result(
        source_imdb_id=source_imdb_id,
        source_rating=source_rating,
        source_image=source_image,
    )

    if not _enabled():
        async with _CACHE_LOCK:
            _CACHE[cache_key] = fallback
        return fallback

    try:
        search_hit = await _search_tmdb(title, year, normalized_type)
        if not search_hit:
            result = fallback
        else:
            tmdb_id = int(search_hit.get("id") or 0)
            detail = await _fetch_tmdb_detail(tmdb_id, normalized_type) if tmdb_id else None
            if not detail:
                result = fallback
            else:
                localized_title = str(
                    detail.get("name")
                    or detail.get("title")
                    or search_hit.get("name")
                    or search_hit.get("title")
                    or title
                ).strip()
                localized_year = str(
                    (detail.get("first_air_date") or detail.get("release_date") or year or "")[:4]
                ).strip()
                genres = [
                    str(item.get("name") or "").strip()
                    for item in (detail.get("genres") or [])
                    if isinstance(item, dict) and str(item.get("name") or "").strip()
                ]
                tmdb_rating = _normalize_rating(
                    detail.get("vote_average") or search_hit.get("vote_average") or 0.0
                )
                imdb_id = str(
                    (detail.get("external_ids") or {}).get("imdb_id")
                    or source_imdb_id
                    or ""
                ).strip()
                rating = str((fallback or {}).get("rating") or "").strip()
                if not rating and imdb_id:
                    rating = await _fetch_imdb_rating(imdb_id)
                if not rating:
                    rating = tmdb_rating

                poster = _image_url(detail.get("poster_path"), kind="poster") or str(
                    (fallback or {}).get("poster") or ""
                ).strip()
                backdrop = _image_url(detail.get("backdrop_path"), kind="backdrop") or str(
                    (fallback or {}).get("backdrop") or ""
                ).strip()
                image = poster or backdrop or str((fallback or {}).get("image") or "").strip()

                result = {
                    "title": localized_title or title,
                    "description": str(detail.get("overview") or "").strip(),
                    "year": localized_year or year,
                    "rating": rating,
                    "tmdb_rating": tmdb_rating,
                    "genres": genres,
                    "image": image,
                    "poster": poster,
                    "backdrop": backdrop,
                    "imdb_id": imdb_id,
                    "imdb_url": _build_imdb_url(imdb_id),
                }
    except Exception:
        result = fallback

    async with _CACHE_LOCK:
        _CACHE[cache_key] = result
    return result
