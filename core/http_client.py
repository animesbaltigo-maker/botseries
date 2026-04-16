from __future__ import annotations

import asyncio

import httpx

from config import HTTP_TIMEOUT, SOURCE_SITE_BASE, UPSTREAM_PROXY_URL

_CLIENT: httpx.AsyncClient | None = None
_CLIENT_LOCK = asyncio.Lock()

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "Referer": f"{SOURCE_SITE_BASE}/",
    "Origin": SOURCE_SITE_BASE,
}

_TIMEOUT = httpx.Timeout(
    float(HTTP_TIMEOUT),
    connect=8.0,
    read=max(18.0, float(HTTP_TIMEOUT)),
    write=max(18.0, float(HTTP_TIMEOUT)),
    pool=40.0,
)

_LIMITS = httpx.Limits(
    max_connections=150,
    max_keepalive_connections=50,
)


async def get_http_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(
                headers=_HEADERS,
                follow_redirects=True,
                timeout=_TIMEOUT,
                limits=_LIMITS,
                http2=False,
                proxy=UPSTREAM_PROXY_URL or None,
                # Proxy routing is controlled explicitly through config so the
                # behavior stays deterministic across CMD/VPS environments.
                trust_env=False,
            )
    return _CLIENT


async def close_http_client() -> None:
    global _CLIENT
    async with _CLIENT_LOCK:
        if _CLIENT is not None:
            await _CLIENT.aclose()
            _CLIENT = None
