from __future__ import annotations

import argparse
import asyncio
import difflib
import html
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(ROOT))

from config import API_HASH, API_ID, BOT_USERNAME, DATA_DIR  # noqa: E402
from handlers.postanime import (  # noqa: E402
    _build_caption,
    _build_payload_item,
    _default_audio_key,
    _looks_like_series,
)
from services.catalog_client import get_content_details, get_seasons, search_content  # noqa: E402
from services.start_payloads import build_start_link, create_start_payload  # noqa: E402


TITLE_NOISE = {
    "series",
    "séries",
    "serie",
    "série",
    "brazil",
    "brasil",
    "dublado",
    "legendado",
    "dual",
    "audio",
    "áudio",
    "temporada",
    "episodio",
    "episódio",
    "assistir",
    "baixar",
    "download",
}


def _normalize(value: str) -> str:
    value = html.unescape(value or "").lower()
    value = re.sub(r"https?://\S+|t\.me/\S+|@\w+", " ", value)
    value = re.sub(r"\b(?:s|t)\d{1,2}\s*e\d{1,3}\b", " ", value, flags=re.I)
    value = re.sub(r"\b(?:ep|eps|episodio|episódio)\s*\d{1,3}(?:\s*/\s*\d{1,3})?\b", " ", value, flags=re.I)
    value = re.sub(r"\b(?:temp|temporada)\s*\d{1,2}\b", " ", value, flags=re.I)
    value = re.sub(r"[^\w\sÀ-ÿ'-]", " ", value)
    words = [word for word in value.split() if word not in TITLE_NOISE]
    return " ".join(words).strip()


def _candidate_queries(text: str) -> list[str]:
    text = html.unescape(text or "")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    candidates: list[str] = []

    for line in lines[:8]:
        cleaned = re.sub(r"^[^\wÀ-ÿ]+", "", line).strip()
        match = re.search(r"(?:t[íi]tulo|nome)\s*[:：]\s*(.+)", cleaned, flags=re.I)
        if match:
            cleaned = match.group(1).strip()
        normalized = _normalize(cleaned)
        if 3 <= len(normalized) <= 80:
            candidates.append(normalized)

    whole = _normalize(" ".join(lines[:6]))
    if 3 <= len(whole) <= 120:
        candidates.append(whole)

    unique = []
    seen = set()
    for item in candidates:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique[:4]


def _score(query: str, item: dict, detail: dict | None = None) -> float:
    title = str((detail or {}).get("title") or item.get("title") or "")
    return difflib.SequenceMatcher(None, _normalize(query), _normalize(title)).ratio()


async def _resolve_series(text: str, min_score: float) -> tuple[dict, dict, list[int], float, str] | None:
    for query in _candidate_queries(text):
        try:
            results = await search_content(query)
        except Exception:
            continue
        series = [item for item in results if isinstance(item, dict) and _looks_like_series(item)]
        for item in series[:5]:
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            try:
                detail = await asyncio.wait_for(get_content_details(url), timeout=20)
            except Exception:
                detail = {}
            score = _score(query, item, detail)
            if score < min_score:
                continue
            try:
                seasons = await asyncio.wait_for(get_seasons(url, preferred_audio=_default_audio_key(detail)), timeout=20)
            except Exception:
                seasons = [1]
            return item, detail, seasons or [1], score, query
    return None


def _button(deep_link: str):
    from telethon import Button

    return [[Button.url("▶️ Assistir no bot", deep_link)]]


async def _download_image(url: str) -> Path | None:
    if not url:
        return None
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        suffix = ".jpg"
    target = Path(tempfile.gettempdir()) / f"series_channel_banner_{abs(hash(url))}{suffix}"
    if target.exists() and target.stat().st_size > 0:
        return target
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        response = await client.get(url)
        response.raise_for_status()
        target.write_bytes(response.content)
    return target


async def _edit_message(client, channel, message, caption: str, buttons, image: str, update_media: bool) -> None:
    if update_media and image:
        image_path = await _download_image(image)
        if image_path:
            await client.edit_message(channel, message.id, caption, file=str(image_path), buttons=buttons, parse_mode="html")
            return
    await client.edit_message(channel, message.id, caption, buttons=buttons, parse_mode="html")


async def migrate(args: argparse.Namespace) -> None:
    if not API_ID or not API_HASH:
        raise SystemExit("Configure API_ID/API_HASH ou USERBOT_API_ID/USERBOT_API_HASH no .env.")

    from telethon import TelegramClient

    session_name = args.session or os.getenv("CHANNEL_MIGRATION_SESSION_NAME") or str(DATA_DIR / "channel_migration_userbot")
    client = TelegramClient(session_name, API_ID, API_HASH)
    await client.start(phone=os.getenv("USERBOT_PHONE") or None)

    channel = await client.get_entity(args.channel)
    scanned = matched = edited = skipped = failed = 0
    start_id = int(args.start_id or 0) or None
    end_id = int(args.end_id or 0) or None

    try:
        async for message in client.iter_messages(channel, min_id=start_id, max_id=end_id, reverse=True):
            if args.limit and scanned >= args.limit:
                break
            scanned += 1
            text = str(getattr(message, "raw_text", "") or "")
            if not text.strip():
                skipped += 1
                continue

            resolved = await _resolve_series(text, args.min_score)
            if not resolved:
                skipped += 1
                if args.verbose:
                    print(f"skip message={message.id} reason=no_match query={_candidate_queries(text)!r}")
                continue

            item, detail, seasons, score, query = resolved
            payload_item = _build_payload_item(detail, item)
            token = create_start_payload(
                {
                    "source": "channel_migration",
                    "query": payload_item.get("title") or query,
                    "selected_audio": payload_item.get("default_audio") or "legendado",
                    "item": payload_item,
                }
            )
            deep_link = build_start_link(args.bot_username or BOT_USERNAME, token)
            caption = _build_caption(detail, seasons)
            buttons = _button(deep_link)
            image = str(detail.get("image") or item.get("image") or "").strip()
            matched += 1

            print(
                f"{'DRY' if not args.apply else 'EDIT'} message={message.id} "
                f"score={score:.2f} query={query!r} title={payload_item.get('title')!r} "
                f"media={'yes' if args.update_media and image else 'no'}"
            )

            if not args.apply:
                continue

            try:
                await _edit_message(client, channel, message, caption, buttons, image, args.update_media)
                edited += 1
                await asyncio.sleep(args.delay)
            except Exception as error:
                failed += 1
                print(f"fail message={message.id} error={error!r}")

    finally:
        await client.disconnect()

    print(
        f"done scanned={scanned} matched={matched} edited={edited} "
        f"skipped={skipped} failed={failed} apply={bool(args.apply)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migra posts antigos de canal para o padrão atual com botão do bot.")
    parser.add_argument("--channel", required=True, help="Canal, ex: @Series_Brazil ou link t.me/+...")
    parser.add_argument("--bot-username", default=BOT_USERNAME, help="Username do bot para deep link.")
    parser.add_argument("--session", default="", help="Sessão userbot Telethon. Default: data/channel_migration_userbot")
    parser.add_argument("--start-id", type=int, default=0, help="Começar depois deste message_id.")
    parser.add_argument("--end-id", type=int, default=0, help="Parar antes deste message_id.")
    parser.add_argument("--limit", type=int, default=25, help="Máximo de mensagens para varrer nesta rodada.")
    parser.add_argument("--min-score", type=float, default=0.72, help="Confiança mínima para casar título.")
    parser.add_argument("--delay", type=float, default=1.2, help="Pausa entre edições quando --apply estiver ativo.")
    parser.add_argument("--apply", action="store_true", help="Edita de verdade. Sem isso é dry-run.")
    parser.add_argument("--update-media", action="store_true", help="Também troca a mídia/banner pelo banner atual.")
    parser.add_argument("--verbose", action="store_true", help="Mostra motivos de skip.")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(migrate(parse_args()))
