"""Inline query - busca rápida de séries e filmes, abrindo no bot com banner."""
import asyncio
import hashlib
import html
import secrets
import time

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InlineQueryResultPhoto,
    InlineQueryResultsButton,
    InputTextMessageContent,
    Update,
)
from telegram.ext import ContextTypes

from services.catalog_client import get_content_details, search_content
from services.start_payloads import build_start_link, create_start_payload
from utils.gatekeeper import is_user_in_required_channel

INLINE_TIMEOUT = 10.0
INLINE_DETAIL_TIMEOUT = 12.0
INLINE_LIMIT = 10

INLINE_PAYLOAD_TTL = 60 * 60
INLINE_PAYLOAD_PREFIX = "pb_i_"
INLINE_PAYLOADS_KEY = "pb_inline_payloads"
INLINE_GATE_START_PARAMETER = "gate_join"


def _now() -> float:
    return time.monotonic()


def _result_id(item: dict, index: int) -> str:
    base = "|".join(
        [
            str(item.get("id") or ""),
            str(item.get("url") or ""),
            str(item.get("title") or ""),
            str(item.get("year") or ""),
            str(index),
        ]
    )
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def _audio_key_from_item(item: dict) -> str:
    return "dublado" if item.get("is_dubbed") else "legendado"


def _audio_text_label(audio_key: str) -> str:
    return "Dublado" if str(audio_key or "").strip().lower() == "dublado" else "Legendado"


def _content_type_label(content_type: str) -> str:
    return "Série" if str(content_type or "").strip().lower() == "series" else "Filme"


def _payload_store(context: ContextTypes.DEFAULT_TYPE) -> dict:
    store = context.application.bot_data.get(INLINE_PAYLOADS_KEY)
    if not isinstance(store, dict):
        store = {}
        context.application.bot_data[INLINE_PAYLOADS_KEY] = store
    return store


def _prune_inline_payloads(context: ContextTypes.DEFAULT_TYPE) -> None:
    store = _payload_store(context)
    now = _now()

    for token, payload in list(store.items()):
        if not isinstance(payload, dict):
            store.pop(token, None)
            continue

        created_at = float(payload.get("created_at") or 0.0)
        if now - created_at > INLINE_PAYLOAD_TTL:
            store.pop(token, None)


def store_inline_payload(context: ContextTypes.DEFAULT_TYPE, payload: dict) -> str:
    _prune_inline_payloads(context)
    store = _payload_store(context)

    while True:
        token = secrets.token_hex(5)
        if token not in store:
            break

    store[token] = {
        **payload,
        "created_at": _now(),
    }
    return token


def get_inline_payload(
    context: ContextTypes.DEFAULT_TYPE,
    token: str,
    *,
    pop: bool = False,
) -> dict:
    _prune_inline_payloads(context)
    store = _payload_store(context)
    payload = store.get(token) or {}
    if pop and token in store:
        store.pop(token, None)
    return payload


def _build_start_link(bot_username: str, token: str) -> str:
    username = str(bot_username or "").strip().lstrip("@")
    return f"https://t.me/{username}?start={INLINE_PAYLOAD_PREFIX}{token}"


def _build_detail_text(detail: dict, *, max_description: int = 500) -> str:
    title = html.escape((detail.get("title") or "Sem título").strip())
    year = html.escape(str(detail.get("year") or "").strip())
    duration = html.escape(str(detail.get("duration") or "").strip())
    rating = html.escape(str(detail.get("rating") or "").strip())
    genres = [str(genre).strip() for genre in (detail.get("genres") or []) if str(genre).strip()]
    genres_text = html.escape(" | ".join(genres[:4])) if genres else "Não informado"
    description = html.escape((detail.get("description") or "Sem sinopse.").strip()[:max_description])
    content_type = _content_type_label(str(detail.get("type") or "movie"))
    title_emoji = "📺" if content_type == "Série" else "🎬"

    type_bits = [content_type]
    if year:
        type_bits.append(year)
    if duration:
        type_bits.append(duration)

    quote_lines = [f"<b>Tipo:</b> {' | '.join(type_bits)}"]
    if rating:
        quote_lines.append(f"<b>Nota:</b> {rating}")
    quote_lines.append(f"<b>Gêneros:</b> {genres_text}")
    quote_text = "\n".join(quote_lines)

    return (
        f"{title_emoji} <b>{title}</b>\n\n"
        f"<blockquote>{quote_text}</blockquote>\n\n"
        f"💬 <i>{description}</i>"
    )


def _build_fallback_detail(item: dict) -> dict:
    return {
        "title": item.get("title") or "Sem título",
        "year": item.get("year") or "",
        "duration": "",
        "rating": "",
        "genres": item.get("genres") or [],
        "description": item.get("description") or "Sem sinopse.",
        "type": item.get("type") or "movie",
        "image": item.get("image") or "",
        "url": item.get("url") or "",
        "audio_options": item.get("audio_options") or [],
        "default_audio": item.get("default_audio") or _audio_key_from_item(item),
    }


def _build_inline_description(detail: dict, selected_audio: str) -> str:
    bits = [_content_type_label(detail.get("type") or "movie"), _audio_text_label(selected_audio)]

    year = str(detail.get("year") or "").strip()
    if year:
        bits.append(year)

    rating = str(detail.get("rating") or "").strip()
    if rating:
        bits.append(f"⭐ {rating}")

    return " · ".join(bits)


async def _safe_get_detail(item: dict) -> dict:
    url = str(item.get("url") or "").strip()
    if not url:
        return _build_fallback_detail(item)

    try:
        detail = await asyncio.wait_for(get_content_details(url), timeout=INLINE_DETAIL_TIMEOUT)
        if isinstance(detail, dict) and detail:
            return {
                **_build_fallback_detail(item),
                **detail,
            }
    except Exception:
        pass

    return _build_fallback_detail(item)


async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.inline_query
    if not query:
        return

    user = getattr(query, "from_user", None)
    user_id = getattr(user, "id", 0) or 0
    if user_id and not await is_user_in_required_channel(context.bot, user_id):
        await query.answer(
            [],
            cache_time=0,
            is_personal=True,
            button=InlineQueryResultsButton(
                text="📢 Entrar no canal para usar",
                start_parameter=INLINE_GATE_START_PARAMETER,
            ),
        )
        return

    text = (query.query or "").strip()
    if len(text) < 2:
        await query.answer([], cache_time=5, is_personal=True)
        return

    try:
        results_raw = await asyncio.wait_for(search_content(text), timeout=INLINE_TIMEOUT)
    except Exception:
        await query.answer([], cache_time=5, is_personal=True)
        return

    bot_username = getattr(context.bot, "username", "") or ""
    if not bot_username:
        await query.answer([], cache_time=5, is_personal=True)
        return

    limited_items = [item for item in results_raw[:INLINE_LIMIT] if str(item.get("url") or "").strip()]
    if not limited_items:
        await query.answer([], cache_time=5, is_personal=True)
        return

    detail_tasks = [_safe_get_detail(item) for item in limited_items]
    details_list = await asyncio.gather(*detail_tasks, return_exceptions=False)

    results = []

    for index, (item, detail) in enumerate(zip(limited_items, details_list), start=1):
        content_url = str(item.get("url") or "").strip()
        selected_audio = _audio_key_from_item(item)
        audio_emoji = "🎙️" if selected_audio == "dublado" else "📝"

        token = create_start_payload(
            {
                "source": "inline",
                "query": text,
                "selected_audio": selected_audio,
                "item": {
                    "id": item.get("id"),
                    "title": item.get("title") or detail.get("title") or "",
                    "year": item.get("year") or detail.get("year") or "",
                    "url": content_url,
                    "image": detail.get("image") or item.get("image") or "",
                    "type": detail.get("type") or item.get("type") or "movie",
                    "is_dubbed": bool(item.get("is_dubbed")),
                    "audio_urls": item.get("audio_urls") or {},
                    "audio_options": detail.get("audio_options") or item.get("audio_options") or [],
                    "default_audio": detail.get("default_audio") or item.get("default_audio") or selected_audio,
                },
            }
        )

        deep_link = build_start_link(bot_username, token)

        title_raw = str(detail.get("title") or item.get("title") or "Sem título").strip()
        year_raw = str(detail.get("year") or item.get("year") or "").strip()
        image_url = str(detail.get("image") or item.get("image") or "").strip()

        label = f"{audio_emoji} {title_raw}"
        if year_raw:
            label += f" ({year_raw})"

        description_text = _build_inline_description(detail, selected_audio)[:128]
        detail_text = _build_detail_text(detail)
        photo_caption = _build_detail_text(detail, max_description=280)

        reply_markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "▶️ Assistir no bot",
                        url=deep_link,
                    )
                ]
            ]
        )

        if image_url:
            results.append(
                InlineQueryResultPhoto(
                    id=_result_id(item, index),
                    photo_url=image_url,
                    thumbnail_url=image_url,
                    title=label[:64],
                    description=description_text,
                    caption=photo_caption,
                    parse_mode="HTML",
                    reply_markup=reply_markup,
                )
            )
        else:
            results.append(
                InlineQueryResultArticle(
                    id=_result_id(item, index),
                    title=label[:64],
                    description=description_text,
                    input_message_content=InputTextMessageContent(
                        message_text=detail_text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    ),
                    reply_markup=reply_markup,
                )
            )

    await query.answer(results, cache_time=15, is_personal=True)
