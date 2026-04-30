"""Callbacks do bot de catalogo."""

import asyncio
import html
import re
import secrets
import time
from urllib.parse import urlparse

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import (
    ADMIN_IDS,
    EPISODES_PER_PAGE,
    SEARCH_SESSION_TTL_SECONDS,
    WATCH_BLOCK_BRAND,
    WATCH_BLOCK_PROMO_COOLDOWN,
    WATCH_BLOCK_URL,
)
from core.video_download_queue import VideoDownloadJob, enqueue_video_download
from handlers.offline_paywall import answer_subscription_check, send_offline_paywall
from handlers.discover import callback_launches, callback_random
from handlers.search import _build_results_keyboard, _build_search_text, get_search_session
from services.cakto_gateway import get_checkout_options
from services.metrics import log_event, mark_user_seen
from services.watch_guard import is_watch_block_active_for_user
from services.subscriptions import is_active_subscriber
from services.catalog_client import (
    get_content_details,
    get_episodes,
    get_player_links,
    get_season_episodes,
    get_seasons,
    search_content,
)
from utils.gatekeeper import ensure_channel_membership

CALLBACK_COOLDOWN = 0.25
_USER_CB_LOCKS: dict[int, asyncio.Lock] = {}
_LAST_CB: dict[int, float] = {}
_PLAYER_FETCH_LOCKS: dict[str, asyncio.Lock] = {}
_RECENT_PLAYER_URLS: dict[str, list[dict]] = {}
_RECENT_PLAYER_URLS_LOCK = asyncio.Lock()
_RECENT_PLAYER_URL_TTL = 120.0
_WATCH_BLOCK_PROMOS: dict[str, float] = {}


def _now() -> float:
    return time.monotonic()


def _user_lock(user_id: int) -> asyncio.Lock:
    lock = _USER_CB_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _USER_CB_LOCKS[user_id] = lock
    return lock


def _player_fetch_lock(content_key: str) -> asyncio.Lock:
    lock = _PLAYER_FETCH_LOCKS.get(content_key)
    if lock is None:
        lock = asyncio.Lock()
        _PLAYER_FETCH_LOCKS[content_key] = lock
    return lock


def _is_cooldown(user_id: int) -> bool:
    last = _LAST_CB.get(user_id, 0.0)
    if _now() - last < CALLBACK_COOLDOWN:
        return True
    _LAST_CB[user_id] = _now()
    return False


def _content_session_key(token: str) -> str:
    return f"pb_content:{token}"


def _episodes_cache_key(token: str, season: int, audio: str = "") -> str:
    audio_key = (audio or "default").strip().lower()
    return f"pb_eps_cache:{token}:{audio_key}:{season}"


def _seasons_cache_key(token: str, audio: str = "") -> str:
    audio_key = (audio or "default").strip().lower()
    return f"pb_seasons_cache:{token}:{audio_key}"


def _movie_cache_key(token: str, audio: str = "") -> str:
    audio_key = (audio or "default").strip().lower()
    return f"pb_movie_cache:{token}:{audio_key}"


def _movie_delivery_cache_key(session: dict) -> str:
    title = _normalize_media_title(str(session.get("title") or "filme"))
    audio = str(session.get("selected_audio") or session.get("default_audio") or "default").strip().lower()
    url = str(session.get("url") or "").strip()
    return f"pb_movie_delivery:{title}:{audio}:{url}"


def _episode_delivery_cache_key(session: dict, season: int, episode: dict, episode_idx: int) -> str:
    title = _normalize_media_title(str(session.get("title") or "serie"))
    audio = str(session.get("selected_audio") or session.get("default_audio") or "default").strip().lower()
    number = _episode_number_value(episode, episode_idx)
    url = str((episode or {}).get("url") or (episode or {}).get("resolver_url") or "").strip()
    return f"pb_episode_delivery:{title}:{audio}:s{season}:e{number}:{url}"


def _player_request_key(content_url: str, audio_key: str = "") -> str:
    return f"{str(content_url or '').strip()}|{str(audio_key or '').strip().lower()}"


def _owner_request_key(user, session_token: str) -> str:
    user_id = getattr(user, "id", 0) or 0
    return f"{user_id}:{session_token}"


def _is_watch_locked_for_user(user) -> bool:
    user_id = getattr(user, "id", 0) or 0
    if _has_subscription_access(user):
        return False
    return is_watch_block_active_for_user(user_id)


def _has_subscription_access(user) -> bool:
    user_id = getattr(user, "id", 0) or 0
    if user_id in ADMIN_IDS:
        return True
    return bool(user_id and is_active_subscriber(user_id))


def _is_temporary_player_url(url: str) -> bool:
    value = (url or "").strip().lower()
    if not value:
        return False
    if "streamtape.com/get_video" in value or "/get_video?" in value:
        return True
    if "tapecontent.net/" in value:
        return True
    return any(token in value for token in ("expires=", "token=", "ip="))


def _should_reuse_player_links(player_links: dict | None) -> bool:
    if not isinstance(player_links, dict) or not player_links:
        return False
    player_url = str(player_links.get("player_url") or "").strip()
    if not player_url:
        return False
    return not _is_temporary_player_url(player_url)


async def _reserve_player_url(content_key: str, owner_key: str, player_url: str) -> bool:
    now = _now()
    async with _RECENT_PLAYER_URLS_LOCK:
        entries = list(_RECENT_PLAYER_URLS.get(content_key) or [])
        entries = [
            entry
            for entry in entries
            if now - float(entry.get("time") or 0.0) <= _RECENT_PLAYER_URL_TTL
        ]

        for entry in entries:
            if entry.get("url") == player_url and entry.get("owner") != owner_key:
                _RECENT_PLAYER_URLS[content_key] = entries
                return False

        entries = [
            entry
            for entry in entries
            if not (entry.get("url") == player_url and entry.get("owner") == owner_key)
        ]
        entries.append({"url": player_url, "owner": owner_key, "time": now})
        _RECENT_PLAYER_URLS[content_key] = entries[-12:]
        return True


async def _fetch_unique_player_links(
    content_url: str,
    preferred_audio: str,
    owner_key: str,
) -> dict:
    content_key = _player_request_key(content_url, preferred_audio)
    last_links: dict = {}

    async with _player_fetch_lock(content_key):
        for attempt in range(4):
            fetched = await asyncio.wait_for(
                get_player_links(str(content_url or ""), preferred_audio=preferred_audio),
                timeout=18,
            )
            player_links = fetched if isinstance(fetched, dict) else {}
            player_url = str(player_links.get("player_url") or "").strip()
            last_links = player_links

            if not player_url or not _is_temporary_player_url(player_url):
                return player_links

            if await _reserve_player_url(content_key, owner_key, player_url):
                return player_links

            await asyncio.sleep(0.45 + (attempt * 0.25))

    return last_links


def _prune_content_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = _now()
    for key, value in list(context.user_data.items()):
        if not key.startswith("pb_content:") or not isinstance(value, dict):
            continue
        created_at = float(value.get("created_at") or 0.0)
        if now - created_at <= SEARCH_SESSION_TTL_SECONDS:
            continue

        context.user_data.pop(key, None)
        token = key.split(":", 1)[1]
        for user_key in list(context.user_data.keys()):
            if user_key.startswith(f"pb_eps_cache:{token}:"):
                context.user_data.pop(user_key, None)
            if user_key.startswith(f"pb_seasons_cache:{token}:"):
                context.user_data.pop(user_key, None)
            if user_key.startswith(f"pb_movie_cache:{token}:"):
                context.user_data.pop(user_key, None)


def _store_content_session(context: ContextTypes.DEFAULT_TYPE, payload: dict) -> str:
    _prune_content_sessions(context)
    token = secrets.token_hex(4)
    context.user_data[_content_session_key(token)] = {
        **payload,
        "created_at": _now(),
    }
    return token


def _get_content_session(context: ContextTypes.DEFAULT_TYPE, token: str) -> dict:
    _prune_content_sessions(context)
    return context.user_data.get(_content_session_key(token)) or {}


async def _safe_answer(query, text: str = "", show_alert: bool = False) -> None:
    try:
        await query.answer(text, show_alert=show_alert)
    except Exception:
        pass


async def _safe_delete(message) -> None:
    try:
        await message.delete()
    except Exception:
        pass


async def _edit_existing_panel(message, text: str, reply_markup, *, image: str = "") -> bool:
    try:
        if getattr(message, "photo", None):
            await message.edit_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
        else:
            await message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=reply_markup,
                disable_web_page_preview=not bool(image),
            )
        return True
    except Exception:
        return False


async def _reply_panel(message, text: str, reply_markup, *, image: str = ""):
    bot = message.get_bot()
    chat_id = getattr(message, "chat_id", None) or message.chat.id

    if image:
        try:
            return await bot.send_photo(
                chat_id=chat_id,
                photo=image,
                caption=text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
        except Exception:
            pass

    return await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=reply_markup,
        disable_web_page_preview=not bool(image),
    )


def _content_type_label(content_type: str) -> str:
    return "Série" if content_type == "series" else "Filme"


def _audio_button_label(audio_key: str, *, locked: bool = False) -> str:
    if locked:
        return f"🔒 {_audio_text_label(audio_key)}"
    if (audio_key or "").strip().lower() == "dublado":
        return "🇧🇷 Dublado"
    return "🇺🇸 Legendado"


def _audio_text_label(audio_key: str) -> str:
    if (audio_key or "").strip().lower() == "dublado":
        return "Dublado"
    return "Legendado"


def _loading_keyboard(label: str = "⏳ Carregando") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data="noop")]])


async def _show_loading_state(query, label: str = "⏳ Carregando"):
    message = getattr(query, "message", None)
    if not message:
        return None
    original_markup = getattr(message, "reply_markup", None)
    if original_markup is None:
        return None
    try:
        await message.edit_reply_markup(reply_markup=_loading_keyboard(label))
    except Exception:
        pass
    return original_markup


async def _restore_reply_markup(message, reply_markup) -> None:
    if not message or reply_markup is None:
        return
    try:
        await message.edit_reply_markup(reply_markup=reply_markup)
    except Exception:
        pass


def _watch_block_promo_key(query) -> str:
    message = getattr(query, "message", None)
    chat_id = getattr(message, "chat_id", None) or getattr(getattr(message, "chat", None), "id", 0) or 0
    user_id = getattr(getattr(query, "from_user", None), "id", 0) or 0
    return f"{chat_id}:{user_id}"


def _watch_block_alert_text() -> str:
    return f"🔒 Disponível apenas para assinantes da {WATCH_BLOCK_BRAND}."


def _watch_block_message_text() -> str:
    brand = html.escape(WATCH_BLOCK_BRAND)
    return (
        f"🔒 <b>Conteúdo exclusivo para assinantes da {brand}</b>\n\n"
        "Esse filme ou episódio está bloqueado aqui no bot no momento.\n\n"
        f"Para assistir com acesso liberado, suporte e catálogo dedicado, assine a "
        f"<b>{brand}</b> pelo botão abaixo."
    )


def _watch_block_keyboard(user_id: int = 0) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(option["label"], url=option["url"])]
        for option in get_checkout_options(user_id)
    ]
    rows.append([InlineKeyboardButton("Ja paguei / verificar", callback_data="subcheck")])
    if not rows or (len(rows) == 1 and WATCH_BLOCK_URL):
        rows.insert(0, [InlineKeyboardButton(f"Assinar {WATCH_BLOCK_BRAND}", url=WATCH_BLOCK_URL)])
    return InlineKeyboardMarkup(rows)


async def _send_watch_block_message(query) -> None:
    message = getattr(query, "message", None)
    if not message:
        return

    cooldown = max(int(WATCH_BLOCK_PROMO_COOLDOWN or 0), 0)
    promo_key = _watch_block_promo_key(query)
    user_id = getattr(getattr(query, "from_user", None), "id", 0) or 0
    now = _now()
    if cooldown > 0:
        last_sent = float(_WATCH_BLOCK_PROMOS.get(promo_key) or 0.0)
        if now - last_sent < cooldown:
            return

    try:
        await message.reply_text(
            _watch_block_message_text(),
            parse_mode="HTML",
            reply_markup=_watch_block_keyboard(user_id),
            disable_web_page_preview=True,
        )
        _WATCH_BLOCK_PROMOS[promo_key] = now
    except Exception:
        pass


async def _show_watch_blocked(query, reply_markup) -> None:
    await _restore_reply_markup(getattr(query, "message", None), reply_markup)
    await _safe_answer(query, _watch_block_alert_text(), show_alert=True)
    await _send_watch_block_message(query)


def _human_bytes(value: int | float | None) -> str:
    size = float(value or 0)
    if size <= 0:
        return "0 MB"
    mb = size / (1024 * 1024)
    if mb < 1024:
        return f"{mb:.1f} MB"
    return f"{mb / 1024:.2f} GB"


def _delivery_progress_text(payload: dict) -> str:
    stage = str((payload or {}).get("stage") or "").strip()
    if stage == "downloading":
        current = int((payload or {}).get("download_bytes") or 0)
        total = int((payload or {}).get("download_total") or 0)
        pct = int((current / max(total, 1)) * 100) if total else 0
        total_text = _human_bytes(total) if total else "calculando"
        return (
            "📥 <b>Baixando video</b>\n\n"
            f"<b>Progresso:</b> {pct}%\n"
            f"<code>{_human_bytes(current)} / {total_text}</code>"
        )
    if stage == "uploading":
        current = int((payload or {}).get("upload_bytes") or 0)
        total = int((payload or {}).get("upload_total") or 0)
        pct = int((current / max(total, 1)) * 100) if total else 0
        return (
            "📤 <b>Enviando video</b>\n\n"
            f"<b>Progresso:</b> {pct}%\n"
            f"<code>{_human_bytes(current)} / {_human_bytes(total)}</code>"
        )
    if stage == "cached":
        return "♻️ <b>Enviando video salvo...</b>"
    if stage == "done":
        return "✅ <b>Video enviado.</b>"
    return "⏳ <b>Preparando video...</b>"


async def _finish_video_delivery(message, request, reply_markup) -> None:
    if not message:
        return

    status_message = None
    last_progress_update = 0.0

    async def progress_callback(payload: dict) -> None:
        nonlocal status_message, last_progress_update
        now = _now()
        stage = str((payload or {}).get("stage") or "")
        if stage not in {"done", "cached"} and now - last_progress_update < 2.0:
            return
        last_progress_update = now

        try:
            text = _delivery_progress_text(payload)
            if status_message is None:
                status_message = await message.reply_text(text, parse_mode="HTML")
            else:
                await status_message.edit_text(text, parse_mode="HTML")
        except Exception:
            pass

    try:
        status_message = await message.reply_text("⏳ <b>Preparando video...</b>", parse_mode="HTML")
        await deliver_video_request(
            message.get_bot(),
            message.chat.id,
            request,
            progress_callback=progress_callback,
        )
    except Exception as exc:
        error_text = html.escape(str(exc or "Não consegui enviar esse vídeo agora.").strip())
        try:
            target_text = (
                "❌ <b>Não consegui enviar esse vídeo no Telegram agora.</b>\n\n"
                f"<i>{error_text}</i>"
            )
            if status_message is not None:
                await status_message.edit_text(target_text, parse_mode="HTML")
            else:
                await message.reply_text(target_text, parse_mode="HTML")
        except Exception:
            pass
    finally:
        await _restore_reply_markup(message, reply_markup)


def _normalize_media_title(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return re.sub(r"[^a-z0-9]+", "", text)


def _audio_key_from_item(item: dict) -> str:
    return "dublado" if item.get("is_dubbed") else "legendado"


def _extract_item_audio_urls(item: dict) -> dict[str, str]:
    audio_urls = item.get("audio_urls") or {}
    if not isinstance(audio_urls, dict):
        return {}

    extracted: dict[str, str] = {}
    for audio_key in ("dublado", "legendado"):
        audio_url = str(audio_urls.get(audio_key) or "").strip()
        if audio_url:
            extracted[audio_key] = audio_url
    return extracted


def _normalize_media_slug(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""

    try:
        raw = urlparse(raw).path.strip("/")
    except Exception:
        raw = raw.strip("/")

    raw = re.sub(r"^assistir-", "", raw)
    raw = re.sub(r"-(dublado|legendado)(?=-\d{4}-\d+/?$)", "", raw)
    raw = re.sub(r"-\d+x\d+(?=-|/|$)", "", raw)
    raw = re.sub(r"-\d{4}-\d+/?$", "", raw)
    return re.sub(r"[^a-z0-9]+", "", raw)


def _collect_audio_urls(results: list[dict], pivot: dict) -> dict[str, str]:
    pivot_title = _normalize_media_title(pivot.get("title") or "")
    pivot_year = str(pivot.get("year") or "").strip()
    pivot_slug = _normalize_media_slug(pivot.get("url") or "")
    audio_urls: dict[str, str] = _extract_item_audio_urls(pivot)

    for item in results:
        item_title = _normalize_media_title(item.get("title") or "")
        item_year = str(item.get("year") or "").strip()
        item_slug = _normalize_media_slug(item.get("url") or "")

        title_matches = bool(pivot_title and item_title and item_title == pivot_title)
        year_matches = not pivot_year or not item_year or item_year == pivot_year
        slug_matches = bool(pivot_slug and item_slug and item_slug == pivot_slug)
        if not slug_matches and not (title_matches and year_matches):
            continue

        item_audio_urls = _extract_item_audio_urls(item)
        if item_audio_urls:
            for audio_key, item_url in item_audio_urls.items():
                if item_url and audio_key not in audio_urls:
                    audio_urls[audio_key] = item_url
            continue

        audio_key = _audio_key_from_item(item)
        item_url = str(item.get("url") or "").strip()
        if item_url and audio_key not in audio_urls:
            audio_urls[audio_key] = item_url

    pivot_audio = _audio_key_from_item(pivot)
    pivot_url = str(pivot.get("url") or "").strip()
    if pivot_url:
        audio_urls[pivot_audio] = pivot_url
    return audio_urls


async def _recover_audio_urls(item: dict, detail: dict) -> dict[str, str]:
    recovered = _extract_item_audio_urls(item)
    base_title = str(item.get("title") or detail.get("title") or "").strip()
    base_year = str(detail.get("year") or item.get("year") or "").strip()
    queries: list[str] = []

    for value in (
        base_title,
        f"{base_title} {base_year}".strip(),
    ):
        query = re.sub(r"\s+", " ", str(value or "").strip())
        if query and query not in queries:
            queries.append(query)

    pivot = {
        "title": item.get("title") or detail.get("title") or "",
        "year": base_year,
        "url": item.get("url") or detail.get("url") or "",
        "is_dubbed": item.get("is_dubbed"),
        "audio_urls": item.get("audio_urls") or {},
    }

    for query in queries:
        try:
            results = await asyncio.wait_for(search_content(query), timeout=10)
        except Exception:
            continue

        merged = _collect_audio_urls(results, pivot)
        for audio_key, audio_url in merged.items():
            if str(audio_url or "").strip() and audio_key not in recovered:
                recovered[audio_key] = str(audio_url).strip()
        if len(recovered) >= 2:
            break

    return recovered


def _build_detail_text(detail: dict) -> str:
    title = html.escape((detail.get("title") or "Sem título").strip())
    year = html.escape(str(detail.get("year") or "").strip())
    duration = html.escape(str(detail.get("duration") or "").strip())
    rating = html.escape(str(detail.get("rating") or "").strip())
    genres = [str(genre).strip() for genre in (detail.get("genres") or []) if str(genre).strip()]
    genres_text = html.escape(" | ".join(genres[:4])) if genres else "Não informado"
    description = html.escape((detail.get("description") or "Sem sinopse.").strip()[:500])
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


def _detail_keyboard(session_token: str, session: dict, *, user_id: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    audio_urls = session.get("audio_urls") or {}
    audio_options = [key for key in ("dublado", "legendado") if str(audio_urls.get(key) or "").strip()]
    watch_blocked = is_watch_block_active_for_user(user_id) and str(session.get("type") or "movie") != "series"

    if not audio_options:
        audio_options = [
            str(option).strip().lower()
            for option in (session.get("audio_options") or [])
            if str(option).strip()
        ]

    if not audio_options:
        audio_options = [str(session.get("selected_audio") or session.get("default_audio") or "legendado").strip().lower()]

    for audio_key in audio_options:
        rows.append(
            [
                InlineKeyboardButton(
                    _audio_button_label(audio_key, locked=watch_blocked),
                    callback_data=f"pb_audio|{session_token}|{audio_key}",
                )
            ]
        )

    back_search_token = str(session.get("search_token") or "").strip()
    back_search_page = int(session.get("search_page") or 1)
    if back_search_token:
        rows.append([InlineKeyboardButton("🔙 Voltar", callback_data=f"pb_back_search|{back_search_token}|{back_search_page}")])
    else:
        rows.append([InlineKeyboardButton("❌ Fechar", callback_data="pb_close")])

    return InlineKeyboardMarkup(rows)


def _episodes_text(title: str, season: int, total: int, audio_key: str) -> str:
    safe_title = html.escape((title or "Série").strip())
    audio_label = html.escape(_audio_text_label(audio_key))
    return (
        f"📺 <b>{safe_title}</b>\n\n"
        "<blockquote>"
        f"📚 <b>Temporada:</b> {season}\n"
        f"🎞️ <b>Episódios:</b> {total}\n"
        f"🎙️ <b>Idioma:</b> {audio_label}"
        "</blockquote>\n\n"
        "<i>Selecione o episódio ou temporada que desejar.</i>"
    )


def _episode_display_label(episode: dict, fallback_index: int) -> str:
    label = str(episode.get("label") or "").strip()
    if label:
        return label
    number = str(episode.get("episode") or "").strip()
    if number:
        return f"Episódio {number}"
    return f"Episódio {fallback_index + 1}"


def _episode_number_value(episode: dict, fallback_index: int) -> int:
    number = str((episode or {}).get("episode") or "").strip()
    if not number:
        label = str((episode or {}).get("label") or "").strip()
        match = re.search(r"(\d+)", label)
        if match:
            number = match.group(1)

    if number.isdigit():
        return int(number)
    return fallback_index + 1


def _episode_button_label(episode: dict, fallback_index: int) -> str:
    number = str(episode.get("episode") or "").strip()
    if not number:
        label = str(episode.get("label") or "").strip()
        match = re.search(r"(\d+)", label)
        if match:
            number = match.group(1)

    if number.isdigit():
        return f"Ep {int(number):02d}"
    return f"Ep {fallback_index + 1:02d}"


def _episode_watch_button_label(episode: dict, fallback_index: int) -> str:
    label = _episode_button_label(episode, fallback_index)
    match = re.search(r"(\d+)", label)
    if match:
        return f"▶️ Assistir {int(match.group(1)):02d}"
    return f"▶️ Assistir {fallback_index + 1:02d}"


def _episode_counter_label(episode: dict, position: int, total: int) -> str:
    number = str(episode.get("episode") or "").strip()
    if not number.isdigit():
        match = re.search(r"(\d+)", str(episode.get("label") or ""))
        if match:
            number = match.group(1)
    current = int(number) if number.isdigit() else position + 1
    return f"{current:02d}/{max(1, total):02d}"


def _season_picker_text(title: str, season: int, total: int, audio_key: str) -> str:
    safe_title = html.escape((title or "Série").strip())
    audio_label = html.escape(_audio_text_label(audio_key))
    return (
        f"📺 <b>{safe_title}</b>\n\n"
        "<blockquote>"
        f"📚 <b>Temporada atual:</b> {season}\n"
        f"🎞️ <b>Episódios:</b> {total}\n"
        f"🎙️ <b>Idioma:</b> {audio_label}"
        "</blockquote>\n\n"
        "<i>Escolha a temporada que deseja abrir.</i>"
    )


def _player_text(series_title: str, season: int, episode: dict, position: int, total: int) -> str:
    title = html.escape((series_title or "Player direto").strip())
    episode_counter = html.escape(_episode_counter_label(episode, position, total))
    return (
        f"▶️ <b>{title}</b>\n\n"
        "<blockquote>"
        f"🎞️ <b>Episódio:</b> {episode_counter}\n"
        f"📚 <b>Temporada:</b> {season}"
        "</blockquote>\n\n"
        "<b>Obs:</b> <i>Este bot não armazena nenhum arquivo em seu servidor. "
        "Todos os conteúdos são fornecidos por terceiros não afiliados.</i>"
    )


def _movie_player_text(title: str, audio_key: str) -> str:
    safe_title = html.escape((title or "Filme").strip())
    audio_label = html.escape(_audio_text_label(audio_key))
    return (
        f"▶️ <b>{safe_title}</b>\n\n"
        "<blockquote>"
        "🎬 <b>Tipo:</b> Filme\n"
        f"🎙️ <b>Idioma:</b> {audio_label}"
        "</blockquote>\n\n"
        "<b>Obs:</b> <i>Este bot não armazena nenhum arquivo em seu servidor. "
        "Todos os conteúdos são fornecidos por terceiros não afiliados.</i>"
    )


def _is_direct_stream_url(url: str) -> bool:
    value = (url or "").lower()
    return any(
        token in value
        for token in (".mp4", ".m3u8", "googlevideo.com/videoplayback", "/videoplayback?", "/get_video?")
    )


def _best_download_url(player_links: dict | None) -> str:
    candidates = _download_candidates(player_links)
    return candidates[0]["url"] if candidates else ""


def _download_candidates(player_links: dict | None) -> list[dict]:
    if not isinstance(player_links, dict):
        return []

    candidates: list[tuple[str, str]] = []
    downloads = player_links.get("downloads") or {}
    if isinstance(downloads, dict):
        for server in ("byse", "mixdrop", "doodstream", "streamtape"):
            item = downloads.get(server)
            if isinstance(item, dict):
                candidates.append((server, str(item.get("url") or "").strip()))
        for server, item in downloads.items():
            if server in {"byse", "mixdrop", "doodstream", "streamtape"}:
                continue
            if isinstance(item, dict):
                candidates.append((server, str(item.get("url") or "").strip()))

    candidates.append(("player", str(player_links.get("player_url") or "").strip()))

    result = []
    seen = set()
    for server, candidate in candidates:
        if candidate and _is_direct_stream_url(candidate):
            if candidate in seen:
                continue
            seen.add(candidate)
            result.append({"label": _best_download_server(player_links, candidate) if server != "player" else "Player", "url": candidate})
    return result


def _best_download_server(player_links: dict | None, selected_url: str) -> str:
    downloads = (player_links or {}).get("downloads") or {}
    if isinstance(downloads, dict):
        for server, item in downloads.items():
            if isinstance(item, dict) and str(item.get("url") or "").strip() == selected_url:
                return str(item.get("label") or server).strip() or server
    return "Player"


def _player_keyboard(
    session_token: str,
    session: dict,
    *,
    player_url: str,
    downloads: dict | None = None,
    season: int = 1,
    page: int = 1,
    episode_idx: int | None = None,
    total_episodes: int = 0,
    watch_label: str = "▶️ Assistir",
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    if episode_idx is not None:
        rows.append([InlineKeyboardButton("📥 Baixar no Telegram", callback_data=f"pb_dl_ep|{session_token}|{season}|{episode_idx}")])
    else:
        rows.append([InlineKeyboardButton("📥 Baixar no Telegram", callback_data=f"pb_dl_movie|{session_token}")])

    if episode_idx is not None and total_episodes > 0:
        nav: list[InlineKeyboardButton] = []
        if episode_idx > 0:
            nav.append(
                InlineKeyboardButton(
                    "⬅️ Anterior",
                    callback_data=f"pb_play|{session_token}|{season}|{page}|{episode_idx - 1}",
                )
            )
        if episode_idx + 1 < total_episodes:
            nav.append(
                InlineKeyboardButton(
                    "Próximo ➡️",
                    callback_data=f"pb_play|{session_token}|{season}|{page}|{episode_idx + 1}",
                )
            )
        if nav:
            rows.append(nav)
        rows.append(
            [InlineKeyboardButton("🔙 Lista de episódios", callback_data=f"pb_eps|{session_token}|{season}|{page}")]
        )
        return InlineKeyboardMarkup(rows)

    rows.append([InlineKeyboardButton("🔙 Detalhes", callback_data=f"pb_detail|{session_token}")])
    return InlineKeyboardMarkup(rows)


def _episodes_keyboard(
    episodes: list[dict],
    page: int,
    session_token: str,
    session: dict,
    season: int,
    total_seasons: int,
) -> InlineKeyboardMarkup:
    start = max(0, (page - 1) * EPISODES_PER_PAGE)
    end = start + EPISODES_PER_PAGE
    page_items = episodes[start:end]
    total_pages = max(1, ((len(episodes) - 1) // EPISODES_PER_PAGE) + 1)

    rows: list[list[InlineKeyboardButton]] = []
    rows.append(
        [
            InlineKeyboardButton(
                f"📚 Temporada {season:02d}",
                callback_data=f"pb_pick_season|{session_token}|{season}|{page}",
            )
        ]
    )

    row: list[InlineKeyboardButton] = []
    for absolute_index, episode in enumerate(page_items, start=start):
        row.append(
            InlineKeyboardButton(
                _episode_button_label(episode, absolute_index),
                callback_data=f"pb_play|{session_token}|{season}|{page}|{absolute_index}",
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if total_pages > 1:
        nav: list[InlineKeyboardButton] = []
        if page > 1:
            nav.append(InlineKeyboardButton("⬅️ Anterior", callback_data=f"pb_eps|{session_token}|{season}|{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        if end < len(episodes):
            nav.append(InlineKeyboardButton("Próxima ➡️", callback_data=f"pb_eps|{session_token}|{season}|{page + 1}"))
        if nav:
            rows.append(nav)

    rows.append([InlineKeyboardButton("🔙 Detalhes", callback_data=f"pb_detail|{session_token}")])
    return InlineKeyboardMarkup(rows)


def _season_picker_keyboard(
    session_token: str,
    seasons: list[int],
    current_season: int,
    return_page: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []

    for season in seasons:
        label = f"✅ T{season:02d}" if season == current_season else f"T{season:02d}"
        row.append(
            InlineKeyboardButton(
                label,
                callback_data=f"pb_season|{session_token}|{season}",
            )
        )
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton("🔙 Voltar aos episódios", callback_data=f"pb_eps|{session_token}|{current_season}|{return_page}")])
    return InlineKeyboardMarkup(rows)


async def _load_series_payload(
    context: ContextTypes.DEFAULT_TYPE,
    session_token: str,
    season: int,
) -> tuple[list[int], list[dict]]:
    session = _get_content_session(context, session_token)
    selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "").strip().lower()
    audio_urls = session.get("audio_urls") or {}
    series_url = str(audio_urls.get(selected_audio) or session.get("url") or "").strip()
    if not series_url:
        return [], []

    seasons = context.user_data.get(_seasons_cache_key(session_token, selected_audio))
    if not isinstance(seasons, list):
        seasons = await get_seasons(series_url, preferred_audio=selected_audio)
        if not seasons:
            seasons = [1]
        context.user_data[_seasons_cache_key(session_token, selected_audio)] = seasons

    episodes = context.user_data.get(_episodes_cache_key(session_token, season, selected_audio))
    if not isinstance(episodes, list):
        if len(seasons) > 1:
            episodes = await get_season_episodes(series_url, season, preferred_audio=selected_audio)
        else:
            episodes = await get_episodes(series_url, preferred_audio=selected_audio)
        context.user_data[_episodes_cache_key(session_token, season, selected_audio)] = episodes

    return seasons, episodes


async def _load_movie_player(
    context: ContextTypes.DEFAULT_TYPE,
    session_token: str,
    *,
    owner_key: str = "",
) -> dict:
    session = _get_content_session(context, session_token)
    selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "").strip().lower()
    cached = context.user_data.get(_movie_cache_key(session_token, selected_audio))
    if _should_reuse_player_links(cached):
        return cached

    audio_urls = session.get("audio_urls") or {}
    url = str(audio_urls.get(selected_audio) or session.get("url") or "").strip()
    if not url:
        return {}

    player_links = await _fetch_unique_player_links(
        url,
        selected_audio,
        owner_key or _player_request_key(url, selected_audio),
    )
    if not isinstance(player_links, dict):
        player_links = {}

    if _should_reuse_player_links(player_links):
        context.user_data[_movie_cache_key(session_token, selected_audio)] = player_links
    else:
        context.user_data.pop(_movie_cache_key(session_token, selected_audio), None)
    return player_links


async def _show_episodes_panel(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    session_token: str,
    season: int,
    page: int,
    *,
    restore_markup=None,
) -> None:
    session = _get_content_session(context, session_token)
    if not session:
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Sessão expirada. Faça uma nova busca.", show_alert=True)
        return

    try:
        seasons, episodes = await _load_series_payload(context, session_token, season)
    except Exception as exc:
        print("ERRO EPISODES:", repr(exc))
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Não consegui carregar os episódios agora.", show_alert=True)
        return

    if not episodes:
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Nenhum episódio foi encontrado.", show_alert=True)
        return

    selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "").strip().lower()
    text = _episodes_text(str(session.get("title") or ""), season, len(episodes), selected_audio)
    keyboard = _episodes_keyboard(
        episodes,
        page,
        session_token,
        session,
        season,
        max(1, len(seasons)),
    )

    if not await _edit_existing_panel(query.message, text, keyboard, image=str(session.get("image") or "")):
        await _reply_panel(query.message, text, keyboard, image=str(session.get("image") or ""))
    await _safe_answer(query)


async def _show_season_picker_panel(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    session_token: str,
    season: int,
    page: int,
    *,
    restore_markup=None,
) -> None:
    session = _get_content_session(context, session_token)
    if not session:
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Sessão expirada. Faça uma nova busca.", show_alert=True)
        return

    try:
        seasons, episodes = await _load_series_payload(context, session_token, season)
    except Exception as exc:
        print("ERRO SEASON PICKER:", repr(exc))
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Não consegui carregar as temporadas agora.", show_alert=True)
        return

    selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "").strip().lower()
    text = _season_picker_text(str(session.get("title") or ""), season, len(episodes), selected_audio)
    keyboard = _season_picker_keyboard(session_token, seasons or [season], season, page)

    if not await _edit_existing_panel(query.message, text, keyboard, image=str(session.get("image") or "")):
        await _reply_panel(query.message, text, keyboard, image=str(session.get("image") or ""))
    await _safe_answer(query)


async def _show_movie_player_panel(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    session_token: str,
    user,
    *,
    restore_markup=None,
) -> None:
    session = _get_content_session(context, session_token)
    if not session:
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Sessão expirada. Faça uma nova busca.", show_alert=True)
        return

    try:
        player_links = await asyncio.wait_for(
            _load_movie_player(
                context,
                session_token,
                owner_key=_owner_request_key(user, session_token),
            ),
            timeout=18,
        )
    except Exception as exc:
        print("ERRO MOVIE PLAYER:", repr(exc))
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Não consegui abrir o StreamTape agora.", show_alert=True)
        return

    player_url = str(player_links.get("player_url") or "").strip()
    if not player_url:
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Não encontrei um link válido do StreamTape.", show_alert=True)
        return

    log_event(
        event_type="watch_click",
        user_id=user.id if user else 0,
        username=((user.username or user.first_name or "") if user else ""),
        query_text=str(session.get("title") or ""),
    )

    selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "").strip().lower()
    text = _movie_player_text(str(session.get("title") or "Filme"), selected_audio)
    keyboard = _player_keyboard(
        session_token,
        session,
        player_url=player_url,
        downloads=player_links.get("downloads"),
    )
    if not await _edit_existing_panel(query.message, text, keyboard, image=str(session.get("image") or "")):
        await _reply_panel(query.message, text, keyboard, image=str(session.get("image") or ""))
    await _safe_answer(query)


async def _show_search_page(query, context: ContextTypes.DEFAULT_TYPE, token: str, page: int, *, restore_markup=None) -> None:
    session = get_search_session(context, token)
    if not session:
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Sessão expirada. Faça uma nova busca.", show_alert=True)
        return

    results = session.get("results") or []
    if not results:
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Nenhum resultado disponível.", show_alert=True)
        return

    heading = str(session.get("heading") or "Resultado da busca")
    text = _build_search_text(str(session.get("query") or ""), page, len(results), heading=heading)
    keyboard = _build_results_keyboard(results, page, len(results), token)

    if not await _edit_existing_panel(query.message, text, keyboard):
        await _reply_panel(query.message, text, keyboard)
    await _safe_answer(query)


async def _show_detail_panel(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    session_token: str,
    *,
    create_new: bool,
    restore_markup=None,
) -> None:
    session = _get_content_session(context, session_token)
    if not session:
        await _restore_reply_markup(getattr(query, "message", None), restore_markup)
        await _safe_answer(query, "Sessão expirada. Faça uma nova busca.", show_alert=True)
        return

    text = str(session.get("detail_text") or "")
    image = str(session.get("image") or "").strip()
    keyboard = _detail_keyboard(session_token, session, user_id=getattr(getattr(query, "from_user", None), "id", 0) or 0)

    if create_new:
        await _reply_panel(query.message, text, keyboard, image=image)
    else:
        if not await _edit_existing_panel(query.message, text, keyboard, image=image):
            await _reply_panel(query.message, text, keyboard, image=image)

    await _safe_answer(query)


async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return

    user = update.effective_user
    if user:
        mark_user_seen(user.id, user.username or user.first_name or "")

    data = query.data

    if data == "noop":
        await _safe_answer(query)
        return

    if data == "pb_launches":
        await callback_launches(update, context)
        return

    if data == "pb_random":
        await callback_random(update, context)
        return

    if data == "pb_requests_hint":
        await _safe_answer(query, "Use /pedido no privado para abrir a central.", show_alert=True)
        return

    if data == "pb_close":
        await _safe_answer(query)
        if query.message:
            await _safe_delete(query.message)
        return

    if not await ensure_channel_membership(update, context):
        await _safe_answer(query)
        return

    if user and _is_cooldown(user.id):
        await _safe_answer(query, "⏳ Aguarde um instante...", show_alert=False)
        return

    if user:
        async with _user_lock(user.id):
            await _handle_callback(update, context)
    else:
        await _handle_callback(update, context)


async def _handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not query.data:
        return

    data = query.data

    if data == "subcheck":
        await answer_subscription_check(query, user.id if user else 0)
        return

    if data.startswith("pb_page|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 3:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return
        await _show_search_page(query, context, parts[1], int(parts[2]), restore_markup=original_markup)
        return

    if data.startswith("pb_back_search|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 3:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return
        await _show_search_page(query, context, parts[1], int(parts[2]), restore_markup=original_markup)
        return

    if data.startswith("pb_item|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 3:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return

        search_token = parts[1]
        index = int(parts[2])
        source_page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
        session = get_search_session(context, search_token)
        if not session:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Sessão expirada. Faça uma nova busca.", show_alert=True)
            return

        results = session.get("results") or []
        if index < 0 or index >= len(results):
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Item inválido.", show_alert=True)
            return

        item = results[index]
        try:
            detail = await asyncio.wait_for(get_content_details(item["url"]), timeout=15)
        except Exception as exc:
            print("ERRO AO CARREGAR DETALHES:", repr(exc))
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _reply_panel(
                query.message,
                "❌ <b>Não consegui carregar os detalhes agora.</b>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Voltar", callback_data=f"pb_back_search|{search_token}|{source_page}")]]),
            )
            return

        detail_text = _build_detail_text(detail)
        content_url = str(detail.get("url") or item.get("url") or "").strip()
        audio_urls = _collect_audio_urls(results, item)
        if len(audio_urls) < 2:
            recovered_audio_urls = await _recover_audio_urls(item, detail)
            for audio_key, audio_url in recovered_audio_urls.items():
                if str(audio_url or "").strip() and audio_key not in audio_urls:
                    audio_urls[audio_key] = str(audio_url).strip()

        item_audio_options = [
            str(option).strip().lower()
            for option in (item.get("audio_options") or [])
            if str(option).strip()
        ]
        detail_audio_options = [
            str(option).strip().lower()
            for option in (detail.get("audio_options") or [])
            if str(option).strip()
        ]
        if content_url:
            for audio_key in item_audio_options + detail_audio_options:
                audio_urls.setdefault(audio_key, content_url)

        audio_options = [key for key in ("dublado", "legendado") if str(audio_urls.get(key) or "").strip()]
        if not audio_options:
            audio_options = item_audio_options or detail_audio_options or [str(detail.get("default_audio") or item.get("default_audio") or _audio_key_from_item(item) or "legendado").strip().lower()]
            if content_url:
                for audio_key in audio_options:
                    audio_urls.setdefault(audio_key, content_url)

        default_audio = str(item.get("default_audio") or detail.get("default_audio") or _audio_key_from_item(item) or "legendado").strip().lower()
        if default_audio not in audio_options and audio_options:
            default_audio = audio_options[0]

        content_session = {
            "url": content_url,
            "title": detail.get("title") or item.get("title") or "",
            "type": detail.get("type") or "movie",
            "image": detail.get("image") or item.get("image") or "",
            "detail_text": detail_text,
            "audio_urls": audio_urls,
            "audio_options": audio_options,
            "default_audio": default_audio,
            "selected_audio": default_audio,
            "search_token": search_token,
            "search_page": source_page,
        }
        session_token = _store_content_session(context, content_session)

        log_event(
            event_type="open_item",
            user_id=user.id if user else 0,
            username=((user.username or user.first_name or "") if user else ""),
            query_text=str(content_session.get("title") or ""),
        )

        if query.message:
            await _safe_delete(query.message)
        await _show_detail_panel(query, context, session_token, create_new=True)
        return

    if data.startswith("pb_detail|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 2:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return
        await _show_detail_panel(query, context, parts[1], create_new=False, restore_markup=original_markup)
        return

    if data.startswith("pb_audio|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 3:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return

        session_token = parts[1]
        selected_audio = str(parts[2] or "").strip().lower()
        session = _get_content_session(context, session_token)
        if not session:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Sessão expirada. Faça uma nova busca.", show_alert=True)
            return

        if str(session.get("type") or "movie") != "series" and _is_watch_locked_for_user(user):
            await _show_watch_blocked(query, original_markup)
            return

        session["selected_audio"] = selected_audio
        context.user_data[_content_session_key(session_token)] = session

        if str(session.get("type") or "movie") == "series":
            await _show_episodes_panel(query, context, session_token, 1, 1, restore_markup=original_markup)
        else:
            await _show_movie_player_panel(query, context, session_token, user, restore_markup=original_markup)
        return

    if data.startswith("pb_eps|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 4:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return

        session_token = parts[1]
        season = int(parts[2]) if parts[2].isdigit() else 1
        page = int(parts[3]) if parts[3].isdigit() else 1
        await _show_episodes_panel(query, context, session_token, season, page, restore_markup=original_markup)
        return

    if data.startswith("pb_season|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 3:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return
        session_token = parts[1]
        season = int(parts[2]) if parts[2].isdigit() else 1
        await _show_episodes_panel(query, context, session_token, season, 1, restore_markup=original_markup)
        return

    if data.startswith("pb_pick_season|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 4:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return
        session_token = parts[1]
        season = int(parts[2]) if parts[2].isdigit() else 1
        page = int(parts[3]) if parts[3].isdigit() else 1
        await _show_season_picker_panel(query, context, session_token, season, page, restore_markup=original_markup)
        return

    if data.startswith("pb_watch|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 2:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return

        if _is_watch_locked_for_user(user):
            await _show_watch_blocked(query, original_markup)
            return

        await _show_movie_player_panel(query, context, parts[1], user, restore_markup=original_markup)
        return

    if data.startswith("pb_dl_movie|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 2:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return

        session_token = parts[1]
        session = _get_content_session(context, session_token)
        if not session:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "SessÃ£o expirada. FaÃ§a uma nova busca.", show_alert=True)
            return

        if _is_watch_locked_for_user(user):
            await _show_watch_blocked(query, original_markup)
            return
        if not _has_subscription_access(user):
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await send_offline_paywall(query, user, str(session.get("title") or ""))
            return

        try:
            player_links = await asyncio.wait_for(_load_movie_player(context, session_token), timeout=18)
        except Exception as exc:
            print("ERRO MOVIE DOWNLOAD:", repr(exc))
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "NÃ£o consegui preparar o filme agora.", show_alert=True)
            return

        download_candidates = _download_candidates(player_links)
        player_url = download_candidates[0]["url"] if download_candidates else ""
        if not player_url or not _is_direct_stream_url(player_url):
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Esse filme ainda nÃ£o pode ser enviado no Telegram.", show_alert=True)
            return

        selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "").strip().lower()
        title = str(session.get("title") or "Filme").strip() or "Filme"
        server_label = _best_download_server(player_links, player_url)
        log_event(
            event_type="telegram_video_request",
            user_id=user.id if user else 0,
            username=((user.username or user.first_name or "") if user else ""),
            query_text=title,
        )
        caption = (
            f"<b>{html.escape(title)}</b>\n"
            "<b>Tipo:</b> Filme\n"
            f"<b>Idioma:</b> {html.escape(_audio_text_label(selected_audio))}"
        )
        try:
            await enqueue_video_download(
                context.application,
                VideoDownloadJob(
                    user_id=user.id if user else 0,
                    chat_id=query.message.chat_id,
                    content_id=_movie_delivery_cache_key(session),
                    item_label=_audio_text_label(selected_audio),
                    quality=server_label,
                    title=title,
                    video_url=player_url,
                    caption=caption,
                    video_urls=download_candidates,
                ),
            )
        except RuntimeError as error:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, str(error), show_alert=True)
            return
        await _restore_reply_markup(getattr(query, "message", None), original_markup)
        await _safe_answer(query, "â³ Preparando o filme no Telegram...")
        return

    if data.startswith("pb_dl_ep|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 4:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return

        session_token = parts[1]
        season = int(parts[2]) if parts[2].isdigit() else 1
        episode_idx = int(parts[3]) if parts[3].isdigit() else 0
        session = _get_content_session(context, session_token)
        if not session:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "SessÃ£o expirada. FaÃ§a uma nova busca.", show_alert=True)
            return

        if _is_watch_locked_for_user(user):
            await _show_watch_blocked(query, original_markup)
            return
        if not _has_subscription_access(user):
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await send_offline_paywall(query, user, str(session.get("title") or ""))
            return

        try:
            _, episodes = await _load_series_payload(context, session_token, season)
        except Exception as exc:
            print("ERRO DOWNLOAD EPISODIO:", repr(exc))
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "NÃ£o consegui preparar esse episÃ³dio agora.", show_alert=True)
            return

        if not episodes or episode_idx < 0 or episode_idx >= len(episodes):
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "EpisÃ³dio invÃ¡lido.", show_alert=True)
            return

        episode = episodes[episode_idx]
        player_links = episode.get("player_links") if isinstance(episode, dict) else {}
        selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "").strip().lower()
        if not isinstance(player_links, dict) or not player_links:
            try:
                player_links = await asyncio.wait_for(
                    get_player_links(str(episode.get("url") or ""), preferred_audio=selected_audio),
                    timeout=18,
                )
            except Exception as exc:
                print("ERRO DOWNLOAD PLAYER LINKS:", repr(exc))
                player_links = {}

        download_candidates = _download_candidates(player_links)
        player_url = download_candidates[0]["url"] if download_candidates else ""
        if not player_url or not _is_direct_stream_url(player_url):
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Esse episÃ³dio ainda nÃ£o pode ser enviado no Telegram.", show_alert=True)
            return

        if isinstance(episode, dict):
            episode["player_links"] = player_links
        context.user_data[_episodes_cache_key(session_token, season, selected_audio)] = episodes

        title = str(session.get("title") or "SÃ©rie").strip() or "SÃ©rie"
        episode_number = _episode_number_value(episode, episode_idx)
        label = _episode_display_label(episode, episode_idx)
        server_label = _best_download_server(player_links, player_url)
        log_event(
            event_type="telegram_video_request",
            user_id=user.id if user else 0,
            username=((user.username or user.first_name or "") if user else ""),
            query_text=f"{title} - {label}",
        )
        caption = (
            f"<b>{html.escape(title)}</b>\n"
            f"<b>Episodio:</b> {html.escape(label)}\n"
            f"<b>Temporada:</b> {season}\n"
            f"<b>Idioma:</b> {html.escape(_audio_text_label(selected_audio))}"
        )
        try:
            await enqueue_video_download(
                context.application,
                VideoDownloadJob(
                    user_id=user.id if user else 0,
                    chat_id=query.message.chat_id,
                    content_id=_episode_delivery_cache_key(session, season, episode, episode_idx),
                    item_label=f"T{season:02d}E{episode_number:02d}",
                    quality=server_label,
                    title=title,
                    video_url=player_url,
                    caption=caption,
                    video_urls=download_candidates,
                ),
            )
        except RuntimeError as error:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, str(error), show_alert=True)
            return
        await _restore_reply_markup(getattr(query, "message", None), original_markup)
        await _safe_answer(query, "â³ Preparando o episÃ³dio no Telegram...")
        return

    if data.startswith("pb_play|"):
        original_markup = await _show_loading_state(query)
        parts = data.split("|")
        if len(parts) < 5:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query)
            return

        session_token = parts[1]
        season = int(parts[2]) if parts[2].isdigit() else 1
        page = int(parts[3]) if parts[3].isdigit() else 1
        episode_idx = int(parts[4]) if parts[4].isdigit() else 0
        session = _get_content_session(context, session_token)
        if not session:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Sessão expirada. Faça uma nova busca.", show_alert=True)
            return

        if _is_watch_locked_for_user(user):
            await _show_watch_blocked(query, original_markup)
            return

        try:
            _, episodes = await _load_series_payload(context, session_token, season)
        except Exception as exc:
            print("ERRO PLAYER:", repr(exc))
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Não consegui carregar o StreamTape agora.", show_alert=True)
            return

        if not episodes or episode_idx < 0 or episode_idx >= len(episodes):
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Episódio inválido.", show_alert=True)
            return

        episode = episodes[episode_idx]
        player_links = episode.get("player_links") if isinstance(episode, dict) else {}
        selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "").strip().lower()
        if not _should_reuse_player_links(player_links):
            try:
                player_links = await _fetch_unique_player_links(
                    str(episode.get("url") or ""),
                    selected_audio,
                    _owner_request_key(user, session_token),
                )
            except Exception as exc:
                print("ERRO PLAYER LINKS:", repr(exc))
                player_links = {}

        player_url = str(player_links.get("player_url") or "").strip()
        if not player_url:
            await _restore_reply_markup(getattr(query, "message", None), original_markup)
            await _safe_answer(query, "Não encontrei um link válido do StreamTape.", show_alert=True)
            return

        if isinstance(episode, dict):
            if _should_reuse_player_links(player_links):
                episode["player_links"] = player_links
            else:
                episode.pop("player_links", None)
        context.user_data[_episodes_cache_key(session_token, season, selected_audio)] = episodes

        title = str(session.get("title") or "")
        label = _episode_display_label(episode, episode_idx)
        log_event(
            event_type="watch_click",
            user_id=user.id if user else 0,
            username=((user.username or user.first_name or "") if user else ""),
            query_text=title,
        )
        log_event(
            event_type="episode_click",
            user_id=user.id if user else 0,
            username=((user.username or user.first_name or "") if user else ""),
            query_text=f"{title} - {label}",
        )

        text = _player_text(title, season, episode, episode_idx, len(episodes))
        keyboard = _player_keyboard(
            session_token,
            session,
            player_url=player_url,
            downloads=player_links.get("downloads"),
            season=season,
            page=max(1, (episode_idx // EPISODES_PER_PAGE) + 1),
            episode_idx=episode_idx,
            total_episodes=len(episodes),
            watch_label=_episode_watch_button_label(episode, episode_idx),
        )

        if not await _edit_existing_panel(query.message, text, keyboard, image=str(session.get("image") or "")):
            await _reply_panel(query.message, text, keyboard, image=str(session.get("image") or ""))
        await _safe_answer(query)
        return

    await _safe_answer(query)
