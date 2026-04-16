"""Postagem automatica de novos episodios."""

import asyncio
import html
import json
import logging
from pathlib import Path
from threading import Lock

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import (
    ADMIN_IDS,
    CANAL_ATUALIZACOES_TAG,
    CANAL_POSTAGEM_EPISODIOS,
    DATA_DIR,
)
from services.catalog_client import get_content_details, get_recent_series
from services.start_payloads import build_start_link, create_start_payload

POSTED_JSON_PATH = Path(DATA_DIR) / "episodios_postados.json"

LOGGER = logging.getLogger(__name__)
_POSTED_FILE_LOCK = Lock()
_POSTED_JOB_LOCK = asyncio.Lock()
_POSTED_CACHE: set[str] | None = None


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def _load_posted_unlocked() -> set[str]:
    global _POSTED_CACHE

    if _POSTED_CACHE is not None:
        return set(_POSTED_CACHE)

    if not POSTED_JSON_PATH.exists():
        _POSTED_CACHE = set()
        return set()

    try:
        data = json.loads(POSTED_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        _POSTED_CACHE = set()
        return set()

    if isinstance(data, list):
        _POSTED_CACHE = {str(item).strip() for item in data if str(item).strip()}
        return set(_POSTED_CACHE)

    _POSTED_CACHE = set()
    return set()


def _load_posted() -> set[str]:
    with _POSTED_FILE_LOCK:
        return _load_posted_unlocked()


def _save_posted(posted: set[str]) -> None:
    global _POSTED_CACHE

    normalized = {str(item).strip() for item in posted if str(item).strip()}
    with _POSTED_FILE_LOCK:
        POSTED_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        POSTED_JSON_PATH.write_text(
            json.dumps(sorted(normalized), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _POSTED_CACHE = set(normalized)


def _audio_label(item: dict) -> str:
    audio = str(item.get("audio") or "").strip()
    if audio:
        return audio

    audio_options = [
        str(option).strip().lower()
        for option in (item.get("audio_options") or [])
        if str(option).strip()
    ]

    labels = []
    if "dublado" in audio_options:
        labels.append("Dublado")
    if "legendado" in audio_options:
        labels.append("Legendado")

    if labels:
        return " | ".join(labels)

    return "Dublado" if item.get("is_dubbed") else "Legendado"


def _status_label(item: dict) -> str:
    raw = str(item.get("status") or "").strip()
    if raw:
        return raw

    for key in ("release_status", "episode_status", "content_status"):
        value = str(item.get(key) or "").strip()
        if value:
            return value

    return "Lançado"


def _episode_label(item: dict) -> str:
    candidates = [
        item.get("latest_episode"),
        item.get("last_episode"),
        item.get("episode"),
        item.get("ep"),
        item.get("current_episode"),
        item.get("number"),
    ]

    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value

    for key in ("title", "name"):
        raw_title = str(item.get(key) or "").strip()
        if not raw_title:
            continue

        lowered = raw_title.lower()
        markers = ["episodio", "episódio", "ep."]
        for marker in markers:
            if marker in lowered:
                idx = lowered.find(marker)
                extracted = raw_title[idx:].strip()
                if extracted:
                    return extracted

    return "Novo episódio"


def _episode_identity(item: dict) -> str:
    url = str(item.get("url") or "").strip()
    episode = _episode_label(item)
    if url:
        return f"{url}|{episode}"
    return f"{str(item.get('title') or '').strip()}|{episode}"


def _build_caption(item: dict, detail: dict | None = None) -> str:
    detail = detail or {}

    title = html.escape((detail.get("title") or item.get("title") or "Sem título").strip())
    episode = html.escape(_episode_label(item))
    status = html.escape(_status_label(item))
    audio = html.escape(_audio_label(item))
    footer = html.escape(CANAL_ATUALIZACOES_TAG or "@AtualizacoesOn")

    return (
        f"📺 <b>{title}</b>\n\n"
        "<blockquote>"
        f"<b>Episódio:</b> {episode}\n"
        f"<b>Status:</b> {status}\n"
        f"<b>Áudio:</b> {audio}"
        "</blockquote>\n\n"
        f"» {footer}"
    )


def _pick_image(item: dict, detail: dict | None = None) -> str:
    detail = detail or {}

    candidates = [
        detail.get("image"),
        detail.get("banner_url"),
        detail.get("banner"),
        detail.get("cover_url"),
        detail.get("cover"),
        detail.get("poster_url"),
        detail.get("poster"),
        item.get("image"),
        item.get("banner_url"),
        item.get("banner"),
        item.get("cover_url"),
        item.get("cover"),
        item.get("poster_url"),
        item.get("poster"),
        item.get("thumbnail_url"),
        item.get("thumbnail"),
        item.get("thumb"),
    ]

    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value

    return ""


def _build_payload_item(item: dict, detail: dict | None = None) -> dict:
    detail = detail or {}
    selected_audio = "dublado" if item.get("is_dubbed") else "legendado"

    return {
        "id": item.get("id"),
        "title": detail.get("title") or item.get("title") or "",
        "year": detail.get("year") or item.get("year") or "",
        "url": str(item.get("url") or "").strip(),
        "image": _pick_image(item, detail),
        "type": detail.get("type") or item.get("type") or "series",
        "is_dubbed": bool(item.get("is_dubbed")),
        "audio_urls": item.get("audio_urls") or {},
        "audio_options": detail.get("audio_options") or item.get("audio_options") or [],
        "default_audio": detail.get("default_audio") or item.get("default_audio") or selected_audio,
    }


def _build_keyboard(
    context: ContextTypes.DEFAULT_TYPE,
    item: dict,
    detail: dict | None = None,
) -> InlineKeyboardMarkup:
    bot_username = getattr(context.bot, "username", "") or ""
    if not bot_username:
        return InlineKeyboardMarkup([])

    payload_item = _build_payload_item(item, detail)
    token = create_start_payload(
        {
            "source": "novoseps",
            "query": "",
            "selected_audio": payload_item.get("default_audio") or "legendado",
            "item": payload_item,
        }
    )
    deep_link = build_start_link(bot_username, token)

    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("▶️ Assistir no bot", url=deep_link)]]
    )


async def _fetch_detail(item: dict) -> dict:
    url = str(item.get("url") or "").strip()
    if not url:
        return {}

    try:
        detail = await asyncio.wait_for(get_content_details(url), timeout=15)
        if isinstance(detail, dict):
            return detail
    except Exception as exc:
        LOGGER.warning("Falha ao buscar detalhes de %s: %r", url, exc)

    return {}


async def _post_item(context: ContextTypes.DEFAULT_TYPE, item: dict, destination) -> bool:
    try:
        detail = await _fetch_detail(item)
        caption = _build_caption(item, detail)
        keyboard = _build_keyboard(context, item, detail)
        image = _pick_image(item, detail)

        if image:
            try:
                await context.bot.send_photo(
                    chat_id=destination,
                    photo=image,
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as photo_error:
                LOGGER.warning("Falha ao enviar foto do item %s: %r", item.get("url"), photo_error)
                await context.bot.send_message(
                    chat_id=destination,
                    text=caption,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                    disable_web_page_preview=True,
                )
        else:
            await context.bot.send_message(
                chat_id=destination,
                text=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

        return True

    except Exception as exc:
        LOGGER.error("Erro ao postar item %s: %r", item.get("url"), exc)
        return False


async def _recent_feed(limit: int) -> list[dict]:
    items = await get_recent_series(limit=limit)

    merged: list[dict] = []
    seen: set[str] = set()

    for item in items:
        identity = _episode_identity(item)
        if not identity or identity in seen:
            continue
        seen.add(identity)
        merged.append(item)

    return merged


async def _check_and_post(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    destination,
    limit: int = 10,
    delay: float = 2.0,
):
    async with _POSTED_JOB_LOCK:
        posted = _load_posted()
        items = await _recent_feed(limit=limit)

        success = 0
        fail = 0

        for item in items:
            identity = _episode_identity(item)
            if not identity or identity in posted:
                continue

            ok = await _post_item(context, item, destination)
            if ok:
                posted.add(identity)
                _save_posted(posted)
                success += 1
            else:
                fail += 1

            await asyncio.sleep(delay)

        return success, fail


async def postnovoseps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text("❌ <b>Sem permissão.</b>", parse_mode="HTML")
        return

    limit = 10
    if context.args:
        try:
            limit = max(1, min(30, int(context.args[0])))
        except Exception:
            pass

    destination = CANAL_POSTAGEM_EPISODIOS or message.chat_id

    status = await message.reply_text(
        "📡 <b>Verificando novos episódios...</b>\n<i>Aguarde um instante.</i>",
        parse_mode="HTML",
    )

    try:
        success, fail = await _check_and_post(
            context,
            destination=destination,
            limit=limit,
        )

        await status.edit_text(
            "✅ <b>Verificação concluída.</b>\n\n"
            f"<b>Destino:</b> <code>{html.escape(str(destination))}</code>\n"
            f"<b>Postados:</b> <code>{success}</code>\n"
            f"<b>Falhas:</b> <code>{fail}</code>",
            parse_mode="HTML",
        )
    except Exception as exc:
        LOGGER.exception("ERRO POSTNOVOSEPS: %r", exc)
        await status.edit_text(
            "❌ <b>Não consegui verificar os novos episódios agora.</b>\n\n"
            "<i>Tente novamente em instantes.</i>",
            parse_mode="HTML",
        )


async def auto_post_new_eps_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not CANAL_POSTAGEM_EPISODIOS:
        LOGGER.warning("[AUTO_POST] CANAL_POSTAGEM_EPISODIOS nao configurado; job ignorado.")
        return

    LOGGER.info("[AUTO_POST] iniciando checagem de novos episodios...")
    try:
        success, fail = await _check_and_post(
            context,
            destination=CANAL_POSTAGEM_EPISODIOS,
            limit=12,
        )
        LOGGER.info("[AUTO_POST] postados=%s falhas=%s", success, fail)
    except Exception as exc:
        LOGGER.exception("[AUTO_POST] erro=%r", exc)