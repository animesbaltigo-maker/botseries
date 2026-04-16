import asyncio
import html
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import BOT_BRAND
from handlers.callbacks import (
    _audio_key_from_item,
    _build_detail_text,
    _detail_keyboard,
    _recover_audio_urls,
    _reply_panel,
    _store_content_session,
)
from handlers.inline import INLINE_PAYLOAD_PREFIX, get_inline_payload
from services.start_payloads import START_PAYLOAD_PREFIX, get_start_payload
from services.catalog_client import get_content_details
from services.metrics import mark_user_seen
from services.referral_db import (
    register_interaction,
    register_referral_click,
    try_qualify_referral,
    upsert_user,
)
from services.user_registry import register_user
from utils.gatekeeper import ensure_channel_membership

START_COOLDOWN = 1.0
START_DEEP_LINK_TTL = 8.0

_START_USER_LOCKS: dict[int, asyncio.Lock] = {}
_START_INFLIGHT: dict[str, float] = {}


def _user_lock(user_id: int) -> asyncio.Lock:
    lock = _START_USER_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _START_USER_LOCKS[user_id] = lock
    return lock


def _now() -> float:
    return time.monotonic()


def _payload_key(user_id: int, payload: str) -> str:
    return f"{user_id}:{payload}"


def _is_start_cooldown(context: ContextTypes.DEFAULT_TYPE, user_id: int, payload: str) -> bool:
    now = _now()
    last_ts = context.user_data.get(f"start_last:{user_id}", 0.0)
    last_payload = context.user_data.get(f"start_payload:{user_id}", "")
    if payload and payload == last_payload and (now - last_ts) < START_COOLDOWN:
        return True
    context.user_data[f"start_last:{user_id}"] = now
    context.user_data[f"start_payload:{user_id}"] = payload
    return False


def _is_inflight(user_id: int, payload: str) -> bool:
    key = _payload_key(user_id, payload)
    last = _START_INFLIGHT.get(key, 0.0)
    if not last:
        return False
    if _now() - last > START_DEEP_LINK_TTL:
        _START_INFLIGHT.pop(key, None)
        return False
    return True


def _set_inflight(user_id: int, payload: str) -> None:
    _START_INFLIGHT[_payload_key(user_id, payload)] = _now()


def _clear_inflight(user_id: int, payload: str) -> None:
    _START_INFLIGHT.pop(_payload_key(user_id, payload), None)


def _start_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🍿 Lançamentos", callback_data="pb_launches")],
        [InlineKeyboardButton("🎲 Aleatório", callback_data="pb_random")],
    ]
    return InlineKeyboardMarkup(rows)


async def _safe_delete(message) -> None:
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


def _get_deeplink_payload(context: ContextTypes.DEFAULT_TYPE, payload: str) -> dict:
    if payload.startswith(INLINE_PAYLOAD_PREFIX):
        token = payload[len(INLINE_PAYLOAD_PREFIX):].strip()
        if token:
            return get_inline_payload(context, token, pop=False) or {}

    if payload.startswith(START_PAYLOAD_PREFIX):
        token = payload[len(START_PAYLOAD_PREFIX):].strip()
        if token:
            return get_start_payload(token) or {}

    return {}


async def _open_deeplink_payload_from_start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    payload: str,
) -> bool:
    message = update.effective_message
    if not message:
        return False

    inline_payload = _get_deeplink_payload(context, payload)
    if not inline_payload:
        await message.reply_text(
            "❌ <b>Esse link expirou.</b>\n\n<i>Volte no inline e selecione o conteúdo novamente.</i>",
            parse_mode="HTML",
        )
        return True

    item = inline_payload.get("item") or {}
    content_url = str(item.get("url") or "").strip()
    if not content_url:
        await message.reply_text(
            "❌ <b>Não encontrei esse conteúdo.</b>\n\n<i>Tente selecionar novamente pelo inline.</i>",
            parse_mode="HTML",
        )
        return True

    service_message = None
    try:
        service_message = await message.reply_text(
            "⏳ <b>Buscando no catálogo...</b>\n<i>Aguarde um instante.</i>",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

        detail = await asyncio.wait_for(get_content_details(content_url), timeout=15)
        if not isinstance(detail, dict) or not detail:
            raise RuntimeError("Detalhes não encontrados.")

        detail_text = _build_detail_text(detail)

        selected_audio = str(
            inline_payload.get("selected_audio")
            or item.get("default_audio")
            or _audio_key_from_item(item)
            or "legendado"
        ).strip().lower()

        audio_urls = {}
        raw_audio_urls = item.get("audio_urls") or {}
        if isinstance(raw_audio_urls, dict):
            for audio_key in ("dublado", "legendado"):
                audio_url = str(raw_audio_urls.get(audio_key) or "").strip()
                if audio_url:
                    audio_urls[audio_key] = audio_url

        recovered_audio_urls = await _recover_audio_urls(item, detail)
        for audio_key, audio_url in recovered_audio_urls.items():
            if str(audio_url or "").strip():
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

        if not audio_urls and content_url:
            audio_urls[selected_audio] = content_url

        audio_options = [key for key in ("dublado", "legendado") if str(audio_urls.get(key) or "").strip()]
        if not audio_options:
            audio_options = item_audio_options or detail_audio_options or [selected_audio]

        default_audio = str(
            detail.get("default_audio")
            or item.get("default_audio")
            or selected_audio
            or "legendado"
        ).strip().lower()

        if default_audio not in audio_options and audio_options:
            default_audio = audio_options[0]

        if selected_audio not in audio_options and audio_options:
            selected_audio = audio_options[0]

        content_session = {
            "url": content_url,
            "title": detail.get("title") or item.get("title") or "",
            "type": detail.get("type") or item.get("type") or "movie",
            "image": detail.get("image") or item.get("image") or "",
            "detail_text": detail_text,
            "audio_urls": audio_urls,
            "audio_options": audio_options,
            "default_audio": default_audio,
            "selected_audio": selected_audio,
            "search_token": "",
            "search_page": 1,
        }

        session_token = _store_content_session(context, content_session)
        keyboard = _detail_keyboard(session_token, content_session)

        await _safe_delete(service_message)
        await _reply_panel(
            message,
            detail_text,
            keyboard,
            image=str(content_session.get("image") or ""),
        )
        return True

    except Exception:
        await _safe_delete(service_message)
        await message.reply_text(
            "❌ <b>Não consegui abrir esse conteúdo agora.</b>\n\n<i>Tente novamente em instantes.</i>",
            parse_mode="HTML",
        )
        return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    username = user.username or user.first_name or ""
    mark_user_seen(user.id, username)
    upsert_user(user.id, user.username or "", user.first_name or "")
    register_user(user.id, username)

    payload = context.args[0].strip() if context.args else ""

    if payload and _is_start_cooldown(context, user.id, payload):
        await message.reply_text("⏳ Aguarde um instante antes de repetir essa ação.")
        return

    if payload and _is_inflight(user.id, payload):
        await message.reply_text("⏳ Essa solicitação já está sendo processada.")
        return

    async with _user_lock(user.id):
        if payload:
            _set_inflight(user.id, payload)

        try:
            if payload.startswith("ref_"):
                ref_code = payload[4:]
                register_referral_click(ref_code, user.id)
                try_qualify_referral(user.id)

            if not await ensure_channel_membership(update, context):
                return

            register_interaction(user.id)

            if payload.startswith(INLINE_PAYLOAD_PREFIX) or payload.startswith(START_PAYLOAD_PREFIX):
                handled = await _open_deeplink_payload_from_start(update, context, payload)
                if handled:
                    return

            first_name = html.escape(user.first_name or "cinefilo")
            text = (
                f"👋 <b>Olá, {first_name}!</b>\n\n"
                f"Bem-vindo ao <b>{html.escape(BOT_BRAND)}</b>, seu bot de <b>filmes e séries</b> no Telegram.\n\n"
                "Aqui você encontra conteúdos para explorar e assistir com mais praticidade, direto pelo bot.\n\n"
                "<i>Escolha uma opção abaixo para começar.</i>"
            )

            await message.reply_text(
                text,
                parse_mode="HTML",
                reply_markup=_start_keyboard(),
                disable_web_page_preview=True,
            )
        finally:
            if payload:
                _clear_inflight(user.id, payload)