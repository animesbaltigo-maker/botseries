"""Verifica se o usuario entrou no canal obrigatorio."""

import html
import logging
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import REQUIRED_CHANNEL, REQUIRED_CHANNEL_URL

LOGGER = logging.getLogger(__name__)

_MEMBERSHIP_CACHE: dict[int, tuple[bool, float]] = {}
_MEMBER_TTL = 15
# Revalida rápido para refletir entrada/saída do canal quase na hora.
_NON_MEMBER_TTL = 5
_ALLOWED_MEMBER_STATUSES = {"member", "administrator", "creator"}


def _cache_get(user_id: int) -> bool | None:
    item = _MEMBERSHIP_CACHE.get(user_id)
    if not item:
        return None

    allowed, expires_at = item
    if time.time() >= expires_at:
        _MEMBERSHIP_CACHE.pop(user_id, None)
        return None
    return allowed


def _cache_set(user_id: int, allowed: bool) -> None:
    ttl = _MEMBER_TTL if allowed else _NON_MEMBER_TTL
    _MEMBERSHIP_CACHE[user_id] = (allowed, time.time() + ttl)


def _is_member_allowed(member) -> bool:
    status = str(getattr(member, "status", "") or "").strip().lower()
    if status in _ALLOWED_MEMBER_STATUSES:
        return True
    if status == "restricted" and bool(getattr(member, "is_member", False)):
        return True
    return False


def _channel_keyboard() -> InlineKeyboardMarkup | None:
    if not REQUIRED_CHANNEL_URL:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📢 Entrar no canal", url=REQUIRED_CHANNEL_URL)]]
    )


def _required_channel_label() -> str:
    value = str(REQUIRED_CHANNEL or "").strip()
    return html.escape(value or "canal obrigatório")


async def is_user_in_required_channel(bot, user_id: int) -> bool:
    if not REQUIRED_CHANNEL:
        return True

    cached = _cache_get(user_id)
    if cached is not None:
        return cached

    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        allowed = _is_member_allowed(member)
        if not allowed:
            LOGGER.info(
                "Canal obrigatório negado para user_id=%s em %s: status=%s is_member=%s",
                user_id,
                REQUIRED_CHANNEL,
                getattr(member, "status", None),
                getattr(member, "is_member", None),
            )
        _cache_set(user_id, allowed)
        return allowed
    except Exception as exc:
        LOGGER.warning(
            "Falha ao validar canal obrigatório para user_id=%s em %s. "
            "Verifique se o bot está no canal e com permissão para consultar membros.",
            user_id,
            REQUIRED_CHANNEL,
            exc_info=exc,
        )
        _cache_set(user_id, False)
        return False


async def ensure_channel_membership(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> bool:
    if not REQUIRED_CHANNEL:
        return True

    user = update.effective_user
    message = update.effective_message
    if not user:
        return False

    allowed = await is_user_in_required_channel(context.bot, user.id)
    if allowed:
        return True

    if not message:
        return False

    await message.reply_text(
        "🔒 <b>Acesso restrito</b>\n\n"
        f"Para usar o bot, você precisa entrar primeiro em {_required_channel_label()}.\n"
        "Depois de entrar no canal, tente novamente.",
        parse_mode="HTML",
        reply_markup=_channel_keyboard(),
    )
    return False
