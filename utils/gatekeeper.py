"""Verifica se o usuario entrou em todos os canais obrigatorios."""

import html
import logging
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import REQUIRED_CHANNELS, REQUIRED_CHANNEL_URL

LOGGER = logging.getLogger(__name__)

_MEMBERSHIP_CACHE: dict[int, tuple[bool, float]] = {}
_MEMBER_TTL = 15
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
    return status == "restricted" and bool(getattr(member, "is_member", False))


def _channel_keyboard() -> InlineKeyboardMarkup | None:
    if not REQUIRED_CHANNEL_URL:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📢 Entrar nos canais", url=REQUIRED_CHANNEL_URL)]]
    )


async def is_user_in_required_channel(bot, user_id: int) -> bool:
    if not REQUIRED_CHANNELS:
        return True

    cached = _cache_get(user_id)
    if cached is not None:
        return cached

    for channel in REQUIRED_CHANNELS:
        try:
            member = await bot.get_chat_member(channel, user_id)
        except Exception as exc:
            LOGGER.warning(
                "Falha ao validar canal obrigatorio para user_id=%s em %s.",
                user_id,
                channel,
                exc_info=exc,
            )
            _cache_set(user_id, False)
            return False

        if not _is_member_allowed(member):
            LOGGER.info(
                "Canal obrigatorio negado para user_id=%s em %s: status=%s is_member=%s",
                user_id,
                channel,
                getattr(member, "status", None),
                getattr(member, "is_member", None),
            )
            _cache_set(user_id, False)
            return False

    _cache_set(user_id, True)
    return True


async def ensure_channel_membership(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> bool:
    if not REQUIRED_CHANNELS:
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

    name = html.escape(user.first_name or "amigo")
    await message.reply_text(
        f"🛑 <b>Calma aí, {name}</b>\n\n"
        "Para usar este comando, você precisa entrar nos meus canais primeiro.\n\n"
        "Assim você fica por dentro das novidades, avisos e atualizações.\n\n"
        "Clique abaixo, entre nos canais da pasta e volte para tentar novamente.",
        parse_mode="HTML",
        reply_markup=_channel_keyboard(),
    )
    return False
