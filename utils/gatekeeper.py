"""Verifica se o usuario entrou no canal obrigatorio."""

import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import REQUIRED_CHANNEL, REQUIRED_CHANNEL_URL

_MEMBERSHIP_CACHE: dict[int, tuple[bool, float]] = {}
_MEMBER_TTL = 300
_NON_MEMBER_TTL = 60


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


async def ensure_channel_membership(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> bool:
    if not REQUIRED_CHANNEL:
        return True

    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return False

    cached = _cache_get(user.id)
    if cached is True:
        return True

    try:
        member = await context.bot.get_chat_member(REQUIRED_CHANNEL, user.id)
        allowed = member.status in ("member", "administrator", "creator")
        _cache_set(user.id, allowed)
        if allowed:
            return True
    except Exception:
        if cached is None:
            return True

    _cache_set(user.id, False)

    keyboard = None
    if REQUIRED_CHANNEL_URL:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📢 Entrar no canal", url=REQUIRED_CHANNEL_URL)]]
        )

    await message.reply_text(
        "🔒 <b>Acesso restrito</b>\n\n"
        "Para usar o bot voce precisa entrar no canal primeiro.\n"
        "Depois disso, tente novamente.",
        parse_mode="HTML",
        reply_markup=keyboard,
    )
    return False
