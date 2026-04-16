import asyncio
import random

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import RECENT_ITEMS_LIMIT
from handlers.search import _build_results_keyboard, _build_search_text, _store_search_session
from services.metrics import mark_user_seen
from services.catalog_client import get_recent_movies, get_recent_series
from utils.gatekeeper import ensure_channel_membership


def _private_only_text() -> str:
    return (
        "🔒 <b>Esse comando so funciona no privado.</b>\n\n"
        "Me chama no PV para navegar pelos titulos com botoes."
    )


def _merge_recent_items(series: list[dict], movies: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen: set[str] = set()
    max_len = max(len(series), len(movies))

    for index in range(max_len):
        for bucket in (series, movies):
            if index >= len(bucket):
                continue
            item = dict(bucket[index])
            url = str(item.get("url") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            merged.append(item)
    return merged


async def _recent_items(limit: int = RECENT_ITEMS_LIMIT) -> list[dict]:
    series, movies = await asyncio.gather(
        get_recent_series(limit=limit),
        get_recent_movies(limit=limit),
    )
    return _merge_recent_items(series, movies)


def _random_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🎬 Abrir detalhes", callback_data=f"pb_item|{token}|0|1")],
            [InlineKeyboardButton("🔄 Sortear outro", callback_data="pb_random")],
        ]
    )


async def _send_launches_message(message, context: ContextTypes.DEFAULT_TYPE, *, edit: bool) -> None:
    items = await _recent_items()
    if not items:
        text = "❌ <b>Nao consegui carregar os lancamentos agora.</b>"
        if edit:
            await message.edit_text(text, parse_mode="HTML")
        else:
            await message.reply_text(text, parse_mode="HTML")
        return

    token = _store_search_session(
        context,
        "novidades recentes",
        items,
        heading="Lancamentos recentes",
    )
    text = _build_search_text("novidades recentes", 1, len(items), heading="Lancamentos recentes")
    keyboard = _build_results_keyboard(items, 1, len(items), token)

    if edit:
        await message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    else:
        await message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def _send_random_message(message, context: ContextTypes.DEFAULT_TYPE, *, edit: bool) -> None:
    items = await _recent_items()
    if not items:
        text = "❌ <b>Nao consegui sortear um titulo agora.</b>"
        if edit:
            await message.edit_text(text, parse_mode="HTML")
        else:
            await message.reply_text(text, parse_mode="HTML")
        return

    item = random.choice(items)
    token = _store_search_session(
        context,
        "sugestao aleatoria",
        [item],
        heading="Sugestao aleatoria",
    )
    text = (
        "🎲 <b>Sugestao aleatoria</b>\n\n"
        "Escolhi um titulo para voce abrir agora.\n"
        "Se nao curtir, toque em <b>Sortear outro</b>."
    )
    keyboard = _random_keyboard(token)

    if edit:
        await message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    else:
        await message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def lancamentos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_channel_membership(update, context):
        return

    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message:
        return

    if user:
        mark_user_seen(user.id, user.username or user.first_name or "")

    if not chat or chat.type != "private":
        await message.reply_text(_private_only_text(), parse_mode="HTML")
        return

    loading = await message.reply_text(
        "🍿 <b>Carregando os lancamentos mais recentes...</b>",
        parse_mode="HTML",
    )
    try:
        await _send_launches_message(loading, context, edit=True)
    except Exception as exc:
        print("ERRO LANCAMENTOS:", repr(exc))
        await loading.edit_text(
            "❌ <b>Nao consegui carregar os lancamentos agora.</b>",
            parse_mode="HTML",
        )


async def aleatorio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_channel_membership(update, context):
        return

    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not message:
        return

    if user:
        mark_user_seen(user.id, user.username or user.first_name or "")

    if not chat or chat.type != "private":
        await message.reply_text(_private_only_text(), parse_mode="HTML")
        return

    loading = await message.reply_text(
        "🎲 <b>Escolhendo um titulo para voce...</b>",
        parse_mode="HTML",
    )
    try:
        await _send_random_message(loading, context, edit=True)
    except Exception as exc:
        print("ERRO ALEATORIO:", repr(exc))
        await loading.edit_text(
            "❌ <b>Nao consegui sortear um titulo agora.</b>",
            parse_mode="HTML",
        )


async def callback_launches(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer("🍿 Carregando lancamentos...")
    await _send_launches_message(query.message, context, edit=True)


async def callback_random(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer("🎲 Sorteando outro titulo...")
    await _send_random_message(query.message, context, edit=True)
