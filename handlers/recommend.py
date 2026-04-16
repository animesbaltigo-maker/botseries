from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from utils.gatekeeper import ensure_channel_membership


def _genre_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⚔️ Ação", callback_data="rec|genre|acao"),
            InlineKeyboardButton("💖 Romance", callback_data="rec|genre|romance"),
        ],
        [
            InlineKeyboardButton("😂 Comédia", callback_data="rec|genre|comedia"),
            InlineKeyboardButton("😱 Terror", callback_data="rec|genre|terror"),
        ],
        [
            InlineKeyboardButton("🧠 Mistério", callback_data="rec|genre|misterio"),
            InlineKeyboardButton("🪄 Fantasia", callback_data="rec|genre|fantasia"),
        ],
        [
            InlineKeyboardButton("🏐 Esportes", callback_data="rec|genre|esportes"),
            InlineKeyboardButton("😭 Drama", callback_data="rec|genre|drama"),
        ],
    ])


async def recomendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return

    message = update.effective_message
    chat = update.effective_chat

    if not message or not chat or chat.type != "private":
        if message:
            await message.reply_text(
                "🔒 <b>Esse comando só funciona no privado.</b>\n\n"
                "Me chama no PV e envie:\n"
                "<code>/recomendar</code>",
                parse_mode="HTML",
            )
        return

    await message.reply_text(
        "🎲 <b>Recomendação aleatória por gênero</b>\n\n"
        "Escolha um gênero abaixo e eu vou sortear um anime aleatório dele.",
        parse_mode="HTML",
        reply_markup=_genre_menu_keyboard(),
    )