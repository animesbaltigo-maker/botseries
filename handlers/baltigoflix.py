from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes


BOT_PRIVATE_URL = "https://t.me/SourceBaltigo_Bot"
MINI_APP_URL = "https://bot-production-1980.up.railway.app/baltigoflix"
BALTIGOFLIX_BANNER_URL = (
    "https://photo.chelpbot.me/AgACAgEAAxkBaDfI-2m66g4WQ-Jj6FZRPjNKhpCO_4kNAAIXrzEbj2ehRbC9NWdU_qoOAQADAgADeQADOgQ/photo.jpg"
)


def _is_group(update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type in ("group", "supergroup"))


async def baltigoflix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message

    # 🔒 BLOQUEIA EM GRUPO
    if _is_group(update):
        texto = (
            "🎬 <b>BaltigoFlix disponível apenas no privado</b>\n\n"
            "Para conhecer os <b>planos</b> e acessar a área premium, "
            "use este comando no <b>chat privado</b>.\n\n"
            "✨ Toque abaixo para abrir:"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Abrir no privado", url=BOT_PRIVATE_URL)]
        ])

        if msg:
            await msg.reply_html(texto, reply_markup=kb)
        return

    # 🎬 TEXO BONITO
    texto = (
        "🎬 <b>BaltigoFlix</b> 📺\n\n"
        "Chegou o <b>BaltigoFlix</b>, a forma definitiva de assistir tudo em um só lugar.\n\n"
        "✨ <b>O que você ganha:</b>\n"
        "✅ Mais de <b>2.000 canais</b>\n"
        "✅ <b>Netflix, Disney+, HBO Max</b> e muito mais\n"
        "✅ Qualidade alta e <b>sem travamentos</b>\n"
        "✅ Suporte dedicado + preço acessível\n\n"
        "🍿 Filmes, séries, animes e TV ao vivo sem limites.\n"
        "Tudo isso direto no seu celular.\n\n"
        "🚀 <b>Toque abaixo e comece agora.</b>"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Abrir BaltigoFlix", web_app=WebAppInfo(url=MINI_APP_URL))]
    ])

    if msg:
        await msg.reply_photo(
            photo=BALTIGOFLIX_BANNER_URL,
            caption=texto,
            parse_mode="HTML",
            reply_markup=kb,
        )