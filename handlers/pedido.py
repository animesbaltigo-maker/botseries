from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

from config import BOT_BRAND, BOT_USERNAME
from utils.gatekeeper import ensure_channel_membership


def _is_group(update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type in ("group", "supergroup"))


async def pedido(update, context):
    if not await ensure_channel_membership(update, context):
        return

    msg = update.effective_message
    if not msg:
        return

    mini_app_url = "https://bot-production-1980.up.railway.app/pedido"
    bot_private_url = f"https://t.me/{BOT_USERNAME}"
    pedido_banner_url = (
        "https://photo.chelpbot.me/AgACAgEAAxkBZ0w54WmrME4Fk9ObOXCy_CjgTb8IHF9cAAJRC2sb1ZFYRTRdgJDi4ysfAQADAgADeQADOgQ/photo.jpg"
    )

    if _is_group(update):
        texto = (
            f"📩 <b>Central de Pedidos — {BOT_BRAND}</b>\n\n"
            "Esse comando funciona apenas no <b>privado</b>.\n\n"
            "Por lá você pode pedir novos <b>filmes/séries</b>, enviar sugestões e reportar erros.\n\n"
            "👇 <b>Toque no botão abaixo para abrir o bot no privado:</b>"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Abrir no privado", url=bot_private_url)]
        ])

        await msg.reply_text(texto, parse_mode="HTML", reply_markup=kb)
        return

    texto = (
        f"📩 <b>Central de Pedidos — {BOT_BRAND}</b>\n\n"
        "Peça novos <b>filmes e séries</b>, envie sugestões ou reporte algum <b>erro</b> em um só lugar.\n\n"
        "👇 <b>Toque no botão abaixo para abrir a central:</b>"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Abrir Central de Pedidos", web_app=WebAppInfo(url=mini_app_url))]
    ])

    await msg.reply_photo(
        photo=pedido_banner_url,
        caption=texto,
        parse_mode="HTML",
        reply_markup=kb,
    )
