from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import ContextTypes

MINIAPP_URL = "https://alberta-utah-living-home.trycloudflare.com/miniapp/index.html"


async def testminiapp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user

    if message is None:
        return

    first_name = user.first_name if user and user.first_name else "pirata"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="📱 Abrir Mini App",
                    web_app=WebAppInfo(url=MINIAPP_URL),
                )
            ]
        ]
    )

    text = (
        f"🎌 <b>Baltigo Mini App</b>\n\n"
        f"Fala, {first_name}.\n"
        f"Clica no botão abaixo pra testar o Mini App dentro do Telegram."
    )

    await message.reply_text(
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )
