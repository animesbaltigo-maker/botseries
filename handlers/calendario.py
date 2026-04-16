from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

BANNER_CALENDARIO = "https://photo.chelpbot.me/AgACAgEAAxkBaMZsyGnDxObzi0-Ptj-LLpuaLvpTPsDyAAIWDGsbD28gRplj8RPKpDtoAQADAgADeQADOgQ/photo.jpg"

async def calendario(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📅 <b>Lançamentos da Temporada</b>\n\n"
        "Fique por dentro dos episódios que estão saindo agora.\n\n"
        "🎬 <b>Animes em lançamento</b>\n"
        "⏰ Atualizado em tempo real\n"
        "🇧🇷 Horário de Brasília\n\n"
        "Escolha uma opção abaixo 👇"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📅 Ver calendário", url="https://anichart.net")
        ],
        [
            InlineKeyboardButton("🔔 Receber atualizações", url="https://t.me/AtualizacoesOn")
        ]
    ])

    await update.message.reply_photo(
        photo=BANNER_CALENDARIO,
        caption=text,
        parse_mode="HTML",
        reply_markup=keyboard
    )