from telegram import Update
from telegram.ext import ContextTypes

from config import BOT_BRAND


async def ajuda(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    username = context.bot.username or "SeuBot"
    text = (
        f"❓ <b>Ajuda - {BOT_BRAND}</b>\n\n"
        "<b>Comandos principais</b>\n"
        "• <code>/buscar nome</code> - busca filmes e series\n"
        "• <code>/lancamentos</code> - mostra novidades recentes\n"
        "• <code>/aleatorio</code> - escolhe um titulo para voce\n"
        "• <code>/pedido</code> - abre a central de pedidos\n"
        "• <code>/indicacoes</code> - mostra seu link e ranking\n\n"
        "<b>Dicas</b>\n"
        "• Toque nos botoes para abrir detalhes, temporadas e players\n"
        "• Em grupos, chame <code>akira</code> para pedir sugestoes de filmes e series\n"
        f"• Inline tambem funciona: <code>@{username} nome do titulo</code>"
    )

    await message.reply_text(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
