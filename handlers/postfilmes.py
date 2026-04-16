"""
Comando /postfilme — admin posta um filme no canal.
"""
import html
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS, CANAL_POSTAGEM, STICKER_DIVISOR
from services.catalog_client import search_content, get_content_details


def _is_admin(user_id):
    return user_id in ADMIN_IDS


def _build_caption(detail: dict) -> str:
    title = html.escape((detail.get("title") or "Sem título").upper())
    year = html.escape(detail.get("year") or "")
    duration = html.escape(detail.get("duration") or "")
    audio = html.escape(detail.get("audio") or "")
    genres = detail.get("genres") or []
    genres_str = html.escape(", ".join(f"#{g}" for g in genres[:4])) if genres else "N/A"
    desc = html.escape((detail.get("description") or "Sem sinopse.")[:400])
    director = html.escape(detail.get("director") or "")

    parts = [f"🎬 <b>{title}</b>"]
    if year:
        parts.append(f"📅 <b>Ano:</b> <i>{year}</i>")
    if duration:
        parts.append(f"⏱️ <b>Duração:</b> <i>{duration}</i>")
    parts.append(f"🎙️ <b>Áudio:</b> <i>{audio}</i>")
    parts.append(f"🎭 <b>Gêneros:</b> <i>{genres_str}</i>")
    if director:
        parts.append(f"🎥 <b>Diretor:</b> <i>{director}</i>")
    parts.append("")
    parts.append(f"📝 {desc}")

    return "\n".join(parts)


def _build_keyboard(detail: dict) -> InlineKeyboardMarkup:
    url = detail.get("url", "")
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("▶️ Assistir agora", url=url)
    ]])


async def postfilmes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not _is_admin(user_id):
        await message.reply_text("❌ <b>Sem permissão.</b>", parse_mode="HTML")
        return

    if not context.args:
        await message.reply_text(
            "❌ Use: <code>/postfilme nome do filme</code>",
            parse_mode="HTML",
        )
        return

    query = " ".join(context.args).strip()
    msg = await message.reply_text("📤 <b>Buscando filme...</b>", parse_mode="HTML")

    try:
        results = await search_content(query)
        # Filtra só filmes (sem séries)
        filmes = [r for r in results if "serie" not in r.get("url", "").lower() and "-u1-" not in r.get("url","")]
        if not filmes:
            filmes = results

        if not filmes:
            await msg.edit_text("❌ <b>Filme não encontrado.</b>", parse_mode="HTML")
            return

        detail = await get_content_details(filmes[0]["url"])
        caption = _build_caption(detail)
        keyboard = _build_keyboard(detail)
        image = detail.get("image") or ""

        if image:
            await context.bot.send_photo(
                chat_id=CANAL_POSTAGEM,
                photo=image,
                caption=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        else:
            await context.bot.send_message(
                chat_id=CANAL_POSTAGEM,
                text=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

        await context.bot.send_sticker(chat_id=CANAL_POSTAGEM, sticker=STICKER_DIVISOR)
        await msg.edit_text(
            f"✅ <b>Filme postado!</b>\n<code>{detail.get('title', query)}</code>",
            parse_mode="HTML",
        )

    except Exception as e:
        print("ERRO POSTFILME:", repr(e))
        await msg.edit_text("❌ <b>Erro ao postar o filme.</b>", parse_mode="HTML")
