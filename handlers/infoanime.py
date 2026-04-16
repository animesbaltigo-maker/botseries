import html

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from utils.gatekeeper import ensure_channel_membership

ANILIST_API = "https://graphql.anilist.co"

_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Content-Type": "application/json",
}


def _pick_title(media: dict) -> str:
    title = media.get("title") or {}
    return (
        title.get("english")
        or title.get("romaji")
        or title.get("native")
        or "Sem título"
    )


def _format_date(start_date: dict) -> str:
    if not start_date:
        return "N/A"

    day = start_date.get("day") or "?"
    month = start_date.get("month") or "?"
    year = start_date.get("year") or "?"
    return f"{day}/{month}/{year}"


async def buscar_multiplos_anilist(nome: str) -> list[dict]:
    query = """
    query ($search: String) {
      Page(perPage: 6) {
        media(search: $search, type: ANIME) {
          id
          siteUrl
          title { romaji english native }
          status
          averageScore
          startDate { day month year }
          genres
          trailer { site id }
        }
      }
    }
    """

    payload = {
        "query": query,
        "variables": {"search": nome},
    }

    try:
        async with httpx.AsyncClient(timeout=10, headers=_HTTP_HEADERS) as client:
            resp = await client.post(ANILIST_API, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return (((data or {}).get("data") or {}).get("Page") or {}).get("media", []) or []
    except Exception as e:
        print("Erro AniList:", repr(e))
        return []


async def buscar_anilist_por_id(anime_id: int) -> dict:
    query = """
    query ($id: Int) {
      Media(id: $id, type: ANIME) {
        id
        siteUrl
        title { romaji english native }
        status
        averageScore
        startDate { day month year }
        genres
        trailer { site id }
      }
    }
    """

    payload = {
        "query": query,
        "variables": {"id": anime_id},
    }

    try:
        async with httpx.AsyncClient(timeout=10, headers=_HTTP_HEADERS) as client:
            resp = await client.post(ANILIST_API, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return (((data or {}).get("data") or {}).get("Media")) or {}
    except Exception as e:
        print("Erro AniList por ID:", repr(e))
        return {}


async def infoanime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return

    if not context.args:
        await update.message.reply_text(
            "❌ <b>Faltou o nome do anime.</b>\n\n"
            "Use assim:\n"
            "<code>/infoanime nome do anime</code>\n\n"
            "📌 <b>Exemplo:</b>\n"
            "<code>/infoanime Naruto</code>",
            parse_mode="HTML",
        )
        return

    nome = " ".join(context.args).strip()

    msg = await update.message.reply_text(
        "🔎 <b>Buscando no AniList...</b>",
        parse_mode="HTML",
    )

    resultados = await buscar_multiplos_anilist(nome)

    if not resultados:
        await msg.edit_text(
            "🚫 <b>Não encontrei nenhum anime com esse nome.</b>",
            parse_mode="HTML",
        )
        return

    botoes = []
    for media in resultados:
        titulo = _pick_title(media)
        if len(titulo) > 45:
            titulo = titulo[:42].rstrip() + "..."
        botoes.append([
            InlineKeyboardButton(
                titulo,
                callback_data=f"info_anime:{media['id']}",
            )
        ])

    await msg.edit_text(
        "📌 <b>Encontrei algumas versões</b>\n\n"
        "Escolha qual você quer ver:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(botoes),
    )


async def callback_info_anime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        anime_id = int(query.data.split(":", 1)[1])
    except Exception:
        await query.answer("Anime inválido.", show_alert=True)
        return

    media = await buscar_anilist_por_id(anime_id)
    if not media:
        await query.answer("Não consegui carregar esse anime.", show_alert=True)
        return

    titulo = html.escape(_pick_title(media))
    score = media.get("averageScore") or "N/A"
    status = html.escape(str(media.get("status") or "N/A"))
    genres = media.get("genres") or []
    genres_text = html.escape(", ".join(genres) if genres else "N/A")
    start_date = _format_date(media.get("startDate") or {})

    texto = (
        f"<b>{titulo}</b>\n\n"
        f"<b>Pontuação:</b> <code>{score}</code>\n"
        f"<b>Situação:</b> <code>{status}</code>\n"
        f"<b>Gênero:</b> <code>{genres_text}</code>\n"
        f"<b>Lançamento:</b> <code>{start_date}</code>"
    )

    imagem = f"https://img.anili.st/media/{media['id']}"

    botoes = []
    trailer = media.get("trailer")
    if trailer and trailer.get("site", "").lower() == "youtube" and trailer.get("id"):
        botoes.append([
            InlineKeyboardButton(
                "🎬 Trailer",
                url=f"https://www.youtube.com/watch?v={trailer['id']}",
            )
        ])

    if media.get("siteUrl"):
        botoes.append([
            InlineKeyboardButton(
                "📖 Ver no AniList",
                url=media["siteUrl"],
            )
        ])

    try:
        await query.message.delete()
    except Exception:
        pass

    await context.bot.send_photo(
        chat_id=query.message.chat.id,
        photo=imagem,
        caption=texto,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(botoes) if botoes else None,
    )