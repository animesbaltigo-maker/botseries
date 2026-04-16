"""
Comando /postanime — admin posta uma série no canal.
Variação:
- /postanime nome da série           -> canal normal
- /postanime d nome da série         -> canal de desenhos
"""
import html

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import (
    ADMIN_IDS,
    CANAL_POSTAGEM,
    CANAL_POSTAGEM_DESENHOS,
    STICKER_DIVISOR,
)
from services.catalog_client import get_content_details, get_seasons, search_content
from services.start_payloads import build_start_link, create_start_payload


def _is_admin(user_id: int | None) -> bool:
    return bool(user_id in ADMIN_IDS) if user_id is not None else False


def _content_audio_label(detail: dict) -> str:
    audio = str(detail.get("audio") or "").strip()
    if audio:
        return audio

    audio_options = [
        str(option).strip().lower()
        for option in (detail.get("audio_options") or [])
        if str(option).strip()
    ]

    labels = []
    if "dublado" in audio_options:
        labels.append("Dublado")
    if "legendado" in audio_options:
        labels.append("Legendado")

    if labels:
        return " | ".join(labels)

    default_audio = str(detail.get("default_audio") or "").strip().lower()
    if default_audio == "dublado":
        return "Dublado"
    if default_audio == "legendado":
        return "Legendado"

    return "Não informado"


def _default_audio_key(detail: dict) -> str:
    default_audio = str(detail.get("default_audio") or "").strip().lower()
    if default_audio in {"dublado", "legendado"}:
        return default_audio

    audio_options = [
        str(option).strip().lower()
        for option in (detail.get("audio_options") or [])
        if str(option).strip()
    ]
    if "legendado" in audio_options:
        return "legendado"
    if "dublado" in audio_options:
        return "dublado"

    audio_text = str(detail.get("audio") or "").strip().lower()
    if "dub" in audio_text:
        return "dublado"

    return "legendado"


def _build_caption(detail: dict, seasons: list) -> str:
    title = html.escape((detail.get("title") or "Sem título").strip())
    year = html.escape(str(detail.get("year") or "").strip())
    duration = html.escape(str(detail.get("duration") or "").strip())
    rating = html.escape(str(detail.get("rating") or "").strip())
    genres = [str(genre).strip() for genre in (detail.get("genres") or []) if str(genre).strip()]
    genres_text = html.escape(" | ".join(genres[:4])) if genres else "Não informado"
    description = html.escape((detail.get("description") or "Sem sinopse.").strip()[:500])
    seasons_count = len(seasons or [])
    audio_text = html.escape(_content_audio_label(detail))

    type_bits = ["Série"]
    if year:
        type_bits.append(year)
    if duration:
        type_bits.append(duration)

    quote_lines = [f"<b>Tipo:</b> {' | '.join(type_bits)}"]
    if rating:
        quote_lines.append(f"<b>Nota:</b> {rating}")
    quote_lines.append(f"<b>Áudio:</b> {audio_text}")
    quote_lines.append(f"<b>Temporadas:</b> {seasons_count}")
    quote_lines.append(f"<b>Gêneros:</b> {genres_text}")
    quote_text = "\n".join(quote_lines)

    return (
        f"📺 <b>{title}</b>\n\n"
        f"<blockquote>{quote_text}</blockquote>\n\n"
        f"💬 <i>{description}</i>"
    )


def _build_payload_item(detail: dict, chosen: dict) -> dict:
    selected_audio = _default_audio_key(detail)

    return {
        "id": chosen.get("id"),
        "title": detail.get("title") or chosen.get("title") or "",
        "year": detail.get("year") or chosen.get("year") or "",
        "url": str(detail.get("url") or chosen.get("url") or "").strip(),
        "image": str(detail.get("image") or chosen.get("image") or "").strip(),
        "type": detail.get("type") or chosen.get("type") or "series",
        "is_dubbed": selected_audio == "dublado",
        "audio_urls": chosen.get("audio_urls") or {},
        "audio_options": detail.get("audio_options") or chosen.get("audio_options") or [],
        "default_audio": detail.get("default_audio") or selected_audio,
    }


def _build_keyboard(
    context: ContextTypes.DEFAULT_TYPE,
    detail: dict,
    chosen: dict,
) -> InlineKeyboardMarkup:
    bot_username = getattr(context.bot, "username", "") or ""
    if not bot_username:
        return InlineKeyboardMarkup([])

    payload_item = _build_payload_item(detail, chosen)
    token = create_start_payload(
        {
            "source": "postanime",
            "query": payload_item.get("title") or "",
            "selected_audio": payload_item.get("default_audio") or "legendado",
            "item": payload_item,
        }
    )
    deep_link = build_start_link(bot_username, token)

    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("▶️ Assistir no bot", url=deep_link)]]
    )


def _looks_like_series(result: dict) -> bool:
    result_type = str(result.get("type") or "").strip().lower()
    if result_type == "series":
        return True

    url = str(result.get("url") or "").strip().lower()
    return any(token in url for token in ("serie", "series", "-u1-"))


async def _send_separator_sticker(
    context: ContextTypes.DEFAULT_TYPE,
    destination,
) -> None:
    try:
        await context.bot.send_sticker(chat_id=destination, sticker=STICKER_DIVISOR)
    except Exception as exc:
        print("ERRO STICKER DIVISOR:", repr(exc))


def _resolve_destination(args: list[str]) -> tuple[str, str, bool]:
    if args and str(args[0]).strip().lower() == "d":
        query = " ".join(args[1:]).strip()
        return CANAL_POSTAGEM_DESENHOS, query, True

    query = " ".join(args).strip()
    return CANAL_POSTAGEM, query, False


async def postanime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text("❌ <b>Sem permissão.</b>", parse_mode="HTML")
        return

    if not context.args:
        await message.reply_text(
            "❌ <b>Use assim:</b>\n"
            "<code>/postanime nome da série</code>\n"
            "<code>/postanime d nome da série</code>",
            parse_mode="HTML",
        )
        return

    destination, query, is_desenho = _resolve_destination(context.args)

    if not query:
        await message.reply_text(
            "❌ <b>Faltou o nome da série.</b>",
            parse_mode="HTML",
        )
        return

    if is_desenho and not destination:
        await message.reply_text(
            "❌ <b>CANAL_POSTAGEM_DESENHOS não configurado.</b>",
            parse_mode="HTML",
        )
        return

    status = await message.reply_text(
        "⏳ <b>Buscando no catálogo...</b>\n<i>Aguarde um instante.</i>",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

    try:
        results = await search_content(query)

        series_results = [result for result in results if _looks_like_series(result)]
        chosen = series_results[0] if series_results else (results[0] if results else None)

        if not chosen:
            await status.edit_text(
                "❌ <b>Série não encontrada.</b>\n\n<i>Tente pesquisar com outro nome.</i>",
                parse_mode="HTML",
            )
            return

        content_url = str(chosen.get("url") or "").strip()
        detail = await get_content_details(content_url)
        seasons = await get_seasons(content_url)

        caption = _build_caption(detail, seasons)
        keyboard = _build_keyboard(context, detail, chosen)
        image = str(detail.get("image") or "").strip()

        if image:
            await context.bot.send_photo(
                chat_id=destination,
                photo=image,
                caption=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        else:
            await context.bot.send_message(
                chat_id=destination,
                text=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

        await _send_separator_sticker(context, destination)

        posted_title = html.escape(str(detail.get("title") or query))
        channel_label = "canal de desenhos" if is_desenho else "canal normal"

        await status.edit_text(
            f"✅ <b>Série postada com sucesso no {channel_label}!</b>\n<code>{posted_title}</code>",
            parse_mode="HTML",
        )

    except Exception as exc:
        print("ERRO POSTANIME:", repr(exc))
        await status.edit_text(
            "❌ <b>Erro ao postar a série.</b>\n\n<i>Tente novamente em instantes.</i>",
            parse_mode="HTML",
        )


async def postserie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await postanime(update, context)