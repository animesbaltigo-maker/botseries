import html
import re
from typing import Any

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import BOT_USERNAME
from utils.gatekeeper import ensure_channel_membership

# TROQUE ESSA IMPORT SE O NOME DA SUA FUNÇÃO DE BUSCA FOR OUTRO
# A ideia é usar a mesma fonte da /buscar
# from services.animefire_client import search_anime  # removido


TRACE_MOE_API = "https://api.trace.moe/search"
TRACE_MOE_ME_API = "https://api.trace.moe/me"

_HTTP_TIMEOUT = httpx.Timeout(45.0, connect=10.0, read=45.0, write=45.0)
_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

MIN_SIMILARITY = 0.90

TRACE_ALLOWED_CHAT_TYPES = {"private", "group", "supergroup"}


def _trace_chat_allowed(update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type in TRACE_ALLOWED_CHAT_TYPES)


def _seconds_to_hhmmss(seconds: float | int | None) -> str:
    try:
        total = int(float(seconds or 0))
    except Exception:
        total = 0

    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60

    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _clean_title(text: Any) -> str:
    text = str(text or "").strip()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("OVA", " OVA ").replace("ONA", " ONA ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _pick_title(anilist_data: Any, fallback_filename: str = "") -> str:
    if isinstance(anilist_data, dict):
        title = anilist_data.get("title") or {}
        picked = (
            title.get("english")
            or title.get("romaji")
            or title.get("native")
            or ""
        ).strip()
        if picked:
            return _clean_title(picked)

        synonyms = anilist_data.get("synonyms") or []
        if synonyms:
            return _clean_title(synonyms[0])

    fallback_filename = _clean_title(fallback_filename)
    return fallback_filename or "Anime desconhecido"


def _pick_secondary_title(anilist_data: Any, main_title: str) -> str:
    if not isinstance(anilist_data, dict):
        return ""

    title = anilist_data.get("title") or {}

    candidates = [
        title.get("romaji"),
        title.get("english"),
        title.get("native"),
    ]

    main_key = (main_title or "").strip().lower()

    for candidate in candidates:
        cleaned = _clean_title(candidate)
        if not cleaned:
            continue
        if cleaned.strip().lower() == main_key:
            continue
        return cleaned

    return ""


def _format_similarity(similarity: float) -> str:
    pct = round(similarity * 100, 2)

    if similarity >= 0.98:
        badge = "🔥"
    elif similarity >= 0.95:
        badge = "✅"
    else:
        badge = "⚠️"

    return f"{badge} <code>{pct}%</code>"


def _extract_episode_text(item: dict) -> str:
    episode = item.get("episode")
    if episode in (None, "", "null"):
        return "Não detectado"
    return str(episode)


def _build_pretty_caption(item: dict, bot_match: dict | None = None) -> str:
    anilist_data = item.get("anilist")
    filename = str(item.get("filename") or "").strip()

    title = _pick_title(anilist_data, filename)
    second_title = _pick_secondary_title(anilist_data, title)

    similarity = float(item.get("similarity") or 0.0)
    at_text = _seconds_to_hhmmss(item.get("at"))
    from_text = _seconds_to_hhmmss(item.get("from"))
    to_text = _seconds_to_hhmmss(item.get("to"))
    episode_text = _extract_episode_text(item)

    title_html = html.escape(title)
    if second_title:
        title_html += f"\n<i>{html.escape(second_title)}</i>"

    lines = [
        "🔎 <b>Cena encontrada</b>",
        "",
        f"🎬 <b>{title_html}</b>",
        "",
        f"🎯 <b>Similaridade:</b> {_format_similarity(similarity)}",
        f"📺 <b>Episódio:</b> <code>{html.escape(episode_text)}</code>",
        f"⏱️ <b>Momento:</b> <code>{html.escape(at_text)}</code>",
        f"🎞️ <b>Trecho:</b> <code>{html.escape(from_text)} - {html.escape(to_text)}</code>",
    ]

    if bot_match:
        lines.extend([
            "",
            "🤖 <b>Esse anime existe no bot</b>",
            f"📚 <b>No bot:</b> <code>{html.escape(bot_match.get('title', title))}</code>",
        ])

    return "\n".join(lines)


def _deep_link_anime(anime_id: str) -> str:
    anime_id = str(anime_id).strip()
    return f"https://t.me/{BOT_USERNAME}?start={anime_id}"


def _deep_link_episode(anime_id: str, episode: str | int | None) -> str:
    anime_id = str(anime_id).strip()
    episode = str(episode or "").strip()
    if not episode:
        return _deep_link_anime(anime_id)
    return f"https://t.me/{BOT_USERNAME}?start=ep_{anime_id}__{episode}"


def _pick_best_bot_result(results: list[dict], trace_title: str) -> dict | None:
    if not results:
        return None

    trace_key = _clean_title(trace_title).lower()

    def score(item: dict) -> tuple[int, int]:
        title = _clean_title(
            item.get("title_romaji")
            or item.get("title")
            or item.get("name")
            or ""
        ).lower()

        exact = int(title == trace_key)
        contains = int(trace_key in title or title in trace_key)
        return (exact, contains)

    results = sorted(results, key=score, reverse=True)
    return results[0] if results else None


async def _find_bot_anime_match(item: dict) -> dict | None:
    anilist_data = item.get("anilist")
    filename = str(item.get("filename") or "").strip()
    trace_title = _pick_title(anilist_data, filename)

    queries = [trace_title]

    if isinstance(anilist_data, dict):
        synonyms = anilist_data.get("synonyms") or []
        for synonym in synonyms[:2]:
            synonym = _clean_title(synonym)
            if synonym and synonym not in queries:
                queries.append(synonym)

    for query in queries:
        try:
            results = await search_anime(query, limit=5)
        except TypeError:
            results = await search_anime(query)
        except Exception:
            results = []

        if results:
            best = _pick_best_bot_result(results, trace_title)
            if best:
                return best

    return None


def _build_keyboard(item: dict, bot_match: dict | None = None) -> InlineKeyboardMarkup | None:
    rows = []

    if bot_match:
        anime_id = (
            bot_match.get("id")
            or bot_match.get("anime_id")
            or bot_match.get("slug")
        )

        if anime_id:
            episode = item.get("episode")

            rows.append([
                InlineKeyboardButton(
                    "▶️ Ver no bot",
                    url=_deep_link_episode(anime_id, episode),
                )
            ])

            rows.append([
                InlineKeyboardButton(
                    "📚 Abrir anime",
                    url=_deep_link_anime(anime_id),
                )
            ])

    video_url = str(item.get("video") or "").strip()
    image_url = str(item.get("image") or "").strip()

    extra_buttons = []
    if video_url:
        extra_buttons.append(InlineKeyboardButton("🎬 Prévia", url=video_url))
    if image_url:
        extra_buttons.append(InlineKeyboardButton("🖼️ Frame", url=image_url))

    if extra_buttons:
        rows.append(extra_buttons)

    anilist_data = item.get("anilist")
    if isinstance(anilist_data, dict):
        anime_id = anilist_data.get("id")
        if anime_id:
            rows.append([
                InlineKeyboardButton(
                    "📖 AniList",
                    url=f"https://anilist.co/anime/{anime_id}",
                )
            ])

    return InlineKeyboardMarkup(rows) if rows else None


async def _trace_search_bytes(file_bytes: bytes, mime_type: str | None = None) -> dict:
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, headers=_HTTP_HEADERS) as client:
        response = await client.post(
            f"{TRACE_MOE_API}?anilistInfo&cutBorders",
            files={"image": ("image", file_bytes, mime_type or "application/octet-stream")},
        )
        response.raise_for_status()
        return response.json()


async def _trace_me() -> dict:
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, headers=_HTTP_HEADERS) as client:
        response = await client.get(TRACE_MOE_ME_API)
        response.raise_for_status()
        return response.json()


async def traceme(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _trace_chat_allowed(update):
        return

    if not await ensure_channel_membership(update, context):
        return

    message = update.effective_message
    if not message:
        return

    context.user_data["trace_waiting_photo"] = True

    await message.reply_text(
        "🔎 <b>Me envie uma foto do anime</b>\n\n"
        "Agora me envie, <b>pode ser print, frame ou cena.</b>.\n"
        "Eu vou tentar achar o anime e, se ele existir no bot, já vou te mandar o botão pra abrir.",
        parse_mode="HTML",
    )


async def tracequota(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _trace_chat_allowed(update):
        return

    if not await ensure_channel_membership(update, context):
        return

    message = update.effective_message
    if not message:
        return

    msg = await message.reply_text(
        "📊 <b>Consultando limite...</b>",
        parse_mode="HTML",
    )

    try:
        data = await _trace_me()

        await msg.edit_text(
            "📊 <b>Status do trace.moe</b>\n\n"
            f"👤 <b>ID:</b> <code>{html.escape(str(data.get('id', 'N/A')))}</code>\n"
            f"📦 <b>Quota:</b> <code>{data.get('quota', 'N/A')}</code>\n"
            f"📉 <b>Usado:</b> <code>{data.get('quotaUsed', 'N/A')}</code>\n"
            f"⚡ <b>Concorrência:</b> <code>{data.get('concurrency', 'N/A')}</code>\n"
            f"🏁 <b>Prioridade:</b> <code>{data.get('priority', 'N/A')}</code>",
            parse_mode="HTML",
        )
    except Exception as e:
        await msg.edit_text(
            "❌ <b>Não consegui achar.</b>\n"
            f"<code>{html.escape(repr(e))}</code>",
            parse_mode="HTML",
        )


async def trace_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _trace_chat_allowed(update):
        return

    if not await ensure_channel_membership(update, context):
        return

    message = update.effective_message
    if not message or not message.photo:
        return

    waiting_mode = context.user_data.get("trace_waiting_photo", False)
    if not waiting_mode:
        return

    context.user_data["trace_waiting_photo"] = False

    status = await message.reply_text(
        "🔎 <b>Procurando essa cena...</b>",
        parse_mode="HTML",
    )

    try:
        photo = message.photo[-1]
        tg_file = await photo.get_file()
        file_bytes = await tg_file.download_as_bytearray()

        if len(file_bytes) > 25 * 1024 * 1024:
            await status.edit_text(
                "❌ <b>A imagem passou de 25 MB.</b>\n"
                "Envie uma imagem menor.",
                parse_mode="HTML",
            )
            return

        data = await _trace_search_bytes(bytes(file_bytes), mime_type="image/jpeg")

        error_text = str(data.get("error") or "").strip()
        results = data.get("result") or []

        if error_text:
            await status.edit_text(
                f"❌ <b>Erro:</b>\n<code>{html.escape(error_text)}</code>",
                parse_mode="HTML",
            )
            return

        if not results:
            await status.edit_text(
                "🚫 <b>Não encontrei resultado para essa cena.</b>",
                parse_mode="HTML",
            )
            return

        top = results[0]
        similarity = float(top.get("similarity") or 0.0)

        if similarity < MIN_SIMILARITY:
            await status.edit_text(
                "⚠️ <b>Encontrei algo, mas a confiança ficou baixa.</b>\n\n"
                f"🎯 <b>Similaridade:</b> <code>{round(similarity * 100, 2)}%</code>\n"
                "Tenta mandar um frame mais limpo, sem blur e sem cortes.",
                parse_mode="HTML",
            )
            return

        bot_match = await _find_bot_anime_match(top)
        caption = _build_pretty_caption(top, bot_match=bot_match)
        keyboard = _build_keyboard(top, bot_match=bot_match)
        preview_image = str(top.get("image") or "").strip()

        try:
            await status.delete()
        except Exception:
            pass

        if preview_image:
            await message.reply_photo(
                photo=preview_image,
                caption=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        else:
            await message.reply_text(
                caption,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

    except httpx.HTTPStatusError as e:
        code = e.response.status_code if e.response else 0

        if code == 402:
            text = (
                "⚠️ <b>Limite temporário atingido.</b>\n"
                "Tenta de novo em alguns instantes."
            )
        elif code == 413:
            text = (
                "❌ <b>A imagem está grande demais.</b>\n"
                "Envie uma menor que 25 MB."
            )
        elif code in (503, 504):
            text = (
                "⚠️ <b>O bot está sobrecarregado agora.</b>\n"
                "Tente novamente daqui a pouco."
            )
        else:
            text = (
                "❌ <b>Falha ao consultar.</b>\n"
                f"<code>HTTP {code}</code>"
            )

        await status.edit_text(text, parse_mode="HTML")

    except Exception as e:
        await status.edit_text(
            "❌ <b>Não consegui analisar essa imagem.</b>\n"
            f"<code>{html.escape(repr(e))}</code>",
            parse_mode="HTML",
        )
