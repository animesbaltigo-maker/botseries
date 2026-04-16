import asyncio
import html
import re
import secrets
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from config import SEARCH_PAGE_SIZE, SEARCH_SESSION_TTL_SECONDS
from services.metrics import log_event, mark_user_seen
from services.catalog_client import search_content
from utils.gatekeeper import ensure_channel_membership

RESULTS_PER_PAGE = max(4, SEARCH_PAGE_SIZE)
SEARCH_COOLDOWN = 1.5
SEARCH_INFLIGHT_TTL = 12.0
SEARCH_TIMEOUT = 20.0

_SEARCH_USER_LOCKS: dict[int, asyncio.Lock] = {}
_SEARCH_INFLIGHT: dict[str, float] = {}


def _now() -> float:
    return time.monotonic()


def _normalize_query(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _pretty_query(text: str) -> str:
    normalized = _normalize_query(text)
    if not normalized:
        return ""
    return normalized[:1].upper() + normalized[1:]


def _is_search_cooldown(context: ContextTypes.DEFAULT_TYPE, user_id: int, query: str) -> bool:
    now = _now()
    last_ts = context.user_data.get(f"search_last:{user_id}", 0.0)
    last_query = context.user_data.get(f"search_query:{user_id}", "")
    if query and last_query == query and (now - last_ts) < SEARCH_COOLDOWN:
        return True
    context.user_data[f"search_last:{user_id}"] = now
    context.user_data[f"search_query:{user_id}"] = query
    return False


def _is_inflight(user_id: int, query: str) -> bool:
    key = f"{user_id}:{query.lower()}"
    item = _SEARCH_INFLIGHT.get(key)
    if not item:
        return False
    if _now() - item > SEARCH_INFLIGHT_TTL:
        _SEARCH_INFLIGHT.pop(key, None)
        return False
    return True


def _set_inflight(user_id: int, query: str) -> None:
    _SEARCH_INFLIGHT[f"{user_id}:{query.lower()}"] = _now()


def _clear_inflight(user_id: int, query: str) -> None:
    _SEARCH_INFLIGHT.pop(f"{user_id}:{query.lower()}", None)


def _user_lock(user_id: int) -> asyncio.Lock:
    lock = _SEARCH_USER_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _SEARCH_USER_LOCKS[user_id] = lock
    return lock


def _session_key(token: str) -> str:
    return f"search_session:{token}"


def _prune_search_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = _now()
    to_delete: list[str] = []
    for key, value in list(context.user_data.items()):
        if not key.startswith("search_session:") or not isinstance(value, dict):
            continue
        created_at = float(value.get("created_at") or 0.0)
        if now - created_at > SEARCH_SESSION_TTL_SECONDS:
            to_delete.append(key)
    for key in to_delete:
        context.user_data.pop(key, None)


def get_search_session(context: ContextTypes.DEFAULT_TYPE, token: str) -> dict:
    _prune_search_sessions(context)
    return context.user_data.get(_session_key(token)) or {}


def _item_media_emoji(item: dict) -> str:
    item_type = str(item.get("type") or "").strip().lower()
    candidate_urls = [str(item.get("url") or "").strip().lower()]
    audio_urls = item.get("audio_urls") or {}
    if isinstance(audio_urls, dict):
        candidate_urls.extend(str(value or "").strip().lower() for value in audio_urls.values())

    if item_type == "series":
        return "📺"
    for url in candidate_urls:
        if "series" in url or "-u1-" in url or "-u2-" in url or "-u3-" in url or re.search(r"-\d+x\d+(?:-|/|$)", url):
            return "📺"
    return "🎬"


def _build_button_title(item: dict) -> str:
    title = (item.get("title") or "Sem título").strip()
    badge = _item_media_emoji(item)
    year = f" ({item['year']})" if item.get("year") else ""
    return f"{badge} {title}{year}"


def _build_search_text(query: str, page: int, total: int, heading: str = "Resultado da busca") -> str:
    total_pages = max(1, ((total - 1) // RESULTS_PER_PAGE) + 1)
    safe_query = html.escape(_pretty_query(query))
    safe_heading = html.escape((heading or "Resultado da busca").strip())
    return (
        f"🔎 <b>{safe_heading}</b>\n\n"
        "<blockquote>"
        f"🎬 <b>Pesquisa:</b> <i>{safe_query}</i>\n"
        f"📚 <b>Resultados:</b> <i>{total}</i>\n"
        f"📄 <b>Página:</b> <i>{page}/{total_pages}</i>"
        "</blockquote>\n\n"
        "<i>Toque em um título para ver os detalhes e assistir.</i>"
    )


def _store_search_session(
    context: ContextTypes.DEFAULT_TYPE,
    query: str,
    results: list,
    *,
    heading: str = "Resultado da busca",
) -> str:
    _prune_search_sessions(context)
    token = secrets.token_hex(4)
    context.user_data[_session_key(token)] = {
        "query": query,
        "results": list(results),
        "heading": heading,
        "created_at": _now(),
    }
    return token


def _build_results_keyboard(results: list, page: int, total: int, token: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    start = (page - 1) * RESULTS_PER_PAGE
    end = start + RESULTS_PER_PAGE
    page_items = results[start:end]

    for index, item in enumerate(page_items, start=start + 1):
        title = _build_button_title(item)
        if len(title) > 45:
            title = title[:42].rstrip() + "..."
        rows.append(
            [
                InlineKeyboardButton(
                    f"{index}. {title}",
                    callback_data=f"pb_item|{token}|{index - 1}|{page}",
                )
            ]
        )

    nav: list[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️ Anterior", callback_data=f"pb_page|{token}|{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page}/{max(1, ((total - 1) // RESULTS_PER_PAGE) + 1)}", callback_data="noop"))
    if end < total:
        nav.append(InlineKeyboardButton("Próxima ➡️", callback_data=f"pb_page|{token}|{page + 1}"))
    if nav:
        rows.append(nav)

    return InlineKeyboardMarkup(rows)


async def _safe_delete(message) -> None:
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


async def _safe_edit(message, text: str, *, reply_markup=None) -> bool:
    if not message:
        return False
    try:
        await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
        return True
    except TelegramError:
        return False


async def buscar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_channel_membership(update, context):
        return

    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message
    if not message or not user:
        return

    mark_user_seen(user.id, user.username or user.first_name or "")

    if not chat or chat.type != "private":
        await message.reply_text(
            "🔒 <b>Esse comando só funciona no privado.</b>\n\n"
            "Me chama no PV e envie:\n"
            "<code>/buscar nome da série ou filme</code>",
            parse_mode="HTML",
        )
        return

    if not context.args:
        await message.reply_text(
            "🔎 <b>Como buscar</b>\n\n"
            "Envie:\n"
            "<code>/buscar nome da série ou filme</code>\n\n"
            "📌 <b>Exemplos</b>\n"
            "• <code>/buscar the boys</code>\n"
            "• <code>/buscar breaking bad</code>\n"
            "• <code>/buscar avatar</code>",
            parse_mode="HTML",
        )
        return

    query = _normalize_query(" ".join(context.args))
    if not query or len(query) < 2:
        await message.reply_text("⚠️ <b>Digite pelo menos 2 caracteres.</b>", parse_mode="HTML")
        return

    if len(query) > 80:
        query = query[:80].rstrip()

    if _is_search_cooldown(context, user.id, query):
        await message.reply_text(
            "⏳ <b>Aguarde um instante antes de repetir essa busca.</b>",
            parse_mode="HTML",
        )
        return

    if _is_inflight(user.id, query):
        await message.reply_text(
            "⏳ <b>Essa busca já está sendo processada.</b>",
            parse_mode="HTML",
        )
        return

    lock = _user_lock(user.id)

    async with lock:
        if _is_inflight(user.id, query):
            await message.reply_text(
                "⏳ <b>Essa busca já está sendo processada.</b>",
                parse_mode="HTML",
            )
            return

        _set_inflight(user.id, query)
        loading = await message.reply_text(
            "🔎 <b>Buscando no catálogo...</b>\n"
            "<i>Vou montar os melhores resultados para você.</i>",
            parse_mode="HTML",
        )

        try:
            results = await asyncio.wait_for(search_content(query), timeout=SEARCH_TIMEOUT)

            log_event(
                event_type="search",
                user_id=user.id,
                username=user.username or user.first_name or "",
                query_text=query,
                result_count=len(results),
            )

            if not results:
                log_event(
                    event_type="search_no_result",
                    user_id=user.id,
                    username=user.username or user.first_name or "",
                    query_text=query,
                    result_count=0,
                )
                await _safe_edit(
                    loading,
                    "❌ <b>Nenhum resultado encontrado.</b>\n\n"
                    "Tente outro nome, uma versão em inglês ou apenas parte do título.",
                )
                return

            token = _store_search_session(context, query, results)
            text = _build_search_text(query, 1, len(results))
            keyboard = _build_results_keyboard(results, 1, len(results), token)

            await _safe_delete(loading)
            await message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)

        except asyncio.TimeoutError:
            await _safe_edit(
                loading,
                "⏳ <b>A busca demorou mais do que o normal.</b>\nTente novamente em instantes.",
            )
        except Exception as exc:
            print("ERRO NA BUSCA:", repr(exc))
            await _safe_edit(
                loading,
                "❌ <b>Erro ao buscar no catálogo.</b>\nTente novamente em alguns instantes.",
            )
        finally:
            _clear_inflight(user.id, query)
