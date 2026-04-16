"""IA para grupos e privado, adaptada para filmes e series."""

import asyncio
import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from config import BOT_USERNAME
from services.ai_group_state import can_reply, mark_reply
from services.gemini_ai import NO_REPLY_TOKEN, generate_group_reply, split_for_telegram
from services.memory import conversation_memory
from services.catalog_client import search_content
from utils.gatekeeper import ensure_channel_membership

TRIGGER = "akira"

_MAX_BUTTONS = 3
_RESOLVE_TIMEOUT = 4.5
_MIN_SCORE_RATIO = 0.60

_BOLD_RE = re.compile(r"<b>([^<]{2,120})</b>")
_SPLIT_RE = re.compile(r"\s*(?:,\s*|\s+e\s+|\s+ou\s+)\s*")

_NOISE_EXACT = frozenset(
    [
        "baltigo",
        "bot",
        "miniapp",
        "webapp",
        "busca",
        "episodio",
        "temporada",
        "dublado",
        "legendado",
        "disponivel",
        "genero",
        "acao",
        "romance",
        "comedia",
        "terror",
        "drama",
        "misterio",
        "fantasia",
        "recomendacao",
        "sugestao",
        "oi",
        "ola",
        "sim",
        "nao",
        "serie",
        "filme",
    ]
)
_NOISE_PREFIX = (
    "@",
    "/",
    "como",
    "aqui",
    "olha",
    "veja",
    "entre",
    "envie",
    "abra",
    "escolha",
    "toque",
    "clique",
    "acesse",
    "passo",
)


def _extract_candidates(reply: str, user_text: str) -> list[str]:
    seen: set[str] = set()
    candidates: list[str] = []

    def _add(title: str) -> None:
        candidate = title.strip().rstrip(".")
        lowered = candidate.lower()
        if len(candidate) < 3 or len(candidate) > 50:
            return
        if lowered in _NOISE_EXACT:
            return
        if any(lowered.startswith(prefix) for prefix in _NOISE_PREFIX):
            return
        if len(candidate.split()) > 7:
            return
        if lowered in seen:
            return
        seen.add(lowered)
        candidates.append(candidate)

    intent_re = re.compile(
        r"(?:assistir|ver|buscar|quero|gosto de|falar de|sobre)\s+([^?!.,\n]{2,40})",
        re.IGNORECASE,
    )
    for match in intent_re.finditer(user_text):
        _add(match.group(1).strip())

    for match in _BOLD_RE.finditer(reply):
        for part in _SPLIT_RE.split(match.group(1).strip()):
            _add(part)
        if len(candidates) >= _MAX_BUTTONS * 2:
            break

    return candidates[: _MAX_BUTTONS * 2]


def _title_similarity(query: str, result_title: str) -> float:
    left = query.lower().strip()
    right = result_title.lower().strip()
    if left == right:
        return 1.0
    if left in right or right in left:
        return 0.85
    left_words = set(left.split())
    right_words = set(right.split())
    if not left_words or not right_words:
        return 0.0
    return len(left_words & right_words) / len(left_words | right_words)


async def _resolve_one(candidate: str) -> tuple[str, str] | None:
    try:
        results = await asyncio.wait_for(search_content(candidate), timeout=_RESOLVE_TIMEOUT)
    except Exception as exc:
        print(f"[group_ai][resolve] erro '{candidate}': {exc}")
        return None

    if not results:
        return None

    best = results[0]
    best_title = str(best.get("title") or "").strip()
    best_url = str(best.get("url") or best.get("id") or "").strip()
    if not best_title or not best_url:
        return None
    if _title_similarity(candidate, best_title) < _MIN_SCORE_RATIO:
        return None
    return best_title, best_url


async def _resolve_buttons(reply: str, user_text: str) -> InlineKeyboardMarkup | None:
    candidates = _extract_candidates(reply, user_text)
    if not candidates:
        return None

    resolved = await asyncio.gather(*[_resolve_one(candidate) for candidate in candidates])

    rows: list[list[InlineKeyboardButton]] = []
    seen_urls: set[str] = set()
    for item in resolved:
        if item is None:
            continue
        title, url = item
        if url in seen_urls:
            continue
        seen_urls.add(url)
        label = f"🎬 {title}"
        if len(label) > 40:
            label = label[:37].rstrip() + "..."
        rows.append([InlineKeyboardButton(label, url=url)])
        if len(rows) >= _MAX_BUTTONS:
            break

    return InlineKeyboardMarkup(rows) if rows else None


async def group_ai_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    chat = update.effective_chat
    if not message or not message.text:
        return

    text = message.text.strip()
    text_lower = text.lower()

    replying_to_bot = bool(
        message.reply_to_message
        and message.reply_to_message.from_user
        and message.reply_to_message.from_user.username
        and BOT_USERNAME
        and message.reply_to_message.from_user.username.lower() == BOT_USERNAME.lower()
    )

    if chat and chat.type == "private":
        if context.user_data.get("broadcast_state"):
            return
        user_text = text
    elif text_lower.startswith(TRIGGER):
        user_text = text[len(TRIGGER):].strip()
    elif replying_to_bot:
        user_text = text
    else:
        return

    if not user_text:
        await message.reply_text(
            "Fala comigo assim: <code>akira me recomenda uma serie ou filme</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not await ensure_channel_membership(update, context):
        return

    if chat and chat.type in ("group", "supergroup") and not can_reply(chat.id):
        await message.reply_text(
            "🧠 Tô recarregando aqui. Me chama de novo em instantes.",
            parse_mode=ParseMode.HTML,
        )
        return

    history = conversation_memory.get_history(message.chat_id)
    reply = await generate_group_reply(user_text, history=history)
    if not reply or reply == NO_REPLY_TOKEN:
        return

    parts, keyboard = await asyncio.gather(
        asyncio.to_thread(split_for_telegram, reply),
        _resolve_buttons(reply, user_text),
    )

    conversation_memory.add_turn(message.chat_id, user_text, reply)
    if chat and chat.type in ("group", "supergroup"):
        mark_reply(chat.id)

    for index, part in enumerate(parts):
        reply_markup = keyboard if index == len(parts) - 1 else None
        await message.reply_text(
            part,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )


async def esquecer_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return

    conversation_memory.clear(message.chat_id)
    await message.reply_text(
        "🧹 Pronto. Limpei o contexto dessa conversa.",
        parse_mode=ParseMode.HTML,
    )
