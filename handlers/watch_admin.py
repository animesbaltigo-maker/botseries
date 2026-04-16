import html

from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS
from services.watch_guard import get_watch_block_status, set_watch_blocked


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def _normalize_action(args: list[str]) -> str:
    if not args:
        return ""
    value = str(args[0] or "").strip().lower()
    aliases = {
        "on": "on",
        "ativar": "on",
        "ativa": "on",
        "bloquear": "on",
        "bloqueia": "on",
        "1": "on",
        "off": "off",
        "desativar": "off",
        "desativa": "off",
        "liberar": "off",
        "libera": "off",
        "desbloquear": "off",
        "0": "off",
    }
    return aliases.get(value, "")


def _status_label(blocked: bool) -> str:
    return "Bloqueado" if blocked else "Liberado"


async def bloqueareps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    if not _is_admin(user.id):
        await message.reply_text("❌ Você não tem permissão para usar esse comando.")
        return

    action = _normalize_action(context.args or [])
    if action == "on":
        set_watch_blocked(True, updated_by=user.id)
        await message.reply_text(
            "🔒 <b>Assistir foi bloqueado.</b>\n\n"
            "Filmes e episódios agora exigirão autorização.",
            parse_mode="HTML",
        )
        return

    if action == "off":
        set_watch_blocked(False, updated_by=user.id)
        await message.reply_text(
            "✅ <b>Assistir foi liberado.</b>\n\n"
            "Os botões de filme e episódios voltaram ao normal.",
            parse_mode="HTML",
        )
        return

    status = get_watch_block_status()
    updated_by = int(status.get("updated_by") or 0)
    updated_by_text = f"<code>{updated_by}</code>" if updated_by else "Não informado"
    updated_at = html.escape(str(status.get("updated_at") or "Não informado"))
    await message.reply_text(
        "🛡️ <b>Controle de assistir</b>\n\n"
        f"Status: <b>{_status_label(bool(status.get('watch_blocked')))}</b>\n"
        f"Última alteração por: {updated_by_text}\n"
        f"Última alteração em: {updated_at}\n\n"
        "Use:\n"
        "<code>/bloqueareps on</code>\n"
        "<code>/bloqueareps off</code>",
        parse_mode="HTML",
    )
