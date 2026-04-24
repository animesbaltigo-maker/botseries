import html
import re

from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS
from services.watch_guard import (
    add_watch_allowed_users,
    clear_watch_allowed_users,
    get_watch_allowed_user_ids,
    get_watch_block_status,
    remove_watch_allowed_users,
    set_watch_blocked,
)


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


def _extract_user_ids(args: list[str]) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()
    for arg in args:
        for token in re.split(r"[\s,;]+", str(arg or "").strip()):
            if not token or not token.isdigit():
                continue
            user_id = int(token)
            if user_id <= 0 or user_id in seen:
                continue
            seen.add(user_id)
            ids.append(user_id)
    return ids


def _format_allowed_ids(user_ids: list[int]) -> str:
    if not user_ids:
        return "<i>Nenhum ID liberado.</i>"
    return "\n".join(f"• <code>{user_id}</code>" for user_id in user_ids[:50])


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
    allowed_user_ids = get_watch_allowed_user_ids()
    await message.reply_text(
        "🛡️ <b>Controle de assistir</b>\n\n"
        f"Status: <b>{_status_label(bool(status.get('watch_blocked')))}</b>\n"
        f"Última alteração por: {updated_by_text}\n"
        f"Última alteração em: {updated_at}\n"
        f"IDs liberados: <b>{len(allowed_user_ids)}</b>\n\n"
        "Use:\n"
        "<code>/bloqueareps on</code>\n"
        "<code>/bloqueareps off</code>\n"
        "<code>/liberaeps 123456789</code>\n"
        "<code>/liberaeps rm 123456789</code>\n"
        "<code>/liberaeps list</code>\n\n"
        "<b>Whitelist atual</b>\n"
        f"{_format_allowed_ids(allowed_user_ids)}",
        parse_mode="HTML",
    )


async def liberaeps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    if not _is_admin(user.id):
        await message.reply_text("❌ Você não tem permissão para usar esse comando.")
        return

    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    action = str(args[0] or "").strip().lower() if args else "list"
    payload_args = args[1:] if args else []

    if action in {"list", "ls", "status"}:
        allowed_user_ids = get_watch_allowed_user_ids()
        await message.reply_text(
            "🧾 <b>IDs liberados no bloqueio</b>\n\n"
            f"Total: <b>{len(allowed_user_ids)}</b>\n\n"
            f"{_format_allowed_ids(allowed_user_ids)}\n\n"
            "Use:\n"
            "<code>/liberaeps 123456789</code>\n"
            "<code>/liberaeps rm 123456789</code>\n"
            "<code>/liberaeps clear</code>",
            parse_mode="HTML",
        )
        return

    if action in {"clear", "limpar"}:
        clear_watch_allowed_users(updated_by=user.id)
        await message.reply_text(
            "🧹 <b>Lista de IDs liberados limpa.</b>\n\n"
            "Agora apenas novos IDs cadastrados voltarão a ter acesso com o bloqueio ativo.",
            parse_mode="HTML",
        )
        return

    if action in {"rm", "del", "rem", "remove", "off"}:
        user_ids = _extract_user_ids(payload_args)
        if not user_ids:
            await message.reply_text(
                "⚠️ <b>Informe pelo menos um ID para remover.</b>\n\n"
                "Exemplo:\n"
                "<code>/liberaeps rm 123456789</code>",
                parse_mode="HTML",
            )
            return

        state = remove_watch_allowed_users(user_ids, updated_by=user.id)
        await message.reply_text(
            "➖ <b>ID(s) removido(s) da whitelist.</b>\n\n"
            f"Removidos agora: <b>{len(user_ids)}</b>\n"
            f"Total liberado: <b>{len(state.get('allowed_user_ids') or [])}</b>",
            parse_mode="HTML",
        )
        return

    add_aliases = {"add", "on", "allow", "liberar", "libera", "autorizar", "autoriza"}
    user_ids = _extract_user_ids(payload_args if action in add_aliases else args)
    if not user_ids:
        await message.reply_text(
            "⚠️ <b>Informe pelo menos um ID válido para liberar.</b>\n\n"
            "Exemplos:\n"
            "<code>/liberaeps 123456789</code>\n"
            "<code>/liberaeps add 123456789 987654321</code>\n"
            "<code>/liberaeps rm 123456789</code>",
            parse_mode="HTML",
        )
        return

    state = add_watch_allowed_users(user_ids, updated_by=user.id)
    await message.reply_text(
        "✅ <b>ID(s) liberado(s) com sucesso.</b>\n\n"
        f"Adicionados agora: <b>{len(user_ids)}</b>\n"
        f"Total liberado: <b>{len(state.get('allowed_user_ids') or [])}</b>\n\n"
        f"{_format_allowed_ids(list(state.get('allowed_user_ids') or []))}",
        parse_mode="HTML",
    )
