import html

from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS
from services.metrics import clear_metrics, get_metrics_report


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def _fmt_top(rows: list[dict], empty_text: str = "Nenhum dado ainda") -> str:
    if not rows:
        return empty_text

    lines: list[str] = []
    for index, row in enumerate(rows, start=1):
        label = html.escape(str(row.get("label") or "Sem titulo"))
        total = int(row.get("total") or 0)
        lines.append(f"{index}. <code>{label}</code> - <b>{total}</b>")
    return "\n".join(lines)


def _normalize_period(args: list[str]) -> str:
    if not args:
        return "total"

    aliases = {
        "hoje": "hoje",
        "today": "hoje",
        "7d": "7d",
        "7dias": "7d",
        "7": "7d",
        "semana": "7d",
        "30d": "30d",
        "30dias": "30d",
        "30": "30d",
        "mes": "30d",
        "total": "total",
        "all": "total",
    }
    return aliases.get((args[0] or "").strip().lower(), "total")


def _period_label(period: str) -> str:
    labels = {
        "hoje": "Hoje",
        "7d": "Ultimos 7 dias",
        "30d": "Ultimos 30 dias",
        "total": "Total",
    }
    return labels.get(period, "Total")


async def metricas(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    if not _is_admin(user.id):
        await message.reply_text("❌ Voce nao tem permissao para usar esse comando.")
        return

    period = _normalize_period(context.args or [])
    data = get_metrics_report(limit=7, period=period)
    top_opened = data.get("top_opened_titles") or data.get("top_opened_animes") or []

    text = (
        "📊 <b>Metricas do bot</b>\n"
        f"🗂 <b>Periodo:</b> {html.escape(_period_label(period))}\n\n"
        "🔎 <b>Buscas mais feitas</b>\n"
        f"{_fmt_top(data.get('top_searches', []))}\n\n"
        "🎬 <b>Titulos mais abertos</b>\n"
        f"{_fmt_top(top_opened)}\n\n"
        "▶️ <b>Cliques em assistir</b>\n"
        f"{_fmt_top(data.get('top_watch_clicks', []))}\n\n"
        "📺 <b>Episodios acessados</b>\n"
        f"{_fmt_top(data.get('top_episodes', []))}\n\n"
        "📉 <b>Buscas sem resultado</b>\n"
        f"<b>{int(data.get('searches_without_result', 0))}</b>\n\n"
        "👥 <b>Novos usuarios</b>\n"
        f"<b>{int(data.get('new_users', 0))}</b>\n\n"
        "🔁 <b>Usuarios ativos</b>\n"
        f"<b>{int(data.get('active_users', 0))}</b>"
    )

    await message.reply_text(
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def metricas_limpar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    if not _is_admin(user.id):
        await message.reply_text("❌ Voce nao tem permissao para usar esse comando.")
        return

    clear_metrics()
    await message.reply_text(
        "🗑 <b>Todas as metricas foram limpas com sucesso.</b>",
        parse_mode="HTML",
    )
