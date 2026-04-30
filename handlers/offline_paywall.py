from __future__ import annotations

import html
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import BALTIGOFLIX_SUPPORT_URL, BOT_BRAND
from services.cakto_api import cakto_api_configured, verify_cakto_payment_for_user
from services.cakto_gateway import get_checkout_options
from services.subscriptions import get_active_subscription

BALTIGOFLIX_OFFER_IMAGE = "https://cdn-checkout.cakto.com.br/images/8f71ba5b-ae9d-45d7-a959-dc749fb51543.jpg"


def _keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(option["label"], url=option["url"])]
        for option in get_checkout_options(user_id)
    ]
    rows.append([InlineKeyboardButton("Ja paguei / verificar", callback_data="subcheck")])
    if BALTIGOFLIX_SUPPORT_URL:
        rows.append([InlineKeyboardButton("Falar com suporte", url=BALTIGOFLIX_SUPPORT_URL)])
    return InlineKeyboardMarkup(rows)


def _text(title: str = "") -> str:
    brand = html.escape(BOT_BRAND or "BaltigoFlix")
    media_line = f"<b>Conteudo:</b> <i>{html.escape(title)}</i>\n" if title else ""

    return (
        "<b>Download bloqueado</b>\n\n"
        f"<b>Area exclusiva para assinantes do {brand}</b>\n"
        f"{media_line}"
        "<b>Status:</b> <code>sem assinatura ativa</code>\n"
        "<b>Liberacao:</b> <i>automatica pelo seu Telegram ID</i>\n\n"
        "<b>Com a assinatura voce libera:</b>\n"
        "- Downloads de episodios e filmes direto no Telegram\n"
        "- Acesso no bot de series e no bot de animes\n"
        "- O mesmo plano e a mesma validade nos dois bots\n\n"
        "<b>Planos:</b>\n"
        "- Mensal: R$ 19,90\n"
        "- Trimestral: R$ 39,90\n"
        "- Semestral: R$ 59,90\n"
        "- Anual: R$ 129,90\n\n"
        "Escolha um plano abaixo. Depois do pagamento, toque em <b>Ja paguei / verificar</b>."
    )


async def planos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    sub = get_active_subscription(user.id)
    if sub:
        expires_at = int(sub.get("expires_at") or 0)
        days_left = max(0, int((expires_at - int(time.time())) / 86400))
        await message.reply_text(
            (
                "<b>Assinatura ativa</b>\n\n"
                f"<b>Plano:</b> {html.escape(str(sub.get('plan_name') or 'BaltigoFlix'))}\n"
                f"<b>Validade restante:</b> {days_left} dia(s)\n\n"
                "Esse acesso libera o bot de series e o bot de animes pelo mesmo Telegram ID."
            ),
            parse_mode="HTML",
        )
        return

    try:
        await message.reply_photo(
            photo=BALTIGOFLIX_OFFER_IMAGE,
            caption=_text(),
            parse_mode="HTML",
            reply_markup=_keyboard(user.id),
        )
    except Exception:
        await message.reply_text(
            _text(),
            parse_mode="HTML",
            reply_markup=_keyboard(user.id),
            disable_web_page_preview=True,
        )


async def send_offline_paywall(query, user, title: str = "") -> None:
    await query.answer("Download exclusivo para assinantes.", show_alert=True)
    if query.message:
        try:
            await query.message.reply_photo(
                photo=BALTIGOFLIX_OFFER_IMAGE,
                caption=_text(title),
                parse_mode="HTML",
                reply_markup=_keyboard(user.id),
            )
        except Exception:
            await query.message.reply_text(
                _text(title),
                parse_mode="HTML",
                reply_markup=_keyboard(user.id),
                disable_web_page_preview=True,
            )


async def answer_subscription_check(query, user_id: int) -> None:
    sub = get_active_subscription(user_id)
    if not sub and cakto_api_configured():
        await query.answer("Verificando pagamento na Cakto...", show_alert=False)
        try:
            result = await verify_cakto_payment_for_user(user_id)
        except Exception:
            result = {"ok": False, "reason": "api_error"}
        if result.get("ok"):
            sub = get_active_subscription(user_id)

    if not sub:
        if not cakto_api_configured():
            text = (
                "Nao consegui verificar pela API da Cakto.\n\n"
                "Chame o suporte para fazermos a liberacao manual."
            )
        else:
            text = (
                "Pagamento ainda nao confirmado.\n\n"
                "Se o Pix ja saiu da conta, aguarde alguns instantes e toque em verificar de novo."
            )
        await query.answer(text, show_alert=True)
        return

    expires_at = int(sub.get("expires_at") or 0)
    days_left = max(0, int((expires_at - int(time.time())) / 86400))
    await query.answer(
        f"Assinatura ativa!\n\nPlano: {sub.get('plan_name') or 'BaltigoFlix'}\n"
        f"Validade restante: {days_left} dia(s).",
        show_alert=True,
    )
