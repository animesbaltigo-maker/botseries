import asyncio
import html

from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS
from services.bingo_system import *

CANAL_ID = -1001823020280


def is_admin(uid):
    return uid in ADMIN_IDS


def fmt(nums):
    return " • ".join(f"{n:02d}" for n in nums)


async def startbingo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    if not is_admin(update.effective_user.id):
        return

    if not start_bingo():
        await update.message.reply_text("⚠️ O bingo já foi iniciado.")
        return

    total_players = len(get_data()["players"])

    await context.bot.send_message(
        CANAL_ID,
        (
            "🎉 <b>BINGO INICIADO!</b>\n\n"
            f"👥 <b>Participantes:</b> <code>{total_players}</code>\n"
            "🎁 <b>Prêmio:</b> Ovo da páscoa\n\n"
            "📢 As cartelas foram encerradas.\n"
            "🍀 Boa sorte a todos!"
        ),
        parse_mode="HTML",
    )

    await update.message.reply_text("✅ Bingo iniciado com sucesso.")


async def sortear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return

    if not is_admin(update.effective_user.id):
        return

    await run_draw(context)


async def startbingo_auto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return

    if not is_admin(update.effective_user.id):
        return

    await update.message.reply_text("🎰 Auto sorteio iniciado.")

    while True:
        data = get_data()

        if not data["active"]:
            break

        await run_draw(context)
        await asyncio.sleep(5)


async def run_draw(context: ContextTypes.DEFAULT_TYPE):
    n = draw_number()
    if not n:
        return

    msg = await context.bot.send_message(
        CANAL_ID,
        "🎰 <b>Girando o globo do bingo...</b>",
        parse_mode="HTML",
    )

    await asyncio.sleep(1.2)

    data = get_data()
    ranking = get_ranking()
    almost = get_almost()

    rank_txt = ""
    medals = ["🥇", "🥈", "🥉"]

    for i, (name, hits, nums) in enumerate(ranking):
        rank_txt += (
            f"{medals[i]} <b>{html.escape(name)}</b> — "
            f"<code>{hits}/6</code>\n"
        )

    if not rank_txt:
        rank_txt = "Ninguém ainda"

    almost_txt = ""
    if almost:
        names = []

        for uid, name in almost:
            names.append(html.escape(name))

            try:
                await context.bot.send_message(
                    int(uid),
                    "🔥 <b>VOCÊ ESTÁ A 1 NÚMERO DO BINGO!</b>\n\nFique ligado no próximo sorteio 👀",
                    parse_mode="HTML",
                )
            except Exception:
                pass

        almost_txt = (
            "\n🔥 <b>QUASE LÁ:</b>\n"
            + " • ".join(names)
        )

    await msg.edit_text(
        (
            f"🎲 <b>NÚMERO SORTEADO:</b> <code>{n:02d}</code>\n\n"
            f"📊 <b>Números já sorteados:</b>\n"
            f"{fmt(data['drawn'])}\n\n"
            f"🏆 <b>TOP 3 DO MOMENTO</b>\n"
            f"{rank_txt}"
            f"{almost_txt}"
        ),
        parse_mode="HTML",
    )

    winner = check_winner()

    if winner:
        await context.bot.send_message(
            CANAL_ID,
            (
                "🏆 <b>BINGO!</b>\n\n"
                f"👤 <b>Vencedor:</b> {html.escape(winner['name'])}\n"
                f"🎟 <b>Cartela:</b>\n<code>{fmt(winner['numbers'])}</code>\n\n"
                "🎁 <b>Prêmio:</b> Ovo da páscoa\n\n"
                "👏 Parabéns!"
            ),
            parse_mode="HTML",
        )

async def resetbingo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user or not message:
        return

    if not is_admin(user.id):
        return

    reset()

    await message.reply_text(
        "♻️ <b>Bingo resetado com sucesso!</b>\n\n"
        "📢 Agora você já pode iniciar uma nova rodada.",
        parse_mode="HTML"
    )