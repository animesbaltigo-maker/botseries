from telegram import Update
from telegram.ext import ContextTypes
from services.bingo_system import register_player


def fmt(nums):
    return " • ".join(f"{n:02d}" for n in nums)


async def bingo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not user or not message:
        return

    nums, status = register_player(user.id, user.first_name)

    # 🔒 já começou
    if status == "started":
        await message.reply_text(
            "❌ <b>O bingo já começou.</b>\n\n"
            "📢 Agora é só acompanhar o sorteio no canal.",
            parse_mode="HTML"
        )
        return

    # 🔁 já está participando
    if status == "exists":
        await message.reply_text(
            (
                "🎟 <b>Você já está participando do bingo!</b>\n\n"
                f"<code>{fmt(nums)}</code>\n\n"
                "📢 Aguarde o início do sorteio no canal.\n"
                "🍀 Boa sorte!"
            ),
            parse_mode="HTML"
        )
        return

    # 🆕 novo jogador
    await message.reply_text(
        (
            "🎟 <b>Sua cartela foi gerada!</b>\n\n"
            f"<code>{fmt(nums)}</code>\n\n"
            "📢 Aguarde o início do sorteio no canal.\n"
            "🏆 <b>Prêmio:</b> Ovo da páscoa\n\n"
            "🍀 Boa sorte!"
        ),
        parse_mode="HTML"
    )