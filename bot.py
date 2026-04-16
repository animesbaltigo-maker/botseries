import logging
from pathlib import Path

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    filters,
)

from config import (
    BOT_API_CONNECTION_POOL,
    BOT_API_CONNECT_TIMEOUT,
    BOT_API_MAX_CONCURRENT_UPDATES,
    BOT_API_POOL_TIMEOUT,
    BOT_API_READ_TIMEOUT,
    BOT_API_WRITE_TIMEOUT,
    BOT_BRAND,
    BOT_TOKEN,
    LOG_DIR,
)
from core.http_client import close_http_client
from handlers.bingo import bingo
from handlers.bingo_admin import resetbingo, sortear, startbingo, startbingo_auto
from handlers.broadcast import broadcast_callbacks, broadcast_command, broadcast_message_router
from handlers.callbacks import callbacks
from handlers.discover import aleatorio, lancamentos
from handlers.group_ai import esquecer_handler, group_ai_handler
from handlers.help import ajuda
from handlers.inline import inline_query
from handlers.metricas import metricas, metricas_limpar
from handlers.novoseps import auto_post_new_eps_job, postnovoseps
from handlers.pedido import pedido
from handlers.postanime import postanime
from handlers.postfilmes import postfilmes
from handlers.referral import indicacoes, referral_button
from handlers.referral_admin import auto_referral_check_job, refstats
from handlers.search import buscar
from handlers.start import start
from handlers.watch_admin import bloqueareps
from services.metrics import init_metrics_db
from services.catalog_client import close_catalog_client
from services.referral_db import init_referral_db

init_metrics_db()
init_referral_db()

LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = Path(LOG_DIR) / "bot.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )


async def post_shutdown(app: Application) -> None:
    await close_catalog_client()
    await close_http_client()


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.exception("Erro nao tratado no bot", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "❌ <b>Ocorreu um erro ao processar sua solicitacao.</b>",
                parse_mode="HTML",
            )
    except Exception:
        pass


def _register_jobs(app: Application) -> None:
    if not app.job_queue:
        LOGGER.warning("JobQueue nao disponivel.")
        return

    app.job_queue.run_repeating(
        auto_post_new_eps_job,
        interval=600,
        first=30,
        name="auto_post",
    )
    app.job_queue.run_repeating(
        auto_referral_check_job,
        interval=3600,
        first=60,
        name="auto_referral_check",
    )


def main() -> None:
    _configure_logging()
    if not BOT_TOKEN:
        raise RuntimeError("Configure BOT_TOKEN no arquivo .env.")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(BOT_API_MAX_CONCURRENT_UPDATES)
        .connection_pool_size(BOT_API_CONNECTION_POOL)
        .pool_timeout(BOT_API_POOL_TIMEOUT)
        .connect_timeout(BOT_API_CONNECT_TIMEOUT)
        .read_timeout(BOT_API_READ_TIMEOUT)
        .write_timeout(BOT_API_WRITE_TIMEOUT)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buscar", buscar))
    app.add_handler(CommandHandler("lancamentos", lancamentos))
    app.add_handler(CommandHandler("aleatorio", aleatorio))
    app.add_handler(CommandHandler("ajuda", ajuda))

    app.add_handler(CommandHandler("postanime", postanime))
    app.add_handler(CommandHandler("postserie", postanime))
    app.add_handler(CommandHandler("postfilme", postfilmes))
    app.add_handler(CommandHandler("postfilmes", postfilmes))
    app.add_handler(CommandHandler("postnovoseps", postnovoseps))

    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("indicacoes", indicacoes))
    app.add_handler(CommandHandler("refstats", refstats))
    app.add_handler(CommandHandler("bingo", bingo))
    app.add_handler(CommandHandler("startbingo", startbingo))
    app.add_handler(CommandHandler("sortear", sortear))
    app.add_handler(CommandHandler("autobingo", startbingo_auto))
    app.add_handler(CommandHandler("resetbingo", resetbingo))
    app.add_handler(CommandHandler("metricas", metricas))
    app.add_handler(CommandHandler("metricaslimpar", metricas_limpar))
    app.add_handler(CommandHandler("bloqueareps", bloqueareps))
    app.add_handler(CommandHandler("pedido", pedido))
    app.add_handler(CommandHandler("esquecer", esquecer_handler))

    app.add_handler(InlineQueryHandler(inline_query))

    app.add_handler(CallbackQueryHandler(broadcast_callbacks, pattern=r"^bc\|"))
    app.add_handler(CallbackQueryHandler(referral_button, pattern=r"^noop_indicar$"))
    app.add_handler(CallbackQueryHandler(callbacks, pattern=r"^(pb_|noop$)"))

    app.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND, broadcast_message_router),
        group=99,
    )
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, group_ai_handler),
        group=100,
    )

    _register_jobs(app)
    app.add_error_handler(error_handler)

    LOGGER.info("%s rodando...", BOT_BRAND)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
