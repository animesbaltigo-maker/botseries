"""Entrega de episódios e filmes pelo próprio Telegram com cache reaproveitável."""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from telegram import Bot

from config import (
    DATA_DIR,
    EPISODE_CACHE_CHAT_ID,
    TELETHON_UPLOAD_MAX_MB,
    VIDEO_DOWNLOAD_MAX_MB,
    VIDEO_DOWNLOAD_PROTECT_CONTENT,
    VIDEO_UPLOAD_MAX_MB,
    VIDEO_SEND_CONCURRENCY,
    VIDEO_SEND_ENABLED,
    VIDEO_SEND_MAX_MB,
    VIDEO_SEND_TIMEOUT,
    VIDEO_TMP_DIR,
)
from core.http_client import get_http_client
from core.telethon_uploader import send_file_with_telethon, telethon_configured

LOGGER = logging.getLogger(__name__)
DB_PATH = Path(DATA_DIR) / "episode_cache.sqlite3"
MAX_BYTES = int(VIDEO_DOWNLOAD_MAX_MB or VIDEO_SEND_MAX_MB) * 1024 * 1024
BOT_UPLOAD_MAX_BYTES = int(VIDEO_UPLOAD_MAX_MB) * 1024 * 1024
TELETHON_MAX_BYTES = int(TELETHON_UPLOAD_MAX_MB or VIDEO_SEND_MAX_MB) * 1024 * 1024

_DELIVERY_SEMAPHORE: asyncio.Semaphore | None = None
_PENDING_LOCK = asyncio.Lock()
_PENDING_UPLOADS: dict[str, asyncio.Future[dict[str, Any]]] = {}

ProgressCallback = Callable[[dict[str, Any]], Awaitable[None] | None]


@dataclass(slots=True)
class VideoDeliveryRequest:
    cache_key: str
    source_url: str
    display_name: str
    caption: str
    parse_mode: str = "HTML"


class _ProgressFile:
    def __init__(self, path: Path, on_read: Callable[[int], None]) -> None:
        self._handle = path.open("rb")
        self._on_read = on_read
        self._bytes_read = 0
        self.name = self._handle.name

    def read(self, size: int = -1) -> bytes:
        chunk = self._handle.read(size)
        if chunk:
            self._bytes_read += len(chunk)
            self._on_read(self._bytes_read)
        return chunk

    def close(self) -> None:
        self._handle.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


@contextmanager
def _conn():
    init_episode_delivery_db()
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_episode_delivery_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS cached_videos (
                cache_key TEXT PRIMARY KEY,
                display_name TEXT NOT NULL DEFAULT '',
                source_url TEXT NOT NULL DEFAULT '',
                stored_chat_id TEXT NOT NULL DEFAULT '',
                stored_message_id INTEGER NOT NULL DEFAULT 0,
                file_id TEXT NOT NULL DEFAULT '',
                file_unique_id TEXT NOT NULL DEFAULT '',
                mime_type TEXT NOT NULL DEFAULT '',
                file_size INTEGER NOT NULL DEFAULT 0,
                duration INTEGER NOT NULL DEFAULT 0,
                width INTEGER NOT NULL DEFAULT 0,
                height INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL DEFAULT 0,
                last_used_at REAL NOT NULL DEFAULT 0
            )
            """
        )


def _delivery_semaphore() -> asyncio.Semaphore:
    global _DELIVERY_SEMAPHORE
    if _DELIVERY_SEMAPHORE is None:
        _DELIVERY_SEMAPHORE = asyncio.Semaphore(max(1, int(VIDEO_SEND_CONCURRENCY or 1)))
    return _DELIVERY_SEMAPHORE


def _safe_caption(value: str) -> str:
    return str(value or "").strip()[:1024]


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', " ", str(value or "").strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    return cleaned[:180] or "video"


def _chat_target(value: str | int | None) -> str | int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def get_cached_video(cache_key: str) -> dict[str, Any] | None:
    with _conn() as connection:
        row = connection.execute(
            "SELECT * FROM cached_videos WHERE cache_key = ?",
            (str(cache_key or "").strip(),),
        ).fetchone()
    return _row_to_dict(row)


def store_cached_video(cache_key: str, record: dict[str, Any]) -> None:
    payload = dict(record or {})
    payload["cache_key"] = str(cache_key or "").strip()
    payload["display_name"] = str(payload.get("display_name") or "").strip()
    payload["source_url"] = str(payload.get("source_url") or "").strip()
    payload["stored_chat_id"] = str(payload.get("stored_chat_id") or "").strip()
    payload["stored_message_id"] = int(payload.get("stored_message_id") or 0)
    payload["file_id"] = str(payload.get("file_id") or "").strip()
    payload["file_unique_id"] = str(payload.get("file_unique_id") or "").strip()
    payload["mime_type"] = str(payload.get("mime_type") or "").strip()
    payload["file_size"] = int(payload.get("file_size") or 0)
    payload["duration"] = int(payload.get("duration") or 0)
    payload["width"] = int(payload.get("width") or 0)
    payload["height"] = int(payload.get("height") or 0)
    payload["created_at"] = float(payload.get("created_at") or time.time())
    payload["last_used_at"] = float(payload.get("last_used_at") or payload["created_at"])

    with _conn() as connection:
        connection.execute(
            """
            INSERT INTO cached_videos (
                cache_key, display_name, source_url, stored_chat_id, stored_message_id,
                file_id, file_unique_id, mime_type, file_size, duration, width, height,
                created_at, last_used_at
            )
            VALUES (
                :cache_key, :display_name, :source_url, :stored_chat_id, :stored_message_id,
                :file_id, :file_unique_id, :mime_type, :file_size, :duration, :width, :height,
                :created_at, :last_used_at
            )
            ON CONFLICT(cache_key) DO UPDATE SET
                display_name = excluded.display_name,
                source_url = excluded.source_url,
                stored_chat_id = excluded.stored_chat_id,
                stored_message_id = excluded.stored_message_id,
                file_id = excluded.file_id,
                file_unique_id = excluded.file_unique_id,
                mime_type = excluded.mime_type,
                file_size = excluded.file_size,
                duration = excluded.duration,
                width = excluded.width,
                height = excluded.height,
                created_at = excluded.created_at,
                last_used_at = excluded.last_used_at
            """,
            payload,
        )


def touch_cached_video(cache_key: str) -> None:
    with _conn() as connection:
        connection.execute(
            "UPDATE cached_videos SET last_used_at = ? WHERE cache_key = ?",
            (time.time(), str(cache_key or "").strip()),
        )


def delete_cached_video(cache_key: str) -> None:
    with _conn() as connection:
        connection.execute(
            "DELETE FROM cached_videos WHERE cache_key = ?",
            (str(cache_key or "").strip(),),
        )


async def _wait_or_claim_pending(cache_key: str) -> tuple[asyncio.Future[dict[str, Any]], bool]:
    async with _PENDING_LOCK:
        pending = _PENDING_UPLOADS.get(cache_key)
        if pending is not None:
            return pending, False
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        _PENDING_UPLOADS[cache_key] = future
        return future, True


async def _release_pending(cache_key: str, future: asyncio.Future[dict[str, Any]]) -> None:
    async with _PENDING_LOCK:
        if _PENDING_UPLOADS.get(cache_key) is future:
            _PENDING_UPLOADS.pop(cache_key, None)


async def _emit_progress(callback: ProgressCallback | None, payload: dict[str, Any]) -> None:
    if callback is None:
        return
    try:
        result = callback(payload)
        if inspect.isawaitable(result):
            await result
    except Exception:
        LOGGER.debug("Falha ao atualizar progresso da entrega", exc_info=True)


async def _download_video_file(
    source_url: str,
    display_name: str,
    *,
    progress_callback: ProgressCallback | None = None,
) -> tuple[Path, dict[str, Any]]:
    client = await get_http_client()
    filename = _safe_filename(display_name)
    tmp_path = Path(VIDEO_TMP_DIR) / f"{int(time.time() * 1000)}-{filename}.mp4"
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    headers = {
        "Accept": "video/*,*/*;q=0.8",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    total_written = 0
    mime_type = ""

    try:
        async with client.stream("GET", source_url, headers=headers) as response:
            response.raise_for_status()
            mime_type = str(response.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
            if mime_type and "video" not in mime_type and "octet-stream" not in mime_type:
                raise RuntimeError("Esse player ainda não fornece um arquivo de vídeo direto.")

            content_length = int(response.headers.get("content-length") or 0)
            if content_length and content_length > MAX_BYTES:
                raise RuntimeError(f"Esse vídeo excede o limite configurado de {VIDEO_SEND_MAX_MB} MB.")

            await _emit_progress(
                progress_callback,
                {
                    "stage": "downloading",
                    "download_bytes": 0,
                    "download_total": content_length,
                    "upload_bytes": 0,
                    "upload_total": 0,
                },
            )

            with tmp_path.open("wb") as handle:
                async for chunk in response.aiter_bytes(1024 * 1024):
                    if not chunk:
                        continue

                    total_written += len(chunk)
                    if total_written > MAX_BYTES:
                        raise RuntimeError(f"Esse vídeo excede o limite configurado de {VIDEO_SEND_MAX_MB} MB.")

                    handle.write(chunk)
                    await _emit_progress(
                        progress_callback,
                        {
                            "stage": "downloading",
                            "download_bytes": total_written,
                            "download_total": content_length or total_written,
                            "upload_bytes": 0,
                            "upload_total": total_written,
                        },
                    )
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise

    if total_written <= 0:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise RuntimeError("Não consegui baixar o arquivo de vídeo.")

    return tmp_path, {
        "mime_type": mime_type,
        "file_size": total_written,
    }


async def _upload_video(
    bot: Bot,
    chat_id: str | int,
    request: VideoDeliveryRequest,
    *,
    progress_callback: ProgressCallback | None = None,
) -> tuple[Any, dict[str, Any]]:
    tmp_path, download_meta = await _download_video_file(
        request.source_url,
        request.display_name,
        progress_callback=progress_callback,
    )

    upload_total = int(download_meta.get("file_size") or 0)
    upload_state = {"sent": 0}
    upload_done = asyncio.Event()

    def _mark_upload(sent: int) -> None:
        upload_state["sent"] = sent

    async def _report_upload() -> None:
        last_sent = -1
        while not upload_done.is_set():
            sent = min(int(upload_state["sent"]), upload_total)
            if sent != last_sent:
                await _emit_progress(
                    progress_callback,
                    {
                        "stage": "uploading",
                        "download_bytes": upload_total,
                        "download_total": upload_total,
                        "upload_bytes": sent,
                        "upload_total": upload_total,
                    },
                )
                last_sent = sent
            try:
                await asyncio.wait_for(upload_done.wait(), timeout=0.6)
            except asyncio.TimeoutError:
                continue

        await _emit_progress(
            progress_callback,
            {
                "stage": "uploading",
                "download_bytes": upload_total,
                "download_total": upload_total,
                "upload_bytes": upload_total,
                "upload_total": upload_total,
            },
        )

    file_handle = _ProgressFile(tmp_path, _mark_upload)
    reporter_task = asyncio.create_task(_report_upload())

    try:
        if upload_total > BOT_UPLOAD_MAX_BYTES:
            if not telethon_configured():
                raise RuntimeError(
                    "Arquivo grande demais para o Bot API oficial. "
                    "Preencha API_ID e API_HASH no .env para ativar o uploader Telethon igual ao bot de animes."
                )
            if upload_total > TELETHON_MAX_BYTES:
                raise RuntimeError(
                    f"Arquivo maior que o limite Telethon configurado ({TELETHON_UPLOAD_MAX_MB} MB)."
                )

            await bot.send_chat_action(chat_id=chat_id, action="upload_video")
            sent = await send_file_with_telethon(
                int(chat_id),
                tmp_path,
                _safe_caption(request.caption),
                as_video=True,
                progress_callback=lambda current, total: _mark_upload(current),
                protect_content=VIDEO_DOWNLOAD_PROTECT_CONTENT,
            )
            if not sent:
                raise RuntimeError("O uploader Telethon nao conseguiu iniciar. Confira API_ID, API_HASH e telethon.")
            return None, download_meta

        await bot.send_chat_action(chat_id=chat_id, action="upload_video")
        message = await bot.send_video(
            chat_id=chat_id,
            video=file_handle,
            caption=_safe_caption(request.caption),
            parse_mode=request.parse_mode,
            supports_streaming=True,
            read_timeout=VIDEO_SEND_TIMEOUT,
            write_timeout=VIDEO_SEND_TIMEOUT,
            connect_timeout=30,
            pool_timeout=30,
        )
        return message, download_meta
    except Exception as exc:
        text = str(exc or "").strip().lower()
        if "entity too large" in text or "file is too big" in text or "too large" in text:
            raise RuntimeError(
                f"Telegram recusou o upload. O arquivo excede o limite configurado/permitido ({VIDEO_UPLOAD_MAX_MB} MB)."
            ) from exc
        raise
    finally:
        upload_done.set()
        try:
            await reporter_task
        except Exception:
            pass
        file_handle.close()
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.debug("Nao consegui remover arquivo temporario: %s", tmp_path)


def _record_from_message(
    message: Any,
    request: VideoDeliveryRequest,
    *,
    stored_chat_id: str | int | None,
    download_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    video = getattr(message, "video", None)
    if video is None:
        raise RuntimeError("O Telegram não retornou um vídeo válido para esse envio.")

    meta = download_meta or {}
    now = time.time()

    return {
        "display_name": request.display_name,
        "source_url": request.source_url,
        "stored_chat_id": str(stored_chat_id or ""),
        "stored_message_id": int(getattr(message, "message_id", 0) or 0),
        "file_id": str(video.file_id or "").strip(),
        "file_unique_id": str(video.file_unique_id or "").strip(),
        "mime_type": str(meta.get("mime_type") or "").strip(),
        "file_size": int(getattr(video, "file_size", 0) or meta.get("file_size") or 0),
        "duration": int(getattr(video, "duration", 0) or 0),
        "width": int(getattr(video, "width", 0) or 0),
        "height": int(getattr(video, "height", 0) or 0),
        "created_at": now,
        "last_used_at": now,
    }


async def _send_from_cached_record(
    bot: Bot,
    chat_id: str | int,
    record: dict[str, Any],
    request: VideoDeliveryRequest,
    *,
    progress_callback: ProgressCallback | None = None,
) -> Any:
    file_id = str(record.get("file_id") or "").strip()
    if file_id:
        await _emit_progress(progress_callback, {"stage": "cached", "status": "sending"})
        await bot.send_chat_action(chat_id=chat_id, action="upload_video")
        return await bot.send_video(
            chat_id=chat_id,
            video=file_id,
            caption=_safe_caption(request.caption),
            parse_mode=request.parse_mode,
            supports_streaming=True,
            read_timeout=VIDEO_SEND_TIMEOUT,
            write_timeout=VIDEO_SEND_TIMEOUT,
            connect_timeout=30,
            pool_timeout=30,
        )

    stored_chat_id = _chat_target(record.get("stored_chat_id"))
    stored_message_id = int(record.get("stored_message_id") or 0)
    if stored_chat_id is not None and stored_message_id > 0:
        await _emit_progress(progress_callback, {"stage": "cached", "status": "copying"})
        return await bot.copy_message(
            chat_id=chat_id,
            from_chat_id=stored_chat_id,
            message_id=stored_message_id,
            read_timeout=VIDEO_SEND_TIMEOUT,
            write_timeout=VIDEO_SEND_TIMEOUT,
            connect_timeout=30,
            pool_timeout=30,
        )

    raise RuntimeError("Esse vídeo em cache não possui um arquivo reutilizável.")


async def _materialize_record(
    bot: Bot,
    target_chat_id: str | int,
    request: VideoDeliveryRequest,
    *,
    progress_callback: ProgressCallback | None = None,
) -> tuple[dict[str, Any], bool]:
    cache_chat_id = None if telethon_configured() else _chat_target(EPISODE_CACHE_CHAT_ID)
    upload_target = cache_chat_id if cache_chat_id is not None else target_chat_id
    message, download_meta = await _upload_video(
        bot,
        upload_target,
        request,
        progress_callback=progress_callback,
    )
    if message is None:
        return {}, True
    record = _record_from_message(
        message,
        request,
        stored_chat_id=upload_target,
        download_meta=download_meta,
    )
    return record, upload_target == target_chat_id


async def deliver_video_request(
    bot: Bot,
    chat_id: str | int,
    request: VideoDeliveryRequest,
    *,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    if not VIDEO_SEND_ENABLED:
        raise RuntimeError("O envio de vídeos pelo Telegram está desativado.")

    source_url = str(request.source_url or "").strip()
    if not source_url:
        raise RuntimeError("Não encontrei um arquivo de vídeo para esse conteúdo.")

    cache_key = str(request.cache_key or "").strip()
    if not cache_key:
        raise RuntimeError("Não consegui montar a chave de cache desse conteúdo.")

    cached = get_cached_video(cache_key)
    if cached:
        try:
            await _send_from_cached_record(
                bot,
                chat_id,
                cached,
                request,
                progress_callback=progress_callback,
            )
            touch_cached_video(cache_key)
            await _emit_progress(progress_callback, {"stage": "done", "status": "cached"})
            return {"status": "cached", "cache_key": cache_key}
        except Exception as exc:
            LOGGER.warning("Cache invalido para %s, reprocessando: %s", cache_key, exc)
            delete_cached_video(cache_key)

    future, owner = await _wait_or_claim_pending(cache_key)
    if owner:
        try:
            async with _delivery_semaphore():
                record, owner_already_received = await _materialize_record(
                    bot,
                    chat_id,
                    request,
                    progress_callback=progress_callback,
                )
                if record:
                    store_cached_video(cache_key, record)
                if not future.done():
                    future.set_result(
                        record
                        if record
                        else {"__error__": "Esse video grande foi enviado diretamente e nao ficou em cache."}
                    )

                if record and not owner_already_received:
                    await _send_from_cached_record(
                        bot,
                        chat_id,
                        record,
                        request,
                        progress_callback=progress_callback,
                    )

                await _emit_progress(progress_callback, {"stage": "done", "status": "uploaded"})
                return {"status": "uploaded", "cache_key": cache_key}
        except Exception as exc:
            if not future.done():
                future.set_result(
                    {
                        "__error__": str(exc or "Não consegui preparar esse vídeo agora.").strip()
                        or "Não consegui preparar esse vídeo agora."
                    }
                )
            raise
        finally:
            await _release_pending(cache_key, future)

    try:
        record = await future
        error_text = str(record.get("__error__") or "").strip()
        if error_text:
            raise RuntimeError(error_text)

        await _send_from_cached_record(
            bot,
            chat_id,
            record,
            request,
            progress_callback=progress_callback,
        )
        touch_cached_video(cache_key)
        await _emit_progress(progress_callback, {"stage": "done", "status": "shared"})
        return {"status": "shared", "cache_key": cache_key}
    finally:
        await _release_pending(cache_key, future)
